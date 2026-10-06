//! Offline downloads: a persistent queue that saves tracks to disk so they play without network.
//!
//! The contract with the UI: four commands
//! (`download_items`, `download_playlist`, `downloads_list`, `downloads_action`) and two events
//! (`downloads-changed`, `download-progress`). Everything here is backend-internal.
//!
//! **Transport.** Bytes are fetched in bounded 4 MiB `Range` requests, the same shape
//! [`crate::audioproxy`] uses for playback: googlevideo throttles an open-ended stream to roughly
//! 2x realtime and serves a bounded range at full speed. The audio proxy's stall guard wraps every
//! send and every body read, and each window's byte count is validated against its
//! `Content-Range`, so a truncated window fails the item instead of being committed as complete.
//!
//! **Persistence.** One row per videoId in the `downloads` table (db.rs). The files live in a
//! directory of their own (`<app data>/downloads`, or the `download_dir` setting) that
//! `clear_caches` never touches. A download writes `<name>.<itag>.part` and atomically renames it
//! to `<name>.<ext>` on completion; only a `.part` whose itag matches the freshly resolved format
//! is ever resumed.
//!
//! **Races.** Every state change is one conditional SQL `UPDATE ... WHERE state = ...`, so a pause,
//! cancel or remove landing in the same instant as a worker finishing resolves to exactly one
//! outcome. The owner of the losing side reconciles the file (keep the partial for `paused`,
//! delete it for `cancelled`/`removed`).
//!
//! **Offline playback.** [`offline_playback`] answers `AppState::resolve` for a videoId whose row
//! is `done`, handing mpv the file path directly: no network, no URL cache, no watch-history ping.
//!
//! **Anonymous by default.** Everything the download itself puts on the wire — the `/player`
//! resolve, the validation probe, the byte fetch, the playlist walk — runs under
//! [`innertube::anonymously`] with a freshly bootstrapped visitorData, so no request carries the
//! user's Google/YouTube credentials even while playback is signed in, and a track YouTube only
//! serves to an account fails with “Sign-in required for this track” instead of quietly retrying
//! signed in. The `download_use_account` setting (default off) is the opt-in that puts the
//! account back for members-only tracks and private lists.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use innertube::AudioQuality;
use serde::Serialize;

use crate::db::{Db, DownloadCollectionRow, DownloadRow, NewDownload, NewDownloadCollection};

/// How much upstream to ask for in one range request. Matches the playback proxy's chunk: large
/// enough to land in googlevideo's "full speed" regime, small enough that stopping costs little.
const CHUNK: u64 = 4 * 1024 * 1024;

/// Headroom the free-space preflight keeps on the volume while downloading: the filesystem's own
/// bookkeeping and anything else running need room, and filling a disk to the last byte is how
/// writes start failing mid-window. 64 MiB is the reserve Adiker's fork holds back per chunk
/// (their downloads.rs:34, GPL-3.0, Adiker/limusic@1fb1ded).
const SPACE_RESERVE: u64 = 64 * 1024 * 1024;

/// No progress on one connection for this long is a dead upstream. Same value (and reasoning) as
/// the audio proxy's stall guard, which [`fetch_to_file`] also runs `send()` through.
const STALL: Duration = Duration::from_secs(20);

/// Backstop for a whole 4 MiB window. A window on a 32 KB/s link is ~130 s; this only fires on an
/// upstream that keeps trickling forever without ever finishing what it promised.
const WINDOW_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Resolve attempts per item: a 403 mid-file (expired URL) re-resolves and continues, a resume the
/// server refuses restarts from zero. Bounded so a broken track cannot spin.
const ATTEMPTS: usize = 3;

/// Progress is written to the database (and emitted) at most this often, and always at least every
/// 1 MiB, so a crash or quit loses seconds, never the whole attempt.
const PROGRESS_STEP: u64 = 1024 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(500);

/// `downloads-changed` is coalesced to this rate; the trailing change is always sent.
const CHANGED_INTERVAL: Duration = Duration::from_millis(150);

/// The playlist walk's bounds. 5000 is YouTube Music's own playlist cap (import.rs).
const MAX_PLAYLIST_ITEMS: usize = 5000;
const MAX_PLAYLIST_PAGES: usize = 100;

/// Settings key (engine-owned: `set_setting` never accepts it from the UI) holding every folder
/// this engine has written into, as a JSON array of absolute paths. `download_dir` can move, but
/// the files it left behind stay the app's to delete and to sweep — this is that memory.
const KNOWN_DIRS_KEY: &str = "download_dirs";

/// How many remembered folders are kept. Enough for a user who relocates downloads a handful of
/// times, small enough that a hand-edited list cannot grow the scan unbounded.
const KNOWN_DIRS_MAX: usize = 16;

/// Finished downloads keep their cover in `<download dir>/artwork/<videoId>.<ext>` so downloaded
/// rows still show artwork with no network at all.
const ARTWORK_DIR: &str = "artwork";
/// The cover fetch runs after the row is already `done`, inside a worker holding a queue slot —
/// a hung thumbnail host may delay the *next* item by this long, never this one, and never fail it.
const ARTWORK_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on a saved cover. A thumbnail is ~100 KB; this only stops a broken or hostile URL from
/// streaming an unbounded body to disk (same ceiling as the app-icon fetch).
const ARTWORK_MAX: u64 = 16 * 1024 * 1024;

// --- shared types --------------------------------------------------------------------------------

/// What the UI sees for one download. Serialized with snake_case field names, exactly as
/// `id` is the videoId (one row per track).
#[derive(Debug, Clone, Serialize)]
pub struct DownloadItem {
    pub id: String,
    pub video_id: String,
    pub title: String,
    pub artists: String,
    pub album: Option<String>,
    pub thumbnail: Option<String>,
    /// queued | downloading | paused | done | error | cancelled
    pub state: String,
    pub itag: Option<i64>,
    pub mime: Option<String>,
    /// The requested quality until the format is resolved, then the actual one ("256 kbps AAC").
    pub quality_label: String,
    pub path: Option<String>,
    pub bytes_done: i64,
    pub bytes_total: Option<i64>,
    pub error: Option<String>,
    pub added_at: i64,
    pub updated_at: i64,
}

impl From<&DownloadRow> for DownloadItem {
    fn from(r: &DownloadRow) -> Self {
        DownloadItem {
            id: r.video_id.clone(),
            video_id: r.video_id.clone(),
            title: r.title.clone(),
            artists: r.artists.clone(),
            album: r.album.clone(),
            thumbnail: cover_of(r),
            state: r.state.clone(),
            itag: r.itag,
            mime: r.mime.clone(),
            quality_label: r.quality_label.clone(),
            path: r.path.clone(),
            bytes_done: r.bytes_done,
            bytes_total: r.bytes_total,
            error: r.error.clone(),
            added_at: r.added_at,
            updated_at: r.updated_at,
        }
    }
}

/// One offline album or playlist and its ordered membership and live counts.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadCollection {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub artwork_path: Option<String>,
    pub track_count: i64,
    pub downloaded_count: i64,
    pub video_ids: Vec<String>,
}

impl From<&DownloadCollectionRow> for DownloadCollection {
    fn from(row: &DownloadCollectionRow) -> Self {
        Self {
            id: row.id.clone(),
            kind: row.kind.clone(),
            title: row.title.clone(),
            subtitle: row.subtitle.clone(),
            artwork_path: row.artwork_path.clone(),
            track_count: row.track_count,
            downloaded_count: row.downloaded_count,
            video_ids: row.video_ids.clone(),
        }
    }
}

/// Initial command snapshot. The `downloads-changed` event intentionally remains the existing
/// coalesced track list; callers that need collection aggregates use this snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadSnapshot {
    pub items: Vec<DownloadItem>,
    pub collections: Vec<DownloadCollection>,
}

/// Ownership-aware collection-removal counts for a confirmation preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CollectionRemovalPreview {
    pub delete_count: i64,
    pub keep_count: i64,
}

/// A collection header and the selected tracks returned by an album/playlist browse.
#[derive(Debug, Clone)]
pub struct PlaylistDownload {
    pub items: Vec<innertube::SongItem>,
    pub collection: Option<CollectionMetadata>,
}

#[derive(Debug, Clone)]
pub struct CollectionMetadata {
    pub collection: NewDownloadCollection,
    pub artwork_url: Option<String>,
}

/// A playable upstream, resolved by the orchestrator (URL already deciphered, signed and
/// validated for the requesting quality).
#[derive(Debug, Clone)]
pub struct Source {
    pub url: String,
    pub headers: HashMap<String, String>,
    pub itag: i64,
    pub mime: Option<String>,
    pub bitrate: Option<i64>,
}

pub type ResolveFuture = Pin<Box<dyn std::future::Future<Output = Result<Source, String>> + Send>>;
type Resolver = Arc<dyn Fn(String, AudioQuality, bool) -> ResolveFuture + Send + Sync>;

/// Why a worker was asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// Keep the partial file; the row is going to `paused`.
    Pause,
    /// Delete the partial; the row is going to `cancelled` (or to nothing, on `remove`).
    Cancel,
}

// --- engine context ------------------------------------------------------------------------------

/// The downloads engine's whole shared state. `db` is the same `Db` the rest of the app uses;
/// nothing here holds another lock across an await.
pub struct Ctx {
    db: Arc<Db>,
    /// Used when no `download_dir` setting is set.
    default_dir: PathBuf,
    /// The audio-cache directory, so a hand-edited `download_dir` pointing inside it (which
    /// "Clear caches" would wipe) is re-rejected here, not only at `set_setting` time.
    cache_dir: PathBuf,
    app: Option<tauri::AppHandle>,
    resolve: Resolver,
    /// Free-space probe behind the preflight in `fetch_to_file` (see [`ensure_space`]); injected
    /// so tests can simulate a full disk without filling one.
    free_probe: fn(&Path) -> Option<u64>,
    /// ids a UI action asked to stop, checked between body chunks.
    signals: Mutex<HashMap<String, Stop>>,
    /// ids with a live worker, so a requeued id is never claimed twice.
    running: Mutex<HashSet<String>>,
    /// Wakes the supervisor after an enqueue/retry/resume (or a worker freeing a slot).
    notify: tokio::sync::Notify,
    /// What was emitted (bounded). The UI gets these via Tauri too; kept for tests.
    events: Mutex<Vec<(String, serde_json::Value)>>,
    /// `downloads-changed` throttling state: last send and whether a trailing send is scheduled.
    changed: Mutex<Changed>,
}

#[derive(Default)]
struct Changed {
    last: Option<Instant>,
    scheduled: bool,
}

impl Ctx {
    fn dir(&self) -> PathBuf {
        let configured = self
            .db
            .get_setting("download_dir")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && Path::new(s).is_absolute())
            .map(PathBuf::from)
            // A hand-edited `download_dir` inside the audio cache is re-rejected here: "Clear
            // caches" empties that folder, and downloads must never be wiped with it.
            .filter(|p| !inside(p, &self.cache_dir));
        configured.unwrap_or_else(|| self.default_dir.clone())
    }

    /// The folders a download may have been written to: the current `download_dir`, the app
    /// default, and every folder this engine has actually written into before. Deletion and
    /// partial sweeping stay inside these — never a path from the DB alone.
    fn app_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = vec![self.dir()];
        for d in self.known_dirs() {
            if !dirs.contains(&d) {
                dirs.push(d);
            }
        }
        if !dirs.contains(&self.default_dir) {
            dirs.push(self.default_dir.clone());
        }
        dirs
    }

    /// Folders this engine wrote into earlier, remembered under `download_dirs`. A hand-edited
    /// entry is re-checked exactly like `download_dir` (`Ctx::dir`): absolute, never the cache.
    fn known_dirs(&self) -> Vec<PathBuf> {
        let raw = self.db.get_setting(KNOWN_DIRS_KEY).unwrap_or_default();
        serde_json::from_str::<Vec<String>>(&raw)
            .unwrap_or_default()
            .into_iter()
            .map(PathBuf::from)
            .filter(|p| p.is_absolute() && !inside(p, &self.cache_dir))
            .collect()
    }

    /// Remember a folder this engine is about to write into, so a later `download_dir` change
    /// still lets remove/cancel/recovery reach the files that landed there (and sweeps them).
    fn remember_dir(&self, dir: &Path) {
        let mut known = self.known_dirs();
        if known.iter().any(|d| d == dir) {
            return;
        }
        known.push(dir.to_path_buf());
        if known.len() > KNOWN_DIRS_MAX {
            let overflow = known.len() - KNOWN_DIRS_MAX;
            known.drain(0..overflow);
        }
        let ids: Vec<String> = known.iter().map(|d| d.to_string_lossy().into_owned()).collect();
        if let Ok(json) = serde_json::to_string(&ids) {
            self.db.set_setting(KNOWN_DIRS_KEY, &json);
        }
    }

    fn stop(&self, id: &str) -> Option<Stop> {
        self.signals.lock().ok().and_then(|m| m.get(id).copied())
    }

    fn signal(&self, id: &str, stop: Stop) {
        if let Ok(mut m) = self.signals.lock() {
            m.insert(id.to_owned(), stop);
        }
    }

    fn signal_off(&self, id: &str) {
        if let Ok(mut m) = self.signals.lock() {
            m.remove(id);
        }
    }

    /// Wake the supervisor (a command or a worker just changed the queue).
    pub fn nudge(&self) {
        self.notify.notify_one();
    }

    fn emit(&self, event: &str, payload: serde_json::Value) {
        if let Some(app) = &self.app {
            use tauri::Emitter;
            let _ = app.emit(event, payload.clone());
        }
        if let Ok(mut events) = self.events.lock() {
            events.push((event.to_owned(), payload));
            if events.len() > 512 {
                events.remove(0);
            }
        }
    }

    fn emit_progress(&self, id: &str, done: u64, total: Option<u64>) {
        self.emit(
            "download-progress",
            serde_json::json!({ "id": id, "bytes_done": done, "bytes_total": total }),
        );
    }

    /// The full list, coalesced to [`CHANGED_INTERVAL`] with a guaranteed trailing send (a dropped
    /// last event would leave the UI showing a stale state forever).
    fn emit_changed(self: &Arc<Self>) {
        let now = Instant::now();
        let mut changed = match self.changed.lock() {
            Ok(c) => c,
            Err(_) => return,
        };
        let due = changed.last.map_or(true, |t| now.duration_since(t) >= CHANGED_INTERVAL);
        if due {
            changed.last = Some(now);
            drop(changed);
            self.emit_changed_now();
            return;
        }
        if changed.scheduled {
            return;
        }
        changed.scheduled = true;
        drop(changed);
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(CHANGED_INTERVAL).await;
            if let Ok(mut c) = ctx.changed.lock() {
                c.scheduled = false;
                c.last = Some(Instant::now());
            }
            ctx.emit_changed_now();
        });
    }

    fn emit_changed_now(&self) {
        let items: Vec<DownloadItem> = list(self);
        self.emit("downloads-changed", serde_json::json!(items));
    }
}

/// The process-wide engine. `start` runs in Tauri's setup; commands go through [`ctx`].
static CTX: std::sync::OnceLock<Arc<Ctx>> = std::sync::OnceLock::new();

/// The engine, once `start` has run.
pub fn ctx() -> Option<&'static Arc<Ctx>> {
    CTX.get()
}

/// The folders this engine writes finished files into: the configured `download_dir`, the app
/// default, and every folder it has remembered writing into (`Ctx::app_dirs`). The Local scan
/// skips their contents so a `download_dir` pointed into a watched Music folder never indexes
/// our own downloads as duplicate local tracks. Before `start` (unit tests, early startup) the
/// two settings still name where files would land, so the exclusion works without the engine.
pub fn storage_dirs(db: &Db) -> Vec<PathBuf> {
    if let Some(ctx) = CTX.get() {
        return ctx.app_dirs();
    }
    // Engine not started: settings-level fallback. Same shape as `Ctx::dir`/`Ctx::known_dirs`,
    // minus the cache-dir rejection (a `download_dir` hand-edited into the audio cache is not
    // where the engine writes anyway — `Ctx::dir` re-rejects it — and excluding the cache from
    // Local Music is right regardless).
    let mut dirs: Vec<PathBuf> = Vec::new();
    if let Some(d) = db.get_setting("download_dir") {
        let d = d.trim();
        if !d.is_empty() && Path::new(d).is_absolute() {
            dirs.push(PathBuf::from(d));
        }
    }
    let raw = db.get_setting(KNOWN_DIRS_KEY).unwrap_or_default();
    for d in serde_json::from_str::<Vec<String>>(&raw).unwrap_or_default() {
        let p = PathBuf::from(d);
        if p.is_absolute() && !dirs.contains(&p) {
            dirs.push(p);
        }
    }
    dirs
}

/// Build the engine, recover from the last run, and start the supervisor. Called once from
/// `lib.rs` setup.
pub fn start(state: Arc<crate::state::AppState>) {
    if CTX.get().is_some() {
        return;
    }
    let ctx = Arc::new(Ctx {
        db: state.db.clone(),
        default_dir: download_dir_of(&state),
        cache_dir: state.cache_dir().to_path_buf(),
        app: Some(state.app.clone()),
        resolve: prod_resolver(state),
        free_probe: free_space,
        signals: Mutex::new(HashMap::new()),
        running: Mutex::new(HashSet::new()),
        notify: tokio::sync::Notify::new(),
        events: Mutex::new(Vec::new()),
        changed: Mutex::new(Changed::default()),
    });
    if CTX.set(Arc::clone(&ctx)).is_err() {
        return;
    }
    recover(&ctx);
    // Saved covers reach the webview over the asset protocol, whose scope is empty every launch
    // (same reasoning as `local::allow_music_paths`): allow them before the window loads.
    if let Some(app) = &ctx.app {
        allow_artwork(app, &ctx.db);
    }
    tauri::async_runtime::spawn(supervisor(ctx));
}

/// `<app data>/downloads`: beside the SQLite file, never inside `audio-cache/`, so "Clear caches"
/// cannot wipe downloads.
fn download_dir_of(state: &crate::state::AppState) -> PathBuf {
    use tauri::Manager;
    state.app.path().app_data_dir().unwrap_or_else(|_| std::env::temp_dir()).join("downloads")
}

/// Whether downloads may borrow the signed-in account (`download_use_account`, default **off**).
///
/// Off, every network request the download itself makes — the `/player` resolve, the validation
/// probe, the byte fetch, the playlist walk — runs under [`innertube::anonymously`] and cannot
/// carry the user's Google/YouTube credentials even though playback is signed in. On, the user
/// has accepted the linkage (members-only and private lists need it) and the account identity is
/// used exactly as playback uses it.
pub fn use_account(db: &Db) -> bool {
    db.get_setting("download_use_account").is_some_and(|v| v.trim() == "true")
}

/// Drop anything that would authenticate a download's byte fetch: `Cookie`,
/// `Authorization` (SAPISIDHASH), `Proxy-Authorization`.
///
/// Defence in depth. The anonymous resolve already builds cookieless headers (the orchestrator's
/// `Identity::Anonymous` never sees the cookie); this makes the request builder itself refuse
/// them, so no future resolver — or header a server echoed back — can put account credentials on
/// a download even by mistake. Compared case-insensitively; every other header (User-Agent, …)
/// travels untouched.
pub fn anonymous_fetch_headers(headers: HashMap<String, String>) -> HashMap<String, String> {
    headers
        .into_iter()
        .filter(|(k, _)| {
            !matches!(
                k.trim().to_ascii_lowercase().as_str(),
                "cookie" | "authorization" | "proxy-authorization"
            )
        })
        .collect()
}

/// A resolve failure as the queue row should show it.
///
/// Anonymous (the default) turns every “YouTube wanted an account” verdict into one short
/// sentence: the playback wording would tell a signed-in user to sign in, which is not the fix
/// — the fix is the `download_use_account` setting. With the setting on, the account path keeps
/// the orchestrator's own wording unchanged.
pub fn failure_message(error: &crate::orchestrator::ResolveError, anonymous: bool) -> String {
    use crate::orchestrator::ResolveError as E;
    match error {
        E::SignInRequired(_) | E::UploadUnavailable(_) if anonymous => {
            "Sign-in required for this track".to_owned()
        }
        other => other.to_string(),
    }
}

/// The orchestrator as a resolver: same chain, same validation, same quality semantics as
/// playback; only the destination differs — and which identity it asks as (see [`use_account`]).
fn prod_resolver(state: Arc<crate::state::AppState>) -> Resolver {
    Arc::new(move |video_id, quality, is_upload| {
        let state = Arc::clone(&state);
        Box::pin(async move {
            let account = use_account(&state.db);
            let disabled = state.disabled_clients();
            let resolved = if account {
                state.orchestrator.resolve(&video_id, is_upload, quality, &disabled).await
            } else {
                // Anonymous, always — a failure stays a failure; there is no second attempt
                // with the account behind it (that silent fallback is the whole point of the
                // setting being a setting).
                state.orchestrator.resolve_anonymous(&video_id, is_upload, quality, &disabled).await
            };
            match resolved {
                Ok(d) => Ok(Source {
                    url: d.stream_url,
                    headers: if account { d.headers } else { anonymous_fetch_headers(d.headers) },
                    itag: d.itag,
                    mime: d.mime_type,
                    bitrate: d.bitrate,
                }),
                Err(e) => Err(failure_message(&e, !account)),
            }
        })
    })
}

/// Recover what the last run left: rows marked `downloading` go back to `queued` (their worker is
/// gone), `.part` files no live row could resume are swept, and cancelled rows keep no partial.
pub fn recover(ctx: &Ctx) {
    let pruned = ctx.db.prune_missing_collection_tracks();
    if pruned > 0 {
        tracing::info!(pruned, "downloads: removed memberships for missing rows");
    }
    let requeued = ctx.db.requeue_interrupted_downloads();
    if requeued > 0 {
        tracing::info!(requeued, "downloads: interrupted attempts re-queued");
    }
    let dirs = ctx.app_dirs();
    let mut wanted: HashSet<String> = HashSet::new();
    let mut covers: HashSet<PathBuf> = HashSet::new();
    for row in ctx.db.downloads_all() {
        match row.state.as_str() {
            // A partial that could still be resumed (queued/downloading will be re-resolved,
            // error is waiting on retry). The stem is the videoId, so a title change never makes
            // a refresh sweep a usable partial.
            "queued" | "downloading" | "error" | "paused" => {
                wanted.insert(row.video_id.clone());
            }
            "cancelled" => {
                for d in &dirs {
                    remove_parts_for_stem(d, &row.video_id);
                }
            }
            _ => {}
        }
        // Every recorded cover stays; files nothing points at are swept below.
        if let Some(art) = row.artwork_path.as_deref() {
            covers.insert(Path::new(art).canonicalize().unwrap_or_else(|_| PathBuf::from(art)));
        }
    }
    for collection in ctx.db.download_collections() {
        if let Some(art) = collection.artwork_path.as_deref() {
            covers.insert(Path::new(art).canonicalize().unwrap_or_else(|_| PathBuf::from(art)));
        }
    }
    for d in &dirs {
        sweep_orphan_parts(d, &wanted);
        sweep_orphan_artwork(d, &covers);
    }
}

/// Delete `.part` files whose stem matches nothing that could resume them.
fn sweep_orphan_parts(dir: &Path, wanted: &HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(stem) = part_stem_of(&name) else { continue };
        if !wanted.contains(stem) {
            tracing::debug!(file = %name, "downloads: sweeping an orphaned partial file");
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

// --- queries and enqueue -------------------------------------------------------------------------

/// Every download, oldest first (`downloads_list`).
pub fn list(ctx: &Ctx) -> Vec<DownloadItem> {
    ctx.db.downloads_all().iter().map(DownloadItem::from).collect()
}

/// The full command snapshot: per-track queue plus offline collection summaries.
pub fn snapshot(ctx: &Ctx) -> DownloadSnapshot {
    DownloadSnapshot {
        items: list(ctx),
        collections: ctx.db.download_collections().iter().map(DownloadCollection::from).collect(),
    }
}

/// Read the counts that `remove_collection` would produce, without changing rows or files.
pub fn collection_removal_preview(
    ctx: &Arc<Ctx>,
    id: &str,
) -> Result<CollectionRemovalPreview, String> {
    let (delete_count, keep_count) = ctx
        .db
        .preview_download_collection_removal(id)?
        .ok_or_else(|| format!("no such download collection: {id}"))?;
    Ok(CollectionRemovalPreview { delete_count, keep_count })
}

/// The effective quality for a request: the per-call value when given (validated), else the
/// `download_quality` setting, else HIGH.
pub fn picked_quality(db: &Db, requested: Option<&str>) -> Result<String, String> {
    match requested.map(|q| q.trim().to_ascii_uppercase()) {
        Some(q) => validate_quality(&q).map(|_| q),
        None => Ok(quality_setting(db)),
    }
}

fn validate_quality(q: &str) -> Result<(), String> {
    if matches!(q, "HIGH" | "LOW" | "AUTO") {
        Ok(())
    } else {
        Err(format!("unknown download quality: {q}"))
    }
}

fn quality_setting(db: &Db) -> String {
    match db.get_setting("download_quality").as_deref() {
        Some("LOW") => "LOW".to_owned(),
        Some("AUTO") => "AUTO".to_owned(),
        _ => "HIGH".to_owned(),
    }
}

/// Enqueue a batch (the single-track case is a batch of one). Returns one item per accepted entry,
/// in input order, deduped by videoId. A direct request marks every touched row standalone.
pub fn enqueue_items(
    ctx: &Arc<Ctx>,
    items: &[innertube::SongItem],
    quality: &str,
) -> Vec<DownloadItem> {
    let batch = selected_downloads(ctx, items, quality);
    let mut out = Vec::new();
    for (download, _, reset_done) in batch {
        match ctx.db.put_download(&download, reset_done) {
            Ok(row) => {
                ctx.signal_off(&row.video_id);
                out.push(DownloadItem::from(&row));
            }
            Err(e) => {
                tracing::warn!(video_id = %download.video_id, error = %e, "downloads: enqueue failed")
            }
        }
    }
    if !out.is_empty() {
        ctx.emit_changed();
        ctx.nudge();
    }
    out
}

/// Playlist sources that are not album/playlist browse ids still use collection ownership: they
/// are not direct requests, even though there is no persistent collection header for them.
pub fn enqueue_playlist_items(
    ctx: &Arc<Ctx>,
    items: &[innertube::SongItem],
    quality: &str,
) -> Vec<DownloadItem> {
    let batch = selected_downloads(ctx, items, quality);
    let mut out = Vec::new();
    for (download, _, reset_done) in batch {
        match ctx.db.put_playlist_download(&download, reset_done) {
            Ok(row) => {
                ctx.signal_off(&row.video_id);
                out.push(DownloadItem::from(&row));
            }
            Err(e) => {
                tracing::warn!(video_id = %download.video_id, error = %e, "downloads: playlist enqueue failed")
            }
        }
    }
    if !out.is_empty() {
        ctx.emit_changed();
        ctx.nudge();
    }
    out
}

/// Enqueue a collection's selected rows and memberships in one SQLite transaction.
pub fn enqueue_collection(
    ctx: &Arc<Ctx>,
    items: &[innertube::SongItem],
    quality: &str,
    collection: &NewDownloadCollection,
) -> Result<Vec<DownloadItem>, String> {
    let batch = selected_downloads(ctx, items, quality);
    let rows = ctx
        .db
        .put_collection_downloads(collection, &batch)
        .map_err(|e| format!("could not save offline collection: {e}"))?;
    let out: Vec<DownloadItem> = rows.iter().map(DownloadItem::from).collect();
    for item in &out {
        ctx.signal_off(&item.video_id);
    }
    // A collection with no valid tracks is still a harmless persisted header.
    ctx.emit_changed();
    if !out.is_empty() {
        ctx.nudge();
    }
    Ok(out)
}

fn selected_downloads(
    ctx: &Ctx,
    items: &[innertube::SongItem],
    quality: &str,
) -> Vec<(NewDownload, i64, bool)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        if !valid_video_id(&item.video_id) || !seen.insert(item.video_id.clone()) {
            continue;
        }
        let reset_done = ctx
            .db
            .download(&item.video_id)
            .is_some_and(|r| r.state == "done" && !file_present(r.path.as_deref()));
        out.push((
            NewDownload {
                video_id: item.video_id.clone(),
                title: item.title.clone(),
                artists: item.artists.clone(),
                album: item.album.clone(),
                thumbnail: item.thumbnail.clone(),
                quality: quality.to_owned(),
                is_upload: item.is_upload,
            },
            out.len() as i64,
            reset_done,
        ));
    }
    out
}

/// A videoId we will put in a file name and a URL: YouTube's own alphabet, nothing else. Rejects
/// empty ids, path-ish junk and the app's synthetic `LOCAL:` ids (a file on disk needs no
/// download).
fn valid_video_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Whether the path a row names is actually on disk.
fn file_present(path: Option<&str>) -> bool {
    path.is_some_and(|p| Path::new(p).is_file())
}

// --- UI actions ----------------------------------------------------------------------------------

fn remove_collection_action(ctx: &Arc<Ctx>, id: &str) -> Result<(), String> {
    let dirs = ctx.app_dirs();
    let mut stopped = Vec::new();
    let removed = ctx.db.remove_download_collection(id, |row| {
        ctx.signal(&row.video_id, Stop::Cancel);
        stopped.push(row.video_id.clone());
        if let Some(path) = row.path.as_deref() {
            if !delete_owned(&dirs, path) {
                return Err(CANNOT_DELETE.to_owned());
            }
        }
        // Saved track art is best-effort, matching the single-track remove action.
        if let Some(art) = row.artwork_path.as_deref() {
            let _ = delete_owned(&dirs, art);
        }
        for dir in &dirs {
            remove_parts_for_stem(dir, &row.video_id);
        }
        Ok(())
    });
    let collection = match removed {
        Ok(Some(collection)) => collection,
        Ok(None) => return Err(format!("no such download collection: {id}")),
        Err(error) => {
            for video_id in stopped {
                ctx.signal_off(&video_id);
            }
            return Err(error);
        }
    };
    if let Some(art) = collection.artwork_path.as_deref() {
        let _ = delete_owned(&dirs, art);
    }
    ctx.emit_changed();
    Ok(())
}

/// Apply one `downloads_action`. Unknown ids/actions are refused; everything else is idempotent-ish
/// (an action that does not apply to the row's state is a no-op, not an error).
pub fn action(ctx: &Arc<Ctx>, id: &str, action: &str) -> Result<(), String> {
    if action == "remove_collection" {
        return remove_collection_action(ctx, id);
    }
    let Some(row) = ctx.db.download(id) else {
        return Err(format!("no such download: {id}"));
    };
    let dirs = ctx.app_dirs();
    let pstem = row.video_id.clone();
    match action {
        "pause" => match row.state.as_str() {
            "downloading" => {
                ctx.signal(id, Stop::Pause);
                if !ctx.db.set_download_state_if(id, &["downloading"], "paused", None) {
                    // The worker finished (or something else moved the row) in the same instant;
                    // nothing is left to stop.
                    ctx.signal_off(id);
                }
            }
            "queued" => {
                ctx.db.set_download_state_if(id, &["queued"], "paused", None);
            }
            _ => {}
        },
        "resume" => {
            if row.state == "paused" {
                ctx.signal_off(id);
                if ctx.db.set_download_state_if(id, &["paused"], "queued", None) {
                    ctx.nudge();
                }
            }
        }
        "cancel" => {
            match row.state.as_str() {
                // The worker owns the open handle: it deletes the partial when it sees the stop.
                "downloading" => {
                    ctx.signal(id, Stop::Cancel);
                    ctx.db.cancel_download(id);
                }
                "done" => {
                    // Keep the cancellation as a record, but the file goes. The path is cleared so
                    // nothing can hand mpv a file that is about to be deleted.
                    if let Some(path) = row.path.as_deref() {
                        if !delete_owned(&dirs, path) {
                            // Nothing is cancelled: the row keeps its file and the UI hears why.
                            return Err(CANNOT_DELETE.to_owned());
                        }
                    }
                    // The saved cover goes with the audio (best-effort: it is app-managed junk
                    // once the row forgets it, and a leftover must not block the cancellation).
                    if let Some(art) = row.artwork_path.as_deref() {
                        let _ = delete_owned(&dirs, art);
                    }
                    ctx.db.cancel_download(id);
                }
                _ => {
                    ctx.db.cancel_download(id);
                    for d in &dirs {
                        remove_parts_for_stem(d, &pstem);
                    }
                }
            }
        }
        "retry" => {
            if matches!(row.state.as_str(), "error" | "cancelled") {
                ctx.signal_off(id);
                if ctx.db.set_download_state_if(id, &[row.state.as_str()], "queued", None) {
                    ctx.nudge();
                }
            }
        }
        "remove" => {
            ctx.signal(id, Stop::Cancel);
            // Files first, row second: a row deleted over a file that could not be deleted would
            // strand those bytes with nothing left to reach them. The containment check is the
            // same one `cancel` uses — a path outside the app's folders is never the answer.
            if let Some(path) = row.path.as_deref() {
                if !delete_owned(&dirs, path) {
                    ctx.signal_off(id);
                    return Err(CANNOT_DELETE.to_owned());
                }
            }
            // The saved cover is deleted with the row (best-effort, same containment): once the
            // row is gone nothing records it, and `recover` would sweep it as an orphan anyway.
            if let Some(art) = row.artwork_path.as_deref() {
                let _ = delete_owned(&dirs, art);
            }
            if ctx.db.delete_download(id).is_some() {
                for d in &dirs {
                    remove_parts_for_stem(d, &pstem);
                }
            }
        }
        other => return Err(format!("unknown download action: {other}")),
    }
    ctx.emit_changed();
    Ok(())
}

// --- playlists -----------------------------------------------------------------------------------

/// The tracks and header a `download_playlist` call should save: a YouTube playlist or album
/// (paged through continuation tokens up to `count`), a playlist kept on this machine, or On Repeat.
pub async fn collect_playlist(
    state: &Arc<crate::state::AppState>,
    playlist_id: &str,
    count: Option<u32>,
) -> Result<PlaylistDownload, String> {
    let cap = count.map(|n| (n as usize).min(MAX_PLAYLIST_ITEMS)).unwrap_or(MAX_PLAYLIST_ITEMS);
    if cap == 0 {
        return Ok(PlaylistDownload { items: Vec::new(), collection: None });
    }
    // The lists this machine owns need no network at all and are not browse-id collections.
    if let Some(key) = playlist_id.strip_prefix(crate::state::LOCAL_PLAYLIST_PREFIX) {
        let key: i64 = key.parse().map_err(|_| "not a local playlist".to_owned())?;
        return Ok(PlaylistDownload {
            items: local_playlist_songs(&state.db, key).into_iter().take(cap).collect(),
            collection: None,
        });
    }
    if playlist_id == crate::state::ON_REPEAT_ID {
        return Ok(PlaylistDownload {
            items: on_repeat_songs(&state.db).into_iter().take(cap).collect(),
            collection: None,
        });
    }
    // The listing is part of the download action, so it goes out under the same identity as the
    // bytes: with the account out of downloads (the default) the walk is anonymous too — a
    // public album or playlist answers a signed-out browse with the same rows. Private lists are
    // what `download_use_account` is for. Inside the scope the browse cannot mistake YouTube's
    // signed-out shape for a dead session either (transport + `browse` both check the scope).
    let anonymous = !use_account(&state.db);
    let walk = async {
        let client = state
            .clients
            .get(innertube::METADATA_CLIENT)
            .ok_or_else(|| "metadata client missing".to_owned())?;
        // An album page lists its tracks in one response (YouTube does not page albums the way it
        // pages playlists).
        if playlist_id.starts_with("MPRE") {
            let page = state.it.album(client, playlist_id).await.map_err(|e| e.to_string())?;
            let items: Vec<_> = page.items.into_iter().take(cap).collect();
            return Ok(PlaylistDownload {
                collection: collection_metadata(
                    playlist_id,
                    "album",
                    page.title,
                    page.subtitle,
                    page.thumbnail,
                ),
                items,
            });
        }
        let mut page =
            state.it.playlist(client, playlist_id, None).await.map_err(|e| e.to_string())?;
        let title = page.title.clone();
        let subtitle = page.subtitle.clone();
        let artwork = page.thumbnail.clone();
        let mut items = std::mem::take(&mut page.items);
        items.truncate(cap);
        let mut token = page.continuation;
        let mut pages = 1;
        while items.len() < cap && pages < MAX_PLAYLIST_PAGES {
            let Some(next_token) = token else { break };
            let next = match state.it.playlist_continuation(client, &next_token).await {
                Ok(next) => next,
                // A page that fails ends the walk: a short download beats a hung command.
                Err(e) => {
                    tracing::warn!(error = %e, "downloads: playlist walk stopped early");
                    break;
                }
            };
            if next.items.is_empty() {
                break;
            }
            items.extend(next.items);
            items.truncate(cap);
            token = next.continuation;
            pages += 1;
        }
        Ok(PlaylistDownload {
            collection: collection_metadata(playlist_id, "playlist", title, subtitle, artwork),
            items,
        })
    };
    if anonymous {
        let visitor = state.it.anonymous_visitor().await;
        innertube::anonymously(visitor, walk).await
    } else {
        walk.await
    }
}

fn collection_metadata(
    id: &str,
    kind: &str,
    title: Option<String>,
    subtitle: Option<String>,
    artwork_url: Option<String>,
) -> Option<CollectionMetadata> {
    if !valid_collection_id(id) {
        return None;
    }
    Some(CollectionMetadata {
        collection: NewDownloadCollection {
            id: id.to_owned(),
            kind: kind.to_owned(),
            title: title.filter(|s| !s.trim().is_empty()).unwrap_or_else(|| id.to_owned()),
            subtitle,
        },
        artwork_url,
    })
}

/// A collection id is only used as a DB key and a filename component for its cover. It must be a
/// bounded YouTube browse-id token, never a path or URL supplied by the caller.
fn valid_collection_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A playlist kept on this machine, in its stored order.
fn local_playlist_songs(db: &Db, key: i64) -> Vec<innertube::SongItem> {
    db.local_playlist_tracks(key)
        .into_iter()
        .filter_map(|(_, json)| serde_json::from_str::<innertube::SongItem>(&json).ok())
        .collect()
}

/// On Repeat, most-played first — the same rows `commands::on_repeat_songs` builds.
fn on_repeat_songs(db: &Db) -> Vec<innertube::SongItem> {
    let since = crate::db::now_secs() - crate::state::ON_REPEAT_WINDOW_SECS;
    db.top_plays(since, crate::state::ON_REPEAT_LIMIT)
        .into_iter()
        .filter_map(|(json, _)| serde_json::from_str::<innertube::SongItem>(&json).ok())
        .collect()
}

// --- settings ------------------------------------------------------------------------------------

/// The value `set_setting` accepts for a `download_*` key. Kept beside the engine so the three
/// knobs and their bounds live in one place.
pub fn validate_setting(key: &str, value: &str, cache_dir: &Path) -> Result<(), String> {
    match key {
        "download_quality" => validate_quality(value),
        "download_concurrency" => value
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|n| (1..=4).contains(n))
            .map(|_| ())
            .ok_or_else(|| "download_concurrency must be 1, 2, 3 or 4".to_owned()),
        "download_dir" => {
            let value = value.trim();
            if value.is_empty() {
                return Ok(()); // clears back to the default folder
            }
            let path = Path::new(value);
            if !path.is_absolute() {
                return Err("download_dir must be an absolute path".to_owned());
            }
            // The downloads folder must never sit inside the audio cache: "Clear caches" empties
            // that directory file by file.
            if inside(path, cache_dir) {
                return Err("download_dir cannot be inside the audio cache".to_owned());
            }
            Ok(())
        }
        "download_use_account" => match value.trim() {
            "true" | "false" => Ok(()),
            _ => Err("download_use_account must be true or false".to_owned()),
        },
        _ => Ok(()),
    }
}

/// Whether `path` is (or is inside) `root`, comparing what exists on disk when it can and
/// case-insensitive text otherwise. Windows paths are case-insensitive, and `canonicalize` on
/// Windows hands back a `\?\`-prefixed verbatim path that a raw user path never has.
fn inside(path: &Path, root: &Path) -> bool {
    fn norm(p: &Path) -> String {
        let p = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
        let s = p.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/").to_lowercase();
        let s = s.strip_prefix("//?/").unwrap_or(&s).to_string();
        let s = s.strip_prefix("//./").unwrap_or(&s).to_string();
        s.trim_end_matches('/').to_string()
    }
    let (path, root) = (norm(path), norm(root));
    !root.is_empty() && (path == root || path.starts_with(&format!("{root}/")))
}

/// How many downloads may run at once. The setting is validated at write time; a hand-edited
/// database still gets a bounded answer.
pub fn concurrency(db: &Db) -> usize {
    db.get_setting("download_concurrency")
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| (1..=4).contains(n))
        .unwrap_or(2)
}

// --- offline playback ----------------------------------------------------------------------------

/// The playback data for a downloaded track, or `None` when there is nothing to play from disk.
///
/// Called by `AppState::resolve` before the URL cache: a downloaded videoId never touches the
/// network, carries no watch-history ping, and never expires. A row whose file has gone flips to
/// `error` here rather than pretending to be playable.
pub fn offline_playback(db: &Db, video_id: &str) -> Option<crate::orchestrator::PlaybackData> {
    let row = db.download(video_id)?;
    if row.state != "done" {
        return None;
    }
    // Resolved before `row.path` is moved out below: the saved cover when it is on disk —
    // offline playback is exactly where the upstream URL would not load.
    let cover = cover_of(&row);
    let path = row.path?;
    if !Path::new(&path).is_file() {
        tracing::warn!(video_id, path, "downloads: the saved file is missing; asking for a retry");
        if db.set_download_state_if(video_id, &["done"], "error", Some(MISSING_FILE)) {
            if let Some(ctx) = ctx() {
                ctx.emit_changed();
            }
        }
        return None;
    }
    Some(crate::orchestrator::PlaybackData {
        video_id: video_id.to_owned(),
        stream_url: path,
        itag: row.itag.unwrap_or(0),
        headers: HashMap::new(),
        // Never expires, and never enters the URL cache.
        expires_in_seconds: i64::MAX / 2,
        loudness_db: None,
        // A file on disk is not a play to register with YouTube.
        playback_ping: None,
        title: Some(row.title),
        artists: Some(row.artists),
        duration: None,
        thumbnail: cover,
        is_video: None,
        stream_client: "download".to_owned(),
        mime_type: row.mime,
        bitrate: None,
    })
}

const MISSING_FILE: &str = "the downloaded file is missing";

// --- file naming ---------------------------------------------------------------------------------

/// A file name stem for a track: the title with every path-hostile character folded to a space,
/// followed by `[videoId]` so two tracks with one title stay apart and the id can be read back.
fn file_stem(title: &str, video_id: &str) -> String {
    let mut cleaned = String::with_capacity(title.len());
    for c in title.chars() {
        if c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') {
            cleaned.push(' ');
        } else {
            cleaned.push(c);
        }
    }
    let cleaned: String = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    let cleaned: String =
        cleaned.trim_matches(|c: char| c == '.' || c == ' ').chars().take(120).collect();
    let cleaned = cleaned.trim_end_matches('.').trim_end();
    if cleaned.is_empty() {
        // The id is already whitelisted to `[A-Za-z0-9_-]`, so on its own it is always a safe name.
        video_id.to_owned()
    } else {
        format!("{cleaned} [{video_id}]")
    }
}

/// `stem.itag<N>.part`: the itag in the name is what keeps a resume honest — a `.part` written by
/// one format is never appended to by another.
fn part_name(stem: &str, itag: i64) -> String {
    format!("{stem}.itag{itag}.part")
}

/// The stem a `.part` file belongs to, when it is one of ours.
fn part_stem_of(name: &str) -> Option<&str> {
    let name = name.strip_suffix(".part")?;
    name.rsplit_once(".itag")
        .map(|(stem, itag)| {
            // The suffix has to be the numeric itag, or the file is not ours to sweep.
            if itag.is_empty() || !itag.chars().all(|c| c.is_ascii_digit()) {
                ""
            } else {
                stem
            }
        })
        .filter(|s| !s.is_empty())
}

/// The extension for a mime type, from a fixed set. Anything unknown lands on `m4a` — every
/// YouTube Music format is mp4 or webm, and mpv sniffs the content anyway.
fn ext_for_mime(mime: &str) -> &'static str {
    let m = mime.to_ascii_lowercase();
    if m.contains("webm") {
        "webm"
    } else if m.contains("opus") {
        "opus"
    } else if m.contains("mp4") || m.contains("m4a") {
        "m4a"
    } else if m.contains("mpeg") {
        "mp3"
    } else if m.contains("ogg") {
        "ogg"
    } else {
        "m4a"
    }
}

/// The actual resolved format, for `quality_label` ("160 kbps Opus"). No transcode is involved:
/// this only names what was picked.
fn actual_label(src: &Source) -> String {
    let mime = src.mime.as_deref().unwrap_or_default().to_ascii_lowercase();
    let codec = if mime.contains("opus") {
        "Opus"
    } else if mime.contains("mp4a") || mime.contains("mp4") {
        "AAC"
    } else if mime.contains("mpeg") {
        "MP3"
    } else if mime.contains("vorbis") {
        "Vorbis"
    } else {
        "audio"
    };
    match src.bitrate {
        Some(b) if b > 0 => format!("{} kbps {}", b / 1000, codec),
        _ => codec.to_owned(),
    }
}

// --- filesystem helpers --------------------------------------------------------------------------

fn stem_of(row: &DownloadRow) -> String {
    file_stem(&row.title, &row.video_id)
}

/// Sum the `.part` files of one stem (used to restore `bytes_done` after a stop).
fn parts_len(dir: &Path, stem: &str) -> u64 {
    let prefix = format!("{stem}.itag");
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with(&prefix) && name.ends_with(".part")
        })
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .sum()
}

/// Delete every `.part` of one stem (used by cancel/remove).
fn remove_parts_for_stem(dir: &Path, stem: &str) {
    let prefix = format!("{stem}.itag");
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".part") {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Keep only the partial for the resolved itag: a `.part` from a different format is dead weight
/// (and must never be appended to).
fn sweep_other_parts(dir: &Path, stem: &str, keep: &Path) {
    let prefix = format!("{stem}.itag");
    let keep_name = keep.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".part") && name != keep_name {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Adopt `<videoId>.itag<N>.part` from an earlier folder into the one this attempt writes to.
/// Only the exact name moves — the itag in it is what makes the resume honest. A folder on
/// another volume cannot be renamed onto, and leaving it behind is exactly the leak this exists
/// to prevent, so the bytes are dropped there and the download restarts instead.
fn adopt_part(from: &Path, part: &Path) {
    let Some(name) = part.file_name() else { return };
    let src = from.join(name);
    if src == part || !src.is_file() {
        return;
    }
    if std::fs::rename(&src, part).is_err() {
        let _ = std::fs::remove_file(&src);
    }
}

/// The message the UI shows when a finished file cannot be deleted: the row is kept (so the
/// bytes stay reachable and the promise of `remove` is not quietly broken) and this says why.
const CANNOT_DELETE: &str =
    "the file is outside the downloads folders, so nothing was removed from the list";

/// Delete a finished file the app owns, answering whether nothing is left behind. A file that is
/// already gone is a success (there is nothing to leak); a file outside every folder the engine
/// has written into is refused by [`safe_remove`] and reported as a failure, so the caller keeps
/// the row instead of forgetting bytes it never deleted.
fn delete_owned(roots: &[PathBuf], path: &str) -> bool {
    match std::fs::metadata(path) {
        // Nothing on disk (or a path string that names nothing at all): there is no byte left to
        // leak, so the row may go.
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
            ) =>
        {
            return true;
        }
        // Something is there that this process cannot even stat — keep the row, say so.
        Err(_) => {
            tracing::warn!(path, "downloads: cannot check a finished file; keeping its row");
            return false;
        }
        Ok(_) => {}
    }
    let ok = safe_remove(roots, path);
    if !ok {
        tracing::warn!(path, "downloads: refusing to delete a finished file outside the folders");
    }
    ok
}

/// Delete a file only when it really is inside the downloads folder. A path from the database is
/// not trusted on its own: a tampered row must not be able to delete an arbitrary file.
fn safe_remove(roots: &[PathBuf], path: &str) -> bool {
    let Ok(real) = Path::new(path).canonicalize() else { return false };
    let inside_a_root =
        roots.iter().any(|r| r.canonicalize().map(|c| real.starts_with(&c)).unwrap_or(false));
    if !inside_a_root {
        tracing::warn!(path, "downloads: refusing to delete a file outside the downloads folders");
        return false;
    }
    std::fs::remove_file(&real).is_ok()
}

/// The atomic commit: the partial is renamed onto its final name (replacing a stale file of the
/// same name, which can only be a leftover from a removed row).
fn commit(part: &Path, final_path: &Path) -> std::io::Result<()> {
    if final_path.exists() {
        std::fs::remove_file(final_path)?;
    }
    std::fs::rename(part, final_path)
}

// --- free-space preflight -------------------------------------------------------------------------

/// Free bytes on the volume holding `dir`, or `None` when the platform cannot say (which never
/// blocks a download — see [`ensure_space`]). No new dependency: the `windows` crate this app
/// already ships provides `GetDiskFreeSpaceExW`, and unix answers with `statvfs` from `libc`,
/// which was already in the tree for Linux.
#[cfg(windows)]
fn free_space(dir: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows::core::PCWSTR;
    use windows::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain(std::iter::once(0)).collect();
    let mut avail: u64 = 0;
    let mut total: u64 = 0;
    let mut total_free: u64 = 0;
    unsafe {
        GetDiskFreeSpaceExW(
            PCWSTR::from_raw(wide.as_ptr()),
            Some(&mut avail as *mut u64),
            Some(&mut total as *mut u64),
            Some(&mut total_free as *mut u64),
        )
    }
    .ok()?;
    Some(avail)
}

#[cfg(unix)]
fn free_space(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;

    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut buf: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut buf) } != 0 {
        return None;
    }
    // `f_bavail` is counted in `f_frsize`-sized blocks; some filesystems leave it zero, in which
    // case the basic block size is the same unit.
    let unit = if buf.f_frsize > 0 { buf.f_frsize } else { buf.f_bsize };
    Some(buf.f_bavail.saturating_mul(unit as u64))
}

/// Free-space preflight for one upcoming write of `need` bytes: the window plus
/// [`SPACE_RESERVE`] must fit, or the download fails before the request even goes out — with a
/// readable sentence, leaving the partial exactly as it was (no crash, no half-written window).
///
/// The probe answering `None` (exotic filesystem, a transient API failure) skips the check rather
/// than blocking every download on a measurement problem: a disk that really is full still fails
/// at the first `write`, which this module already reports as a readable error.
fn ensure_space(probe: fn(&Path) -> Option<u64>, dir: &Path, need: u64) -> Result<(), FetchError> {
    let Some(free) = probe(dir) else { return Ok(()) };
    let required = need.saturating_add(SPACE_RESERVE);
    if free < required {
        return Err(FetchError::Io(format!(
            "not enough free space: {free} bytes free, {required} needed"
        )));
    }
    Ok(())
}

// --- offline artwork ------------------------------------------------------------------------------

/// Fetch the cover for a finished download into `<download dir>/artwork/<videoId>.<ext>` and
/// record it on the row, so downloaded rows keep their artwork with no network at all. The
/// folder-per-download layout follows Adiker's fork (their downloads.rs:742-753, GPL-3.0,
/// Adiker/limusic@1fb1ded).
///
/// Purely best-effort by contract: the row is already `done` when this runs, every failure is
/// swallowed (debug-logged), and the whole fetch is bounded by [`ARTWORK_TIMEOUT`] — artwork
/// never fails a download and never delays one beyond that bound.
async fn save_artwork(ctx: &Ctx, row: &DownloadRow, dir: &Path) {
    let Some(url) = row.thumbnail.as_deref().filter(|u| u.starts_with("http")) else {
        return;
    };
    let Some(path) = save_artwork_file(ctx, &row.video_id, url, dir).await else { return };
    if !ctx.db.set_download_artwork(&row.video_id, &path.to_string_lossy()) {
        // The row moved on (removed or cancelled while we fetched): keep the database honest
        // and drop the file nothing points at.
        let _ = std::fs::remove_file(&path);
        return;
    }
    // A re-download that changed `download_dir` leaves the previous run's cover behind — it is
    // app-managed now that nothing records it. Containment enforced by `delete_owned`.
    if let Some(old) = row.artwork_path.as_deref() {
        if old != path.to_string_lossy() {
            let _ = delete_owned(&ctx.app_dirs(), old);
        }
    }
    tracing::debug!(video_id = %row.video_id, path = %path.to_string_lossy(), "downloads: cover saved for offline use");
}

/// Collection covers share the same bounded fetch, safe filename, storage folder and asset-scope
/// setup as per-track artwork. This runs from a detached task; every failure is intentionally
/// swallowed and never blocks the queued tracks.
pub async fn save_collection_artwork(ctx: &Arc<Ctx>, id: &str, url: &str) {
    if !valid_collection_id(id) || !url.starts_with("http") {
        return;
    }
    let dir = ctx.dir();
    let Some(path) = save_artwork_file(ctx, id, url, &dir).await else { return };
    let path_string = path.to_string_lossy().into_owned();
    let old = ctx.db.collection_artwork_path(id);
    if !ctx.db.set_collection_artwork(id, &path_string) {
        let _ = std::fs::remove_file(&path);
        return;
    }
    if let Some(old) = old.filter(|old| old != &path_string) {
        let _ = delete_owned(&ctx.app_dirs(), &old);
    }
    ctx.emit_changed();
}

/// Save a cover under the app-managed artwork directory. `stem` is validated again here so a
/// future caller cannot turn a DB value into a path traversal.
async fn save_artwork_file(ctx: &Ctx, stem: &str, url: &str, dir: &Path) -> Option<PathBuf> {
    if !valid_artwork_stem(stem) {
        return None;
    }
    let fetched = fetch_cover(url).await;
    let Some((ext, bytes)) = fetched else {
        tracing::debug!(stem, "downloads: no cover saved (fetch failed or not an image)");
        return None;
    };
    let art_dir = dir.join(ARTWORK_DIR);
    if let Err(e) = std::fs::create_dir_all(&art_dir) {
        tracing::debug!(error = %e, "downloads: could not create the artwork folder");
        return None;
    }
    let path = art_dir.join(format!("{stem}.{ext}"));
    if let Err(e) = std::fs::write(&path, &bytes) {
        tracing::debug!(error = %e, "downloads: could not save the cover");
        return None;
    }
    if let Some(app) = &ctx.app {
        use tauri::Manager;
        let scope = app.asset_protocol_scope();
        let _ = scope.allow_file(&path);
        if let Ok(real) = path.canonicalize() {
            let _ = scope.allow_file(real);
        }
    }
    Some(path)
}

fn valid_artwork_stem(stem: &str) -> bool {
    !stem.is_empty()
        && stem.len() <= 128
        && stem.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// One cover body: `(extension, bytes)`. A non-image answer (an HTML error page served with 200,
/// a JSON body) is refused rather than saved as a broken `.jpg`; a body over [`ARTWORK_MAX`] is
/// refused rather than streamed to disk. Bounded by [`ARTWORK_TIMEOUT`] end to end.
async fn fetch_cover(url: &str) -> Option<(&'static str, Vec<u8>)> {
    let resp = crate::http::client().get(url).timeout(ARTWORK_TIMEOUT).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    if resp.content_length().is_some_and(|len| len > ARTWORK_MAX) {
        return None;
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.is_empty() && !content_type.starts_with("image/") {
        return None;
    }
    let ext = if content_type.contains("png") {
        "png"
    } else if content_type.contains("webp") {
        "webp"
    } else {
        "jpg"
    };
    let bytes = resp.bytes().await.ok()?;
    (bytes.len() as u64 <= ARTWORK_MAX).then_some((ext, bytes.to_vec()))
}

/// Allow every saved cover on the asset protocol at startup: the scope is empty on every launch,
/// and the Downloads page or a restored queue asks for the artwork before any new fetch could
/// re-allow it (same reasoning as `local::allow_music_paths`).
fn allow_artwork(app: &tauri::AppHandle, db: &Db) {
    use tauri::Manager;
    let scope = app.asset_protocol_scope();
    let downloads = db.downloads_all();
    let collections = db.download_collections();
    for art in downloads
        .iter()
        .filter_map(|r| r.artwork_path.as_deref())
        .chain(collections.iter().filter_map(|c| c.artwork_path.as_deref()))
    {
        let _ = scope.allow_file(art);
        if let Ok(real) = Path::new(art).canonicalize() {
            let _ = scope.allow_file(real);
        }
    }
}

/// The cover to display for a download: the saved local file when it is still on disk — the
/// offline case, where the upstream URL is unreachable — else the upstream thumbnail.
fn cover_of(r: &DownloadRow) -> Option<String> {
    r.artwork_path
        .as_deref()
        .filter(|p| Path::new(p).is_file())
        .map(str::to_owned)
        .or_else(|| r.thumbnail.clone())
}

/// Delete cover files in `<dir>/artwork` that no row records any more: a crash between the file
/// write and the record, or a removal that half-landed, strands one otherwise forever. Only that
/// own subfolder is ever read, and only files verified against the database are removed.
fn sweep_orphan_artwork(dir: &Path, referenced: &HashSet<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir.join(ARTWORK_DIR)) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let real = path.canonicalize().unwrap_or_else(|_| path.clone());
        if !referenced.contains(&real) {
            tracing::info!(path = %path.to_string_lossy(), "downloads: dropping an unreferenced cover");
            let _ = std::fs::remove_file(&path);
        }
    }
}

// --- transport -----------------------------------------------------------------------------------

#[derive(Debug)]
enum FetchError {
    /// A pause/cancel asked the worker to stop. The caller reconciles the partial file.
    Stopped,
    /// The server will not serve this resume (it ignored the range on a partial, or the partial is
    /// longer than the file now is): start over.
    Stale,
    /// 403/410 mid-file: the signed URL expired. The caller re-resolves and continues.
    Auth,
    Status(u16),
    /// A window's body ended before its own `Content-Range` promised.
    Short,
    /// A range answer that does not line up with what was asked.
    Inconsistent,
    /// Local or network I/O failure, with a short human message (never a URL).
    Io(String),
}

impl FetchError {
    fn message(&self) -> String {
        match self {
            FetchError::Stopped => "stopped".to_owned(),
            FetchError::Stale => "the server would not continue this download".to_owned(),
            FetchError::Auth => "the stream URL expired".to_owned(),
            FetchError::Status(code) => format!("the stream server answered HTTP {code}"),
            FetchError::Short => "the download ended early".to_owned(),
            FetchError::Inconsistent => "the stream server sent an unexpected range".to_owned(),
            FetchError::Io(m) => m.clone(),
        }
    }
}

/// One bounded range request, with the stall guard on the send. `Range` and `Accept-Encoding`
/// travel together on purpose: without the explicit `identity`, the server may answer a 200 with
/// an encoded whole file, which is the throttled shape this whole module exists to avoid.
async fn request_window(
    src: &Source,
    start: u64,
    last: u64,
) -> Result<reqwest::Response, FetchError> {
    let mut req = crate::http::client()
        .get(&src.url)
        .header(reqwest::header::RANGE.as_str(), format!("bytes={start}-{last}"))
        .header(reqwest::header::ACCEPT_ENCODING.as_str(), "identity");
    for (k, v) in &src.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    match crate::audioproxy::no_stall(req.send()).await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(_)) => Err(FetchError::Io("the stream server could not be reached".to_owned())),
        Err(_) => Err(FetchError::Io("the stream server did not respond".to_owned())),
    }
}

/// `Content-Range: bytes <start>-<end>/<total>`; `total` may be `*`.
fn parse_content_range(headers: &reqwest::header::HeaderMap) -> Option<(u64, u64, Option<u64>)> {
    let value = headers.get(reqwest::header::CONTENT_RANGE)?.to_str().ok()?;
    let value = value.trim().strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let total = total.trim().parse::<u64>().ok();
    let (start, end) = range.split_once('-')?;
    Some((start.trim().parse().ok()?, end.trim().parse().ok()?, total))
}

/// The total out of a 416's `Content-Range: bytes */<total>`.
fn unsatisfied_total(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::CONTENT_RANGE)?
        .to_str()
        .ok()?
        .split_once('/')?
        .1
        .trim()
        .parse()
        .ok()
}

/// Reads one response body into the file, committing progress as it goes.
struct Pump<'a> {
    ctx: &'a Ctx,
    row: &'a DownloadRow,
    total: Option<u64>,
    last_pos: u64,
    last_commit: Instant,
    /// How long a single read may make no progress (tests shrink it).
    stall: Duration,
}

impl Pump<'_> {
    /// Stream one response into `file`. `expected_last` is the window's inclusive last byte for a
    /// 206 (the body must deliver exactly that many bytes), `None` for a whole-body 200.
    /// Returns how many bytes the file holds after this window.
    async fn window(
        &mut self,
        file: &mut std::fs::File,
        resp: reqwest::Response,
        start: u64,
        expected_last: Option<u64>,
    ) -> Result<u64, FetchError> {
        let mut pos = start;
        let mut body = resp.bytes_stream();
        let deadline = tokio::time::Instant::now() + WINDOW_TIMEOUT;
        loop {
            if self.ctx.stop(&self.row.video_id).is_some() {
                return Err(FetchError::Stopped);
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(FetchError::Io("the download timed out".to_owned()));
            }
            let chunk = match tokio::time::timeout(self.stall.min(left), body.next()).await {
                Err(_) => return Err(FetchError::Io("the download stalled".to_owned())),
                Ok(None) => break,
                Ok(Some(Err(_))) => {
                    return Err(FetchError::Io("the connection dropped".to_owned()))
                }
                Ok(Some(Ok(chunk))) => chunk,
            };
            file.write_all(&chunk)
                .map_err(|e| FetchError::Io(format!("could not write the file: {}", e.kind())))?;
            pos += chunk.len() as u64;
            if let Some(last) = expected_last {
                if pos > last + 1 {
                    return Err(FetchError::Inconsistent);
                }
            }
            if pos.saturating_sub(self.last_pos) >= PROGRESS_STEP
                || self.last_commit.elapsed() >= PROGRESS_INTERVAL
            {
                self.commit(pos)?;
            }
        }
        if let Some(last) = expected_last {
            if pos != last + 1 {
                return Err(FetchError::Short);
            }
        }
        self.commit(pos)?;
        Ok(pos)
    }

    /// Persist and emit progress. A refused write means a pause/cancel won: stop.
    fn commit(&mut self, pos: u64) -> Result<(), FetchError> {
        self.last_pos = pos;
        self.last_commit = Instant::now();
        if !self.ctx.db.update_download_progress(
            &self.row.video_id,
            pos as i64,
            self.total.map(|t| t as i64),
        ) {
            return Err(FetchError::Stopped);
        }
        self.ctx.emit_progress(&self.row.video_id, pos, self.total);
        Ok(())
    }
}

/// Fetch the whole file into `part`, continuing from `resume` (the partial's current size).
/// Returns the total file size.
async fn fetch_to_file(
    ctx: &Ctx,
    row: &DownloadRow,
    src: &Source,
    part: &Path,
    resume: u64,
    stall: Duration,
) -> Result<u64, FetchError> {
    // Append: `resume` is the file's own length, so the existing bytes stay exactly where they are.
    let mut file =
        std::fs::OpenOptions::new().create(true).append(true).open(part).map_err(|e| {
            FetchError::Io(format!("could not open the partial file: {}", e.kind()))
        })?;
    let mut pump =
        Pump { ctx, row, total: None, last_pos: resume, last_commit: Instant::now(), stall };
    let mut pos = resume;
    loop {
        // Free-space preflight: this window's bytes plus the reserve must fit before the request
        // goes out. A full disk thus fails the item with a readable sentence (and keeps the
        // partial as-is) instead of dying mid-write. Unknown free space never blocks (ensure_space).
        let window = pump.total.map_or(CHUNK, |t| t.saturating_sub(pos).min(CHUNK));
        if let Some(dir) = part.parent() {
            ensure_space(ctx.free_probe, dir, window)?;
        }
        let last = pos.saturating_add(CHUNK - 1);
        let resp = request_window(src, pos, last).await?;
        match resp.status() {
            reqwest::StatusCode::PARTIAL_CONTENT => {
                let (start, end, total) =
                    parse_content_range(resp.headers()).ok_or(FetchError::Inconsistent)?;
                if start != pos || end < start {
                    return Err(FetchError::Inconsistent);
                }
                if let Some(t) = total {
                    if pump.total.is_some_and(|known| known != t) {
                        return Err(FetchError::Inconsistent);
                    }
                    pump.total = Some(t);
                }
                pos = pump.window(&mut file, resp, pos, Some(end)).await?;
            }
            reqwest::StatusCode::OK => {
                // A server that ignores Range. Only usable from the beginning: anything else means
                // this partial cannot be continued and has to start over.
                if pos > 0 {
                    return Err(FetchError::Stale);
                }
                let len = resp.content_length();
                pos = pump.window(&mut file, resp, pos, None).await?;
                if let Some(t) = len.or(pump.total) {
                    pump.total = Some(t);
                    if pos != t {
                        return Err(FetchError::Short);
                    }
                }
                break;
            }
            reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
                // `bytes */total`. The bytes on disk being exactly the whole file is the one
                // complete answer that can hide behind a 416; everything else is a stale partial.
                // Compare against `pos` — the bytes this attempt holds *now* — not `resume`: the
                // windows may have grown the file past `resume` inside this attempt (a server
                // that omits the complete length from its 206s gives the loop nothing else to
                // stop on), and a 416 whose total equals `pos` says those bytes are the whole file.
                let total = unsatisfied_total(resp.headers());
                return match total {
                    // The partial is already the whole file; nothing left to fetch.
                    Some(t) if pos > 0 && t == pos => Ok(t),
                    _ => Err(FetchError::Stale),
                };
            }
            status
                if status == reqwest::StatusCode::FORBIDDEN
                    || status == reqwest::StatusCode::GONE =>
            {
                return Err(FetchError::Auth)
            }
            status => return Err(FetchError::Status(status.as_u16())),
        }
        if pump.total.is_some_and(|t| pos >= t) {
            break;
        }
    }
    let _ = file.flush();
    let _ = file.sync_all();
    match pump.total {
        Some(t) if pos != t => Err(FetchError::Short),
        _ => Ok(pos),
    }
}

// --- worker --------------------------------------------------------------------------------------

/// Pull work forever. `pump` claims as many items as the concurrency setting allows and waits for
/// them; a nudge (enqueue/retry/resume, or a worker freeing a slot) starts the next round, and a
/// 30-second fallback re-scan covers a nudge that raced the loop.
async fn supervisor(ctx: Arc<Ctx>) {
    loop {
        pump(&ctx).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), ctx.notify.notified()).await;
    }
}

/// Claim and run up to `concurrency` items; returns how many were claimed. Also the tests' driver.
pub async fn pump(ctx: &Arc<Ctx>) -> usize {
    let limit = concurrency(&ctx.db);
    let mut claimed = 0;
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        while set.len() < limit {
            let Some(row) = claim_one(ctx) else { break };
            let worker_ctx = Arc::clone(ctx);
            claimed += 1;
            set.spawn(async move { run_item(worker_ctx, row).await });
        }
        if set.is_empty() {
            break;
        }
        if set.join_next().await.is_none() {
            break;
        }
    }
    claimed
}

/// Claim one row, skipping ids whose previous worker is still finishing a stop. When that happens
/// the row goes straight back to `queued`; the exiting worker's own nudge brings it around again.
fn claim_one(ctx: &Ctx) -> Option<DownloadRow> {
    while let Some(row) = ctx.db.claim_next_download() {
        let free = ctx.running.lock().map(|mut r| r.insert(row.video_id.clone())).unwrap_or(false);
        if free {
            return Some(row);
        }
        ctx.db.set_download_state_if(&row.video_id, &["downloading"], "queued", None);
        break;
    }
    None
}

async fn run_item(ctx: Arc<Ctx>, row: DownloadRow) {
    let id = row.video_id.clone();
    match run_download(&ctx, &row).await {
        Ok(()) => {}
        Err(RunError::Failed(msg)) => {
            if ctx.db.fail_download(&id, &msg) {
                tracing::warn!(video_id = %id, error = %msg, "download failed");
            }
        }
    }
    ctx.signal_off(&id);
    if let Ok(mut running) = ctx.running.lock() {
        running.remove(&id);
    }
    ctx.emit_changed();
    // Freeing the slot may be exactly what a queued item was waiting for.
    ctx.nudge();
}

enum RunError {
    Failed(String),
}

async fn run_download(ctx: &Ctx, row: &DownloadRow) -> Result<(), RunError> {
    // A pause/cancel that landed between the claim and this task starting.
    if ctx.stop(&row.video_id).is_some()
        || !matches!(
            ctx.db.download(&row.video_id).map(|r| r.state).as_deref(),
            Some("downloading")
        )
    {
        reconcile_stop(ctx, row, None);
        return Ok(());
    }
    let dir = ctx.dir();
    let stem = stem_of(row);
    let quality = match row.quality.as_str() {
        "LOW" => AudioQuality::Low,
        "AUTO" => AudioQuality::Auto,
        _ => AudioQuality::High,
    };
    let mut last: Option<String> = None;
    for attempt in 0..ATTEMPTS {
        if ctx.stop(&row.video_id).is_some() {
            reconcile_stop(ctx, row, None);
            return Ok(());
        }
        let src = match (ctx.resolve)(row.video_id.clone(), quality, row.is_upload).await {
            Ok(src) => src,
            Err(e) => return Err(RunError::Failed(friendly(&e))),
        };
        if ctx.stop(&row.video_id).is_some() {
            reconcile_stop(ctx, row, None);
            return Ok(());
        }
        if let Err(e) = std::fs::create_dir_all(&dir) {
            return Err(RunError::Failed(format!(
                "could not create the downloads folder: {}",
                e.kind()
            )));
        }
        // Written from here on, so this folder must stay reachable for remove/cancel/recover even
        // if the user moves `download_dir` before this download ends.
        ctx.remember_dir(&dir);
        ctx.db.set_download_format(
            &row.video_id,
            src.itag,
            src.mime.as_deref(),
            &actual_label(&src),
        );
        let ext = ext_for_mime(src.mime.as_deref().unwrap_or_default());
        let final_path = dir.join(format!("{stem}.{ext}"));
        let part = dir.join(part_name(&row.video_id, src.itag));
        sweep_other_parts(&dir, &row.video_id, &part);
        // A `download_dir` change must not strand usable bytes: the matching partial written in
        // an earlier folder is adopted here, and the rest (another format, dead weight exactly as
        // it is in this folder) is swept in that folder too.
        for d in ctx.app_dirs() {
            if d == dir {
                continue;
            }
            sweep_other_parts(&d, &row.video_id, &part);
            adopt_part(&d, &part);
        }
        let resume = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        match fetch_to_file(ctx, row, &src, &part, resume, STALL).await {
            Ok(total) => {
                // Rename first, commit state second: a `done` row must never point at a file that
                // is not there. A crash between the two leaves the row `downloading` (requeued on
                // restart) and the finished file on disk, which the next attempt overwrites.
                if let Err(e) = commit(&part, &final_path) {
                    let msg = format!("could not save the file: {}", e.kind());
                    ctx.db.set_download_state_if(
                        &row.video_id,
                        &["downloading"],
                        "error",
                        Some(&msg),
                    );
                    return Err(RunError::Failed(msg));
                }
                // The conditional write decides the race; the loser owns the file we renamed.
                if ctx.db.finish_download(
                    &row.video_id,
                    &final_path.to_string_lossy(),
                    total as i64,
                ) {
                    // Cover for offline display — after the row is committed `done`, so a failed
                    // fetch can only lose artwork, never the download (save_artwork is
                    // best-effort by contract).
                    save_artwork(ctx, row, &dir).await;
                    return Ok(());
                }
                reconcile_lost_finish(ctx, row, &part, &final_path);
                return Ok(());
            }
            Err(FetchError::Stopped) => {
                reconcile_stop(ctx, row, Some(&part));
                return Ok(());
            }
            Err(FetchError::Stale) if attempt + 1 < ATTEMPTS => {
                // The format changed or the partial outlived its file: start over.
                let _ = std::fs::remove_file(&part);
                last = Some(FetchError::Stale.message());
                continue;
            }
            Err(FetchError::Auth) if attempt + 1 < ATTEMPTS => {
                // The signed URL expired mid-download; resolving again usually continues it.
                last = Some(FetchError::Auth.message());
                continue;
            }
            Err(e) => return Err(RunError::Failed(e.message())),
        }
    }
    Err(RunError::Failed(last.unwrap_or_else(|| "download failed".to_owned())))
}

/// A stop asked for mid-flight (or before it): the row's own state says which way to reconcile.
fn reconcile_stop(ctx: &Ctx, row: &DownloadRow, part: Option<&Path>) {
    let dirs = ctx.app_dirs();
    match ctx.db.download(&row.video_id).map(|r| r.state) {
        // Paused — or already re-queued by a resume that beat this worker's stop (the resume
        // cleared the stop signal, so the worker only notices when its next progress write is
        // refused): either way the next claim resumes from these bytes, so they stay.
        Some(s) if s == "paused" || s == "queued" => {
            let len = match part {
                Some(p) => std::fs::metadata(p).map(|m| m.len()).unwrap_or(0),
                None => dirs.iter().map(|d| parts_len(d, &row.video_id)).sum(),
            };
            ctx.db.set_download_bytes(&row.video_id, len as i64);
        }
        // Cancelled, removed, or anything else: no partial survives.
        _ => {
            match part {
                Some(p) => {
                    let _ = std::fs::remove_file(p);
                }
                None => {
                    for d in &dirs {
                        remove_parts_for_stem(d, &row.video_id);
                    }
                }
            }
            if ctx.db.download(&row.video_id).is_some() {
                ctx.db.set_download_bytes(&row.video_id, 0);
            }
        }
    }
}

/// The download completed but its claim lost to a pause/cancel. The file is already renamed to
/// its final name, so the loser answers for that file: keep it for a paused resume, drop it
/// otherwise. `part` is the partial path this attempt actually resolved (`src.itag`) — the
/// claim-time `row.itag` predates the resolve and can name a partial the next attempt sweeps.
fn reconcile_lost_finish(ctx: &Ctx, row: &DownloadRow, part: &Path, renamed: &Path) {
    match ctx.db.download(&row.video_id).map(|r| r.state) {
        Some(s) if s == "paused" => {
            // Keep the finished bytes under the partial name so a resume still finds them.
            let kept = if std::fs::rename(renamed, part).is_ok() {
                part.to_path_buf()
            } else {
                renamed.to_path_buf()
            };
            let len = std::fs::metadata(&kept).map(|m| m.len()).unwrap_or(0);
            ctx.db.set_download_bytes(&row.video_id, len as i64);
        }
        _ => {
            let _ = std::fs::remove_file(renamed);
            if ctx.db.download(&row.video_id).is_some() {
                ctx.db.set_download_bytes(&row.video_id, 0);
            }
        }
    }
}

/// Resolve failures reach the UI as their own sentence; strip nothing (the orchestrator's messages
/// name the videoId at most, never a URL).
fn friendly(message: &str) -> String {
    let message = message.trim();
    if message.is_empty() {
        "could not resolve a stream".to_owned()
    } else {
        message.chars().take(300).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::{BodyExt, Full, StreamBody};
    use hyper::body::{Bytes, Frame, Incoming};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use innertube::SongItem;

    type TestBody = http_body_util::combinators::BoxBody<Bytes, std::io::Error>;

    /// Deterministic pseudo-random bytes so a byte-exact comparison means something.
    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| ((i.wrapping_mul(2654435761)) >> 13) as u8).collect()
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        /// Honours every range, exact windows.
        Ranges,
        /// Honours every range but omits the complete length (`bytes s-e/*`), like a proxy that
        /// strips it. The window loop gets no total to stop on — only the final 416 says the end.
        NoTotal,
        /// Ignores Range and answers 200 with the whole body.
        IgnoreRange,
        /// Honours the range but cuts the body short.
        Truncate,
        /// Sends the body in slow chunks (for pause/cancel tests).
        Slow,
        /// Sends headers, then hangs without a byte.
        Stall,
    }

    struct Server {
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        url: String,
    }

    /// A tiny range server over a fixed byte array, recording every Range it was asked for.
    async fn serve(len: usize, mode: Mode) -> Server {
        let data = Arc::new(data(len));
        let ranges: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let url = format!("http://127.0.0.1:{}/track", listener.local_addr().unwrap().port());
        let seen = Arc::clone(&ranges);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let data = Arc::clone(&data);
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let data = Arc::clone(&data);
                        let seen = Arc::clone(&seen);
                        async move { Ok::<_, Infallible>(respond(req, data, seen, mode)) }
                    });
                    let _ =
                        http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                });
            }
        });
        Server { ranges, url }
    }

    fn respond(
        req: Request<Incoming>,
        data: Arc<Vec<u8>>,
        seen: Arc<Mutex<Vec<(u64, u64)>>>,
        mode: Mode,
    ) -> Response<TestBody> {
        // Artwork fetches are plain GETs for a `.jpg`; answer them as an image so the offline
        // cover save sees what a real thumbnail host sends (audio otherwise, with or without Range).
        if req.uri().path().ends_with(".jpg") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "image/jpeg")
                .body(full_body(Bytes::from(data.to_vec())))
                .unwrap();
        }
        let len = data.len() as u64;
        let header = req.headers().get("range").and_then(|v| v.to_str().ok()).map(str::to_owned);
        let (start, end) = match header.as_deref().and_then(parse_test_range) {
            Some((s, e)) => (s, e.min(len.saturating_sub(1))),
            None => (0, len.saturating_sub(1)),
        };
        if let Ok(mut s) = seen.lock() {
            s.push((start, end));
        }
        if mode != Mode::IgnoreRange && start >= len {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header("Content-Range", format!("bytes */{len}"))
                .body(full_body(Bytes::new()))
                .unwrap();
        }
        let slice = data[start as usize..=end as usize].to_vec();
        match mode {
            Mode::Ranges | Mode::NoTotal | Mode::IgnoreRange => {
                let (status, slice) = if mode == Mode::IgnoreRange {
                    (StatusCode::OK, data[start as usize..].to_vec())
                } else {
                    (StatusCode::PARTIAL_CONTENT, slice)
                };
                let mut resp = Response::builder()
                    .status(status)
                    .header("Content-Type", "audio/webm; codecs=opus");
                if status == StatusCode::PARTIAL_CONTENT {
                    // The one difference that matters: no complete length to stop the window loop.
                    let total =
                        if mode == Mode::NoTotal { "*".to_owned() } else { len.to_string() };
                    resp = resp.header("Content-Range", format!("bytes {start}-{end}/{total}"));
                } else {
                    resp = resp.header("Content-Length", slice.len());
                }
                resp.body(full_body(Bytes::from(slice))).unwrap()
            }
            Mode::Truncate => {
                let cut = start + ((end - start + 1) / 2).max(1);
                let cut = cut.min(end + 1);
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("Content-Range", format!("bytes {start}-{end}/{len}"))
                    .header("Content-Type", "audio/webm; codecs=opus")
                    .body(full_body(Bytes::from(data[start as usize..cut as usize].to_vec())))
                    .unwrap()
            }
            Mode::Slow => {
                let chunks: Vec<Vec<u8>> = slice.chunks(32 * 1024).map(|c| c.to_vec()).collect();
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("Content-Range", format!("bytes {start}-{end}/{len}"))
                    .header("Content-Type", "audio/webm; codecs=opus")
                    .body(slow_body(chunks, 40))
                    .unwrap()
            }
            Mode::Stall => Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("Content-Range", format!("bytes {start}-{end}/{len}"))
                .body(stall_body())
                .unwrap(),
        }
    }

    fn parse_test_range(value: &str) -> Option<(u64, u64)> {
        let value = value.strip_prefix("bytes=")?;
        let (a, b) = value.split_once('-')?;
        Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
    }

    fn full_body(bytes: Bytes) -> TestBody {
        Full::new(bytes).map_err(|e| match e {}).boxed()
    }

    /// The body trickles out in chunks, so a test can pause/cancel mid-body deterministically.
    fn slow_body(chunks: Vec<Vec<u8>>, delay_ms: u64) -> TestBody {
        let stream = futures_util::stream::unfold(0usize, move |i| {
            let chunks = chunks.clone();
            async move {
                if i >= chunks.len() {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                Some((Ok::<_, std::io::Error>(Frame::data(Bytes::from(chunks[i].clone()))), i + 1))
            }
        });
        BodyExt::boxed(StreamBody::new(stream))
    }

    /// Headers, then nothing: the stall guard is the only way out.
    fn stall_body() -> TestBody {
        let stream = futures_util::stream::unfold(0usize, |i| async move {
            if i > 0 {
                return None;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
            Some((Ok::<_, std::io::Error>(Frame::data(Bytes::new())), 1))
        });
        BodyExt::boxed(StreamBody::new(stream))
    }

    /// A context over a real file-backed database and a real temp folder; the resolver is the
    /// closure under test rather than the orchestrator.
    struct TestEnv {
        ctx: Arc<Ctx>,
        db: Arc<Db>,
        dir: PathBuf,
    }

    impl TestEnv {
        fn new(name: &str, url: String, itag: i64, mime: &str, bitrate: i64) -> TestEnv {
            // The real platform probe by default: temp dirs have plenty of space, so every
            // existing test also exercises the actual free-space API end to end.
            TestEnv::with_probe(name, url, itag, mime, bitrate, free_space)
        }

        /// As `new`, with the free-space probe injected — how the preflight tests simulate a full
        /// disk (or a platform that cannot measure one) without touching the real volume.
        fn with_probe(
            name: &str,
            url: String,
            itag: i64,
            mime: &str,
            bitrate: i64,
            probe: fn(&Path) -> Option<u64>,
        ) -> TestEnv {
            let mime = mime.to_owned();
            let resolve: Resolver = Arc::new(move |_video, _quality, _upload| {
                let url = url.clone();
                let mime = mime.clone();
                Box::pin(async move {
                    Ok(Source {
                        url,
                        headers: HashMap::new(),
                        itag,
                        mime: Some(mime),
                        bitrate: Some(bitrate),
                    })
                })
            });
            TestEnv::with_resolver(name, resolve, probe)
        }

        /// A context whose resolver is the caller's — a spy, a failure, anything but the
        /// orchestrator. This is what the identity-separation tests drive.
        fn with_resolver(
            name: &str,
            resolve: Resolver,
            probe: fn(&Path) -> Option<u64>,
        ) -> TestEnv {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let id = SEQ.fetch_add(1, Ordering::SeqCst);
            let base =
                std::env::temp_dir().join(format!("limusic-dl-{name}-{}-{id}", std::process::id()));
            std::fs::remove_dir_all(&base).ok();
            std::fs::create_dir_all(&base).unwrap();
            let db = Arc::new(Db::open(&base.join("test.sqlite")).unwrap());
            let dir = base.join("downloads");
            let ctx = Arc::new(Ctx {
                db: Arc::clone(&db),
                default_dir: dir.clone(),
                cache_dir: dir.join("audio-cache"),
                app: None,
                resolve,
                free_probe: probe,
                signals: Mutex::new(HashMap::new()),
                running: Mutex::new(HashSet::new()),
                notify: tokio::sync::Notify::new(),
                events: Mutex::new(Vec::new()),
                changed: Mutex::new(Changed::default()),
            });
            TestEnv { ctx, db, dir }
        }

        fn enqueue_one(&self, video_id: &str, title: &str) -> DownloadItem {
            let item = SongItem {
                video_id: video_id.to_owned(),
                title: title.to_owned(),
                artists: "Tester".to_owned(),
                ..Default::default()
            };
            let mut out = enqueue_items(&self.ctx, &[item], "HIGH");
            assert_eq!(out.len(), 1, "one item in, one item out");
            out.remove(0)
        }

        async fn drain(&self) {
            let mut guard = 0;
            while pump(&self.ctx).await > 0 {
                guard += 1;
                assert!(guard < 64, "the queue never drained");
            }
        }

        fn row(&self, id: &str) -> DownloadRow {
            self.db.download(id).expect("the row went missing")
        }

        fn done_path(&self, id: &str) -> PathBuf {
            let row = self.row(id);
            PathBuf::from(row.path.expect("done row without a path"))
        }

        fn events(&self) -> Vec<(String, serde_json::Value)> {
            self.ctx.events.lock().unwrap().clone()
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            if let Some(base) = self.dir.parent() {
                std::fs::remove_dir_all(base).ok();
            }
        }
    }

    fn song(video_id: &str, title: &str) -> SongItem {
        SongItem {
            video_id: video_id.to_owned(),
            title: title.to_owned(),
            artists: "Tester".to_owned(),
            ..Default::default()
        }
    }

    fn collection(id: &str, title: &str) -> NewDownloadCollection {
        NewDownloadCollection {
            id: id.to_owned(),
            kind: "playlist".to_owned(),
            title: title.to_owned(),
            subtitle: Some("Offline collection".to_owned()),
        }
    }

    // --- downloaded collections -----------------------------------------------------------------

    #[tokio::test]
    async fn playlist_download_records_count_limited_memberships_in_order() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-count", server.url, 251, "audio/webm", 160_000);
        let all =
            [song("aaaaaaaaaaa", "One"), song("bbbbbbbbbbb", "Two"), song("ccccccccccc", "Three")];
        let count = 2;
        let queued =
            enqueue_collection(&env.ctx, &all[..count], "HIGH", &collection("VLcount", "Counted"))
                .unwrap();

        assert_eq!(queued.len(), count, "only the count pick is enqueued");
        assert_eq!(
            env.db.collection_track_positions("VLcount"),
            vec![("aaaaaaaaaaa".into(), 0), ("bbbbbbbbbbb".into(), 1)]
        );
        assert!(env.db.download("ccccccccccc").is_none(), "the unpicked track is not queued");
        let summary = env.db.download_collections().remove(0);
        assert_eq!(summary.title, "Counted");
        assert_eq!(summary.track_count, 2);
        assert_eq!(summary.downloaded_count, 0);
    }

    #[tokio::test]
    async fn collection_download_dedupes_tracks_and_direct_download_marks_standalone() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-dedupe", server.url, 251, "audio/webm", 160_000);
        let repeated = [
            song("aaaaaaaaaaa", "One"),
            song("bbbbbbbbbbb", "Two"),
            song("aaaaaaaaaaa", "One again"),
        ];
        let queued =
            enqueue_collection(&env.ctx, &repeated, "HIGH", &collection("VLdedupe", "Dedupe"))
                .unwrap();
        assert_eq!(queued.len(), 2, "duplicates do not create another row or membership");
        assert_eq!(env.row("aaaaaaaaaaa").standalone, false);
        assert_eq!(
            env.db.collection_track_positions("VLdedupe"),
            vec![("aaaaaaaaaaa".into(), 0), ("bbbbbbbbbbb".into(), 1)]
        );

        enqueue_items(&env.ctx, &[song("aaaaaaaaaaa", "One")], "HIGH");
        assert!(env.row("aaaaaaaaaaa").standalone, "a direct download claims standalone ownership");
        assert!(
            !env.row("bbbbbbbbbbb").standalone,
            "playlist-created rows remain collection-owned"
        );
    }

    #[tokio::test]
    async fn rerunning_a_playlist_download_updates_collection_without_duplicate_memberships() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-rerun", server.url, 251, "audio/webm", 160_000);
        let first = [song("aaaaaaaaaaa", "One"), song("bbbbbbbbbbb", "Two")];
        enqueue_collection(&env.ctx, &first, "HIGH", &collection("VLrerun", "Old title")).unwrap();
        let added_at = env.db.download_collections()[0].added_at;

        let updated =
            [song("aaaaaaaaaaa", "One"), song("bbbbbbbbbbb", "Two"), song("ccccccccccc", "Three")];
        enqueue_collection(&env.ctx, &updated, "HIGH", &collection("VLrerun", "New title"))
            .unwrap();
        let summary = env.db.download_collections().remove(0);
        assert_eq!(summary.title, "New title");
        assert_eq!(summary.added_at, added_at, "updating preserves collection creation time");
        assert_eq!(summary.track_count, 3);
        assert_eq!(env.db.collection_track_positions("VLrerun").len(), 3);
    }

    #[tokio::test]
    async fn removing_collection_keeps_standalone_and_shared_downloads() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-refcount", server.url, 251, "audio/webm", 160_000);
        let first = [
            song("aaaaaaaaaaa", "Shared"),
            song("bbbbbbbbbbb", "Only first"),
            song("ccccccccccc", "Standalone"),
        ];
        let second = [song("aaaaaaaaaaa", "Shared"), song("ddddddddddd", "Only second")];
        enqueue_collection(&env.ctx, &first, "HIGH", &collection("VLfirst", "First")).unwrap();
        enqueue_collection(&env.ctx, &second, "HIGH", &collection("VLsecond", "Second")).unwrap();
        enqueue_items(&env.ctx, &[song("ccccccccccc", "Standalone")], "HIGH");
        env.drain().await;
        let shared = env.done_path("aaaaaaaaaaa");
        let only_first = env.done_path("bbbbbbbbbbb");
        let standalone = env.done_path("ccccccccccc");
        let only_second = env.done_path("ddddddddddd");

        action(&env.ctx, "VLfirst", "remove_collection").unwrap();
        assert!(shared.exists(), "the other collection still owns the shared track");
        assert!(env.row("aaaaaaaaaaa").standalone == false);
        assert!(!only_first.exists() && env.db.download("bbbbbbbbbbb").is_none());
        assert!(standalone.exists() && env.row("ccccccccccc").standalone);
        assert_eq!(
            env.db.collection_track_positions("VLsecond"),
            vec![("aaaaaaaaaaa".into(), 0), ("ddddddddddd".into(), 1)]
        );

        action(&env.ctx, "VLsecond", "remove_collection").unwrap();
        assert!(
            !shared.exists() && env.db.download("aaaaaaaaaaa").is_none(),
            "last collection reference releases shared track"
        );
        assert!(!only_second.exists() && env.db.download("ddddddddddd").is_none());
        assert!(
            standalone.exists() && env.db.download("ccccccccccc").is_some(),
            "standalone file survives collection removal"
        );
    }

    #[tokio::test]
    async fn collection_removal_preview_matches_refcounted_removal() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-preview-counts", server.url, 251, "audio/webm", 160_000);
        let first = [
            song("aaaaaaaaaaa", "Shared"),
            song("bbbbbbbbbbb", "Exclusive"),
            song("ccccccccccc", "Standalone"),
        ];
        enqueue_collection(&env.ctx, &first, "HIGH", &collection("VLpreview", "Preview")).unwrap();
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "Shared")],
            "HIGH",
            &collection("VLother", "Other"),
        )
        .unwrap();
        enqueue_items(&env.ctx, &[song("ccccccccccc", "Standalone")], "HIGH");
        env.drain().await;
        let shared_path = env.done_path("aaaaaaaaaaa");
        let exclusive_path = env.done_path("bbbbbbbbbbb");
        let standalone_path = env.done_path("ccccccccccc");
        assert!(shared_path.exists() && exclusive_path.exists() && standalone_path.exists());

        let preview = collection_removal_preview(&env.ctx, "VLpreview").unwrap();
        assert_eq!(preview.delete_count, 1, "only the exclusively owned track is deleted");
        assert_eq!(preview.keep_count, 2, "the shared and standalone tracks are retained");
        assert!(env.db.download("aaaaaaaaaaa").is_some(), "preview leaves the shared row intact");
        assert!(
            env.db.download("bbbbbbbbbbb").is_some(),
            "preview leaves the exclusive row intact"
        );
        assert!(
            env.db.download("ccccccccccc").is_some(),
            "preview leaves the standalone row intact"
        );
        assert!(env.db.download_collections().iter().any(|c| c.id == "VLpreview"));

        action(&env.ctx, "VLpreview", "remove_collection").unwrap();
        assert!(!exclusive_path.exists(), "the one counted deletion is removed from disk");
        assert!(shared_path.exists(), "the shared file remains on disk");
        assert!(standalone_path.exists(), "the standalone file remains on disk");
        assert!(env.db.download("bbbbbbbbbbb").is_none());
        assert!(env.db.download("aaaaaaaaaaa").is_some(), "shared track remains");
        assert!(env.db.download("ccccccccccc").is_some(), "standalone track remains");
    }

    #[tokio::test]
    async fn removing_collection_deletes_exclusive_track_file_and_row() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-delete", server.url, 251, "audio/webm", 160_000);
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "Owned")],
            "HIGH",
            &collection("VLremove", "Remove me"),
        )
        .unwrap();
        env.drain().await;
        let path = env.done_path("aaaaaaaaaaa");
        assert!(path.exists());

        action(&env.ctx, "VLremove", "remove_collection").unwrap();
        assert!(!path.exists(), "exclusive track file is deleted");
        assert!(env.db.download("aaaaaaaaaaa").is_none(), "exclusive track row is deleted");
        assert!(env.db.download_collections().is_empty(), "collection row is deleted");
    }

    #[tokio::test]
    async fn removing_one_track_drops_its_collection_memberships() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-track-remove", server.url, 251, "audio/webm", 160_000);
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "One"), song("bbbbbbbbbbb", "Two")],
            "HIGH",
            &collection("VLtrackremove", "Track removal"),
        )
        .unwrap();
        env.drain().await;
        let path = env.done_path("aaaaaaaaaaa");
        action(&env.ctx, "aaaaaaaaaaa", "remove").unwrap();

        assert!(!path.exists());
        assert!(env.db.download("aaaaaaaaaaa").is_none());
        assert_eq!(
            env.db.collection_track_positions("VLtrackremove"),
            vec![("bbbbbbbbbbb".into(), 1)]
        );
        assert_eq!(
            env.db.download_collections()[0].track_count,
            1,
            "the collection itself remains"
        );
    }

    #[tokio::test]
    async fn restart_recovery_prunes_memberships_for_missing_rows_and_keeps_live_ones() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-recover", server.url, 251, "audio/webm", 160_000);
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "Live"), song("bbbbbbbbbbb", "Missing")],
            "HIGH",
            &collection("VLrecover", "Recover me"),
        )
        .unwrap();
        let interrupted = env.db.claim_next_download().expect("one track is claimed for recovery");
        assert_eq!(interrupted.state, "downloading");

        let connection =
            rusqlite::Connection::open(env.dir.parent().unwrap().join("test.sqlite")).unwrap();
        connection.execute("DELETE FROM downloads WHERE video_id='bbbbbbbbbbb'", []).unwrap();
        drop(connection);
        recover(&env.ctx);

        assert_eq!(env.row("aaaaaaaaaaa").state, "queued", "interrupted downloads are requeued");
        assert_eq!(env.db.collection_track_positions("VLrecover"), vec![("aaaaaaaaaaa".into(), 0)]);
        let summary = env.db.download_collections().remove(0);
        assert_eq!(summary.track_count, 1, "dangling membership is pruned");
    }

    #[tokio::test]
    async fn collection_artwork_is_best_effort_and_removed_with_the_collection() {
        let server = serve(64 * 1024, Mode::Ranges).await;
        let env =
            TestEnv::new("collection-artwork", server.url.clone(), 251, "audio/webm", 160_000);
        let good = collection("VLartwork", "Artwork");
        enqueue_collection(&env.ctx, &[], "HIGH", &good).unwrap();
        save_collection_artwork(&env.ctx, &good.id, &format!("{}/cover.jpg", server.url)).await;
        let art = PathBuf::from(
            env.db.download_collections()[0].artwork_path.clone().expect("cover saved"),
        );
        assert!(art.starts_with(env.dir.join(ARTWORK_DIR)));
        assert_eq!(std::fs::read(&art).unwrap(), data(64 * 1024));

        let failed = collection("VLnoart", "No artwork");
        enqueue_collection(&env.ctx, &[], "HIGH", &failed).unwrap();
        save_collection_artwork(&env.ctx, &failed.id, "http://[IP_ADDRESS]:9/cover.jpg").await;
        assert!(env
            .db
            .download_collections()
            .iter()
            .find(|c| c.id == failed.id)
            .unwrap()
            .artwork_path
            .is_none());

        action(&env.ctx, &good.id, "remove_collection").unwrap();
        assert!(!art.exists(), "the collection cover is cleaned up");
    }

    #[tokio::test]
    async fn downloads_list_snapshot_serializes_ordered_collection_video_ids() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-snapshot-order", server.url, 251, "audio/webm", 160_000);
        let ordered = [
            song("ccccccccccc", "Third"),
            song("aaaaaaaaaaa", "First"),
            song("bbbbbbbbbbb", "Second"),
        ];
        enqueue_collection(&env.ctx, &ordered, "HIGH", &collection("VLordered", "Ordered"))
            .unwrap();
        env.drain().await;

        let json = serde_json::to_value(snapshot(&env.ctx)).unwrap();
        assert_eq!(
            json["collections"][0]["video_ids"],
            serde_json::json!(["ccccccccccc", "aaaaaaaaaaa", "bbbbbbbbbbb"])
        );
    }

    #[tokio::test]
    async fn downloads_list_snapshot_serializes_collection_summary_shape() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-snapshot", server.url, 251, "audio/webm", 160_000);
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "One")],
            "HIGH",
            &collection("VLsnapshot", "Snapshot"),
        )
        .unwrap();
        env.drain().await;
        let json = serde_json::to_value(snapshot(&env.ctx)).unwrap();

        assert!(json["items"].is_array());
        assert_eq!(json["items"].as_array().unwrap().len(), 1);
        let collection = &json["collections"][0];
        assert_eq!(collection["id"], "VLsnapshot");
        assert_eq!(collection["kind"], "playlist");
        assert_eq!(collection["title"], "Snapshot");
        assert_eq!(collection["subtitle"], "Offline collection");
        assert!(collection["artwork_path"].is_null());
        assert_eq!(collection["track_count"], 1);
        assert_eq!(collection["downloaded_count"], 1);
    }

    #[tokio::test]
    async fn removing_collection_refuses_track_paths_outside_download_folders() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("collection-path", server.url, 251, "audio/webm", 160_000);
        std::fs::create_dir_all(&env.dir).unwrap();
        let outside = env.dir.parent().unwrap().join("outside-audio.bin");
        std::fs::write(&outside, b"keep me").unwrap();
        enqueue_collection(
            &env.ctx,
            &[song("aaaaaaaaaaa", "Tampered")],
            "HIGH",
            &collection("VLoutside", "Outside"),
        )
        .unwrap();
        let claimed = env.db.claim_next_download().unwrap();
        assert!(env.db.finish_download("aaaaaaaaaaa", &outside.to_string_lossy(), 8));
        let error = action(&env.ctx, "VLoutside", "remove_collection").unwrap_err();

        assert!(error.contains("outside the downloads folders"));
        assert!(outside.exists(), "the path trust boundary is preserved");
        assert!(env.db.download("aaaaaaaaaaa").is_some(), "the download row stays reachable");
        assert_eq!(
            env.db.download_collections()[0].track_count,
            1,
            "failed removal rolls back the collection"
        );
        drop(claimed);
    }

    // --- transport ---------------------------------------------------------------------------

    #[tokio::test]
    async fn downloads_in_bounded_ranges_byte_exactly() {
        let server = serve(10 * 1024 * 1024 + 123, Mode::Ranges).await;
        let env =
            TestEnv::new("ranges", server.url.clone(), 251, "audio/webm; codecs=opus", 160_000);
        let item = env.enqueue_one("aaaaaaaaaaa", "A Song");
        env.drain().await;

        let row = env.row("aaaaaaaaaaa");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(row.itag, Some(251));
        assert_eq!(row.mime.as_deref(), Some("audio/webm; codecs=opus"));
        assert_eq!(row.quality_label, "160 kbps Opus", "the label names the actual format");
        let path = env.done_path("aaaaaaaaaaa");
        assert_eq!(path.file_name().unwrap().to_string_lossy(), "A Song [aaaaaaaaaaa].webm");
        assert_eq!(std::fs::read(&path).unwrap(), data(10 * 1024 * 1024 + 123));

        // Every request was a bounded range: no open-ended fetch (the throttled shape), and the
        // windows tile the file without gaps or overlaps.
        let ranges = server.ranges.lock().unwrap().clone();
        assert!(!ranges.is_empty());
        let mut expected = 0u64;
        for (start, end) in &ranges {
            assert_eq!(*start, expected, "windows tile: no gaps, no repeats");
            assert!(end >= start);
            assert!(end - start + 1 <= CHUNK, "every window is a bounded 4 MiB ask");
            expected = end + 1;
        }
        assert_eq!(expected, 10 * 1024 * 1024 + 123, "the last window reaches the end");
        assert_eq!(item.id, "aaaaaaaaaaa");

        // `downloads-changed` is coalesced (150 ms) with a trailing send; let it land.
        tokio::time::sleep(Duration::from_millis(250)).await;
        // The UI saw the state change and the progress.
        let events = env.events();
        assert!(events.iter().any(|(e, p)| e == "downloads-changed"
            && p.as_array().is_some_and(|a| a.iter().any(|i| i["state"] == "done"))));
        assert!(events
            .iter()
            .any(|(e, p)| e == "download-progress" && p["bytes_total"].as_u64().is_some()));
    }

    #[tokio::test]
    async fn a_matching_partial_resumes_from_its_own_length() {
        let len = 6 * 1024 * 1024;
        let server = serve(len, Mode::Ranges).await;
        let env =
            TestEnv::new("resume", server.url.clone(), 140, "audio/mp4; codecs=mp4a.40.2", 128_000);
        env.enqueue_one("bbbbbbbbbbb", "Resume Me");
        std::fs::create_dir_all(&env.dir).unwrap();
        let all = data(len);
        let part_len = 1_000_000usize;
        std::fs::write(env.dir.join(part_name("bbbbbbbbbbb", 140)), &all[..part_len]).unwrap();
        // And a dead partial from a format the resolver will not pick: it must be swept.
        std::fs::write(env.dir.join(part_name("bbbbbbbbbbb", 251)), b"stale").unwrap();

        env.drain().await;

        assert_eq!(env.row("bbbbbbbbbbb").state, "done");
        let path = env.done_path("bbbbbbbbbbb");
        assert_eq!(path.file_name().unwrap().to_string_lossy(), "Resume Me [bbbbbbbbbbb].m4a");
        assert_eq!(std::fs::read(&path).unwrap(), all, "the resumed file is byte-exact");
        assert!(
            !env.dir.join(part_name("bbbbbbbbbbb", 251)).exists(),
            "the other format's partial was swept"
        );
        let ranges = server.ranges.lock().unwrap().clone();
        assert_eq!(ranges[0].0, part_len as u64, "the first ask continues from the partial");
    }

    /// A server that omits the complete length from its 206s (RFC 7233 allows `bytes s-e/*`)
    /// gives the window loop nothing to stop on — the only end-of-file signal is the final 416,
    /// whose total is the bytes now on disk, which this attempt has already grown past its
    /// attempt-start `resume`. Comparing `t == resume` there reads a *complete* download as a
    /// stale partial: the finished file is thrown away and, after three attempts, the item fails.
    #[tokio::test]
    async fn a_416_after_the_window_grew_means_the_file_is_complete() {
        let len = 6 * 1024 * 1024;
        let server = serve(len, Mode::NoTotal).await;
        let env = TestEnv::new("nototal", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("nnnnnnnnnnn", "No Totals");
        std::fs::create_dir_all(&env.dir).unwrap();
        let all = data(len);
        let part_len = 1_000_000usize;
        std::fs::write(env.dir.join(part_name("nnnnnnnnnnn", 251)), &all[..part_len]).unwrap();

        env.drain().await;

        let row = env.row("nnnnnnnnnnn");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(std::fs::read(env.done_path("nnnnnnnnnnn")).unwrap(), all, "byte-exact");
        let ranges = server.ranges.lock().unwrap().clone();
        assert_eq!(
            ranges[0].0, part_len as u64,
            "the partial grew inside one attempt — no restart from zero"
        );
    }

    #[tokio::test]
    async fn a_server_that_ignores_ranges_makes_a_resume_start_over() {
        let len = 2 * 1024 * 1024;
        let server = serve(len, Mode::IgnoreRange).await;
        let env = TestEnv::new("ignore", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("ccccccccccc", "No Ranges");
        std::fs::create_dir_all(&env.dir).unwrap();
        std::fs::write(env.dir.join(part_name("ccccccccccc", 251)), &data(len)[..500_000]).unwrap();

        env.drain().await;

        let row = env.row("ccccccccccc");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(
            std::fs::read(env.done_path("ccccccccccc")).unwrap(),
            data(len),
            "restarted from zero"
        );
        let ranges = server.ranges.lock().unwrap().clone();
        assert!(ranges.iter().any(|(s, _)| *s == 0), "the restart asked from zero");
    }

    #[tokio::test]
    async fn a_short_window_fails_the_item_and_keeps_the_partial() {
        let server = serve(256 * 1024, Mode::Truncate).await;
        let env = TestEnv::new("short", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("ddddddddddd", "Truncated");
        env.drain().await;

        let row = env.row("ddddddddddd");
        assert_eq!(row.state, "error");
        assert!(
            row.error.as_deref().unwrap_or_default().contains("ended early"),
            "{:?}",
            row.error
        );
        assert!(
            env.dir.join(part_name("ddddddddddd", 251)).exists(),
            "the partial survives for a retry"
        );
    }

    #[tokio::test]
    async fn a_stalled_body_is_cut_off() {
        let server = serve(4 * 1024 * 1024, Mode::Stall).await;
        let env = TestEnv::new("stall", server.url.clone(), 251, "audio/webm", 160_000);

        // Drive the transport directly so the test does not wait out the real 20 s guard.
        env.enqueue_one("eeeeeeeeeee", "Stalled");
        assert!(ctx_claim(&env, "eeeeeeeeeee"));
        let row = env.row("eeeeeeeeeee");
        let src = Source {
            url: server.url.clone(),
            headers: HashMap::new(),
            itag: 251,
            mime: Some("audio/webm".to_owned()),
            bitrate: None,
        };
        let part = env.dir.join(part_name(&file_stem("Stalled", "eeeeeeeeeee"), 251));
        std::fs::create_dir_all(&env.dir).unwrap();
        let err = fetch_to_file(&env.ctx, &row, &src, &part, 0, Duration::from_millis(250)).await;
        assert!(matches!(err, Err(FetchError::Io(ref m)) if m.contains("stalled")), "{err:?}");
    }

    /// Claim one id through the real claim path (claim_next + running set).
    fn ctx_claim(env: &TestEnv, video_id: &str) -> bool {
        let row = env.db.claim_next_download();
        match row {
            Some(row) if row.video_id == video_id => {
                env.ctx.running.lock().unwrap().insert(video_id.to_owned());
                true
            }
            _ => false,
        }
    }

    // --- queue state machine ------------------------------------------------------------------

    #[tokio::test]
    async fn pause_freezes_the_partial_and_resume_finishes_it() {
        let len = 3 * 1024 * 1024; // ~4 windows, slow: seconds of download time
        let server = serve(len, Mode::Slow).await;
        let env = TestEnv::new("pause", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("fffffffffff", "Pause Me");

        let worker = tokio::spawn({
            let ctx = Arc::clone(&env.ctx);
            async move { while pump(&ctx).await > 0 {} }
        });

        // Wait for real progress, then pause while the worker is mid-body.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "never saw progress to pause mid-flight");
            let row = env.row("fffffffffff");
            if row.state == "downloading" && row.bytes_done > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        action(&env.ctx, "fffffffffff", "pause").unwrap();
        worker.await.unwrap();
        // A trailing coalesced event may still be in flight; settle before asserting.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let row = env.row("fffffffffff");
        assert_eq!(row.state, "paused");
        let part = env.dir.join(part_name("fffffffffff", 251));
        let frozen = std::fs::metadata(&part).unwrap().len();
        assert!(frozen > 0 && frozen < len as u64, "a partial, not the whole file: {frozen}");
        assert_eq!(row.bytes_done as u64, frozen, "bytes_done reflects the file on disk");

        // Resume finishes the job, and the resumed file is byte-exact.
        action(&env.ctx, "fffffffffff", "resume").unwrap();
        env.drain().await;
        let row = env.row("fffffffffff");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(std::fs::read(env.done_path("fffffffffff")).unwrap(), data(len));
        assert!(frozen < len as u64, "the pause really did freeze mid-file");
    }

    /// A pause and a resume that both land before the old worker reconciles its stop leave the
    /// row `queued` (the resume cleared the stop signal; the worker stops on its refused progress
    /// write instead). That queued row must keep its partial — the next claim resumes from those
    /// bytes, and deleting them here silently throws the whole download away.
    #[tokio::test]
    async fn a_resume_that_beats_the_old_worker_keeps_the_partial() {
        let len = 1024 * 1024;
        let server = serve(len, Mode::Slow).await;
        let env = TestEnv::new("requeue", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("sssssssssss", "Quick Pause Resume");
        assert!(ctx_claim(&env, "sssssssssss"), "claimed, now 'downloading'");

        // The partial the worker wrote before stopping.
        std::fs::create_dir_all(&env.dir).unwrap();
        let all = data(len);
        let part = env.dir.join(part_name("sssssssssss", 251));
        let frozen = 400_000usize;
        std::fs::write(&part, &all[..frozen]).unwrap();

        // The whole race in two actions: pause, then resume before the worker stops.
        action(&env.ctx, "sssssssssss", "pause").unwrap();
        action(&env.ctx, "sssssssssss", "resume").unwrap();
        assert_eq!(env.row("sssssssssss").state, "queued", "the resume landed first");

        // The old worker now reconciles its stop — it stops because its next progress write was
        // refused (the row is no longer 'downloading'), exactly as in the race.
        let claimed = env.row("sssssssssss");
        reconcile_stop(&env.ctx, &claimed, Some(&part));

        assert!(part.exists(), "a re-queued row keeps its partial for the next claim");
        assert_eq!(env.row("sssssssssss").bytes_done as usize, frozen, "bytes preserved");

        // The next claim resumes from those bytes and finishes byte-exact.
        env.ctx.running.lock().unwrap().remove("sssssssssss");
        env.drain().await;
        let row = env.row("sssssssssss");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(std::fs::read(env.done_path("sssssssssss")).unwrap(), all);
        let ranges = server.ranges.lock().unwrap().clone();
        assert_eq!(ranges[0].0, frozen as u64, "the resume continued from the kept bytes");
    }

    /// A pause landing between `commit`'s rename and `finish_download` sends the *finished* file
    /// back to a `.part` — under the itag the resume will actually look for. The claim-time row
    /// snapshot predates the resolve (itag NULL on a first download), so renaming by `row.itag`
    /// parks the bytes under `.itag0.part`, which the next attempt sweeps as another format's
    /// dead weight: the whole file is re-downloaded.
    #[tokio::test]
    async fn a_lost_finish_keeps_the_partial_under_the_resolved_itag() {
        let len = 512 * 1024;
        let server = serve(len, Mode::Ranges).await;
        let env = TestEnv::new("lostfinish", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("ttttttttttt", "Lost Finish");
        assert!(ctx_claim(&env, "ttttttttttt"));

        // The claim-time snapshot predates the resolve: itag is NULL in it, while the attempt
        // records the resolved format in the database — exactly run_download's situation.
        let claimed = env.row("ttttttttttt");
        assert!(claimed.itag.is_none(), "the claim predates the resolve");
        assert!(env.db.set_download_format(
            "ttttttttttt",
            251,
            Some("audio/webm"),
            "160 kbps Opus"
        ));

        // `commit` already renamed the partial onto its final name...
        std::fs::create_dir_all(&env.dir).unwrap();
        let stem = file_stem("Lost Finish", "ttttttttttt");
        let final_path = env.dir.join(format!("{stem}.webm"));
        std::fs::write(&final_path, data(len)).unwrap();

        // ...then a pause wins the finish, and the loser reconciles the renamed file.
        assert!(env.db.set_download_state_if("ttttttttttt", &["downloading"], "paused", None));
        let part = env.dir.join(part_name("ttttttttttt", 251));
        reconcile_lost_finish(&env.ctx, &claimed, &part, &final_path);

        // The finished bytes must wait under the itag the resume will look for — not claim-time NULL.
        assert!(
            !env.dir.join(part_name("ttttttttttt", 0)).exists(),
            "never under the claim-time NULL itag"
        );
        assert!(part.exists(), "kept under the resolved itag251 partial name");
        assert_eq!(std::fs::read(&part).unwrap(), data(len), "the whole file, byte-exact");
        assert_eq!(env.row("ttttttttttt").bytes_done as usize, len);

        // A resume finds those bytes and completes without re-downloading a byte.
        env.ctx.running.lock().unwrap().remove("ttttttttttt");
        action(&env.ctx, "ttttttttttt", "resume").unwrap();
        env.drain().await;
        let row = env.row("ttttttttttt");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(std::fs::read(env.done_path("ttttttttttt")).unwrap(), data(len));
        let ranges = server.ranges.lock().unwrap().clone();
        assert_eq!(
            ranges[0].0, len as u64,
            "the resume asked at EOF: the complete partial was found, nothing re-fetched"
        );
    }

    /// A disk that cannot hold the next window fails the queue item with a readable error *before*
    /// any request goes out — no crash, no half-written window, and a pre-seeded partial is left
    /// exactly as it was (free-space preflight, reserve modeled on Adiker/limusic@1fb1ded).
    #[tokio::test]
    async fn a_full_disk_fails_the_item_before_fetching_and_keeps_the_partial() {
        let server = serve(4 * 1024 * 1024, Mode::Ranges).await;
        let env = TestEnv::with_probe(
            "full-disk",
            server.url.clone(),
            251,
            "audio/webm",
            160_000,
            |_| Some(0), // the probe's answer: nothing is free
        );
        env.enqueue_one("fffffffffff", "No Room");
        // A partial from an earlier attempt must survive the failed preflight untouched.
        std::fs::create_dir_all(&env.dir).unwrap();
        let part = env.dir.join(part_name("fffffffffff", 251));
        let frozen = data(300 * 1024);
        std::fs::write(&part, &frozen).unwrap();

        env.drain().await;
        let row = env.row("fffffffffff");
        assert_eq!(row.state, "error", "the item fails, it does not crash");
        let err = row.error.expect("a readable error is recorded");
        assert!(err.contains("not enough free space"), "readable sentence: {err}");
        assert!(!err.contains("http"), "never a URL");
        assert_eq!(
            std::fs::read(&part).unwrap(),
            frozen,
            "the preflight fired before any write: the partial is byte-identical"
        );
        assert!(
            server.ranges.lock().unwrap().is_empty(),
            "no range request went out — the disk was checked before the fetch"
        );
        assert!(!env.dir.join("No Room [fffffffffff].webm").exists(), "and no final file");
    }

    /// The preflight must never be the reason a download stops when the platform cannot measure
    /// free space at all: unknown is not zero. The download proceeds and completes normally.
    #[tokio::test]
    async fn unknown_free_space_never_blocks_a_download() {
        let server = serve(1024 * 1024, Mode::Ranges).await;
        let env = TestEnv::with_probe(
            "no-probe",
            server.url.clone(),
            251,
            "audio/webm",
            160_000,
            |_| None, // the platform cannot say
        );
        env.enqueue_one("nnnnnnnnnnn", "Unmeasurable");
        env.drain().await;
        let row = env.row("nnnnnnnnnnn");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert_eq!(std::fs::read(env.done_path("nnnnnnnnnnn")).unwrap(), data(1024 * 1024));
    }

    /// The reserve arithmetic itself: `need + SPACE_RESERVE` decides, unknown skips.
    #[test]
    fn the_preflight_keeps_its_reserve_and_fails_readably() {
        let plenty = |_: &Path| Some(100 * 1024 * 1024);
        assert!(ensure_space(plenty, Path::new("."), CHUNK).is_ok(), "room to spare passes");
        let none = |_: &Path| Some(SPACE_RESERVE - 1);
        let err = ensure_space(none, Path::new("."), 0).expect_err("reserve itself must stay free");
        assert!(err.message().contains("not enough free space"));
        let blind = |_: &Path| None;
        assert!(ensure_space(blind, Path::new("."), u64::MAX).is_ok(), "unknown never blocks");
    }

    /// A finished download saves its cover beside the audio, records it, and every reader prefers
    /// the local file — which is what makes the row show artwork with no network (offline).
    #[tokio::test]
    async fn a_finished_download_saves_its_cover_for_offline_use() {
        let server = serve(512 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("artwork", server.url.clone(), 251, "audio/webm", 160_000);
        let item = SongItem {
            video_id: "wwwwwwwwwww".to_owned(),
            title: "Covered Song".to_owned(),
            artists: "Tester".to_owned(),
            thumbnail: Some(format!("{}/cover.jpg", server.url)),
            ..Default::default()
        };
        let mut out = enqueue_items(&env.ctx, &[item], "HIGH");
        assert_eq!(out.len(), 1);
        out.remove(0);
        env.drain().await;

        let row = env.row("wwwwwwwwwww");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        let art = row.artwork_path.as_deref().expect("the cover was recorded");
        let art_path = PathBuf::from(art);
        assert!(
            art_path.starts_with(env.dir.join(ARTWORK_DIR)),
            "stored under the download dir's artwork folder: {art}"
        );
        assert_eq!(art_path.file_name().unwrap().to_string_lossy(), "wwwwwwwwwww.jpg");
        // The fixture answers `.jpg` paths with its whole body as image/jpeg.
        assert_eq!(
            std::fs::read(&art_path).unwrap(),
            data(512 * 1024),
            "the cover bytes, byte-exact"
        );

        // Every reader prefers the saved file over the remote URL...
        let item = list(&env.ctx).into_iter().find(|i| i.id == "wwwwwwwwwww").unwrap();
        assert_eq!(item.thumbnail.as_deref(), Some(art), "the list shows the local cover");
        let pb = offline_playback(&env.db, "wwwwwwwwwww").unwrap();
        assert_eq!(pb.thumbnail.as_deref(), Some(art), "and so does offline playback");
        // ...and the audio file itself is untouched by the artwork step.
        assert_eq!(std::fs::read(env.done_path("wwwwwwwwwww")).unwrap(), data(512 * 1024));
    }

    /// Artwork is a nice-to-have: a cover fetch that fails (unreachable host, non-image answer)
    /// must leave the download `done` with no error, keeping the remote thumbnail as fallback.
    #[tokio::test]
    async fn a_failed_cover_fetch_never_fails_the_download() {
        let server = serve(256 * 1024, Mode::Ranges).await;
        // Nothing listens on port 9 (discard) — the cover GET fails fast.
        let env = TestEnv::new("artwork-fail", server.url.clone(), 251, "audio/webm", 160_000);
        let item = SongItem {
            video_id: "vvvvvvvvvvv".to_owned(),
            title: "Uncovered Song".to_owned(),
            artists: "Tester".to_owned(),
            thumbnail: Some("http://[IP_ADDRESS]:9/cover.jpg".to_owned()),
            ..Default::default()
        };
        enqueue_items(&env.ctx, &[item], "HIGH");
        env.drain().await;

        let row = env.row("vvvvvvvvvvv");
        assert_eq!(row.state, "done", "the download itself succeeded: {:?}", row.error);
        assert!(row.error.is_none(), "and carries no error");
        assert!(row.artwork_path.is_none(), "no cover was recorded");
        let item = list(&env.ctx).into_iter().find(|i| i.id == "vvvvvvvvvvv").unwrap();
        assert_eq!(
            item.thumbnail.as_deref(),
            row.thumbnail.as_deref(),
            "the remote thumbnail stays the fallback"
        );

        // The non-image answer is refused too: the same host serving audio to a `.jpg` URL...
        let bad = SongItem {
            video_id: "uuuuuuuuuuu".to_owned(),
            title: "Wrong Type".to_owned(),
            artists: "Tester".to_owned(),
            // ...but the fixture answers .jpg paths as images; point at the audio root instead,
            // whose Content-Type is audio/webm — fetch_cover must refuse it.
            thumbnail: Some(server.url.clone()),
            ..Default::default()
        };
        enqueue_items(&env.ctx, &[bad], "HIGH");
        env.drain().await;
        let row = env.row("uuuuuuuuuuu");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        assert!(row.artwork_path.is_none(), "a non-image body is not saved as a cover");
    }

    /// Removing a download takes its saved cover with it — no orphaned files in artwork/.
    #[tokio::test]
    async fn removing_a_download_removes_its_cover() {
        let server = serve(128 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("artwork-remove", server.url.clone(), 251, "audio/webm", 160_000);
        let item = SongItem {
            video_id: "rrrrrrrrrrr".to_owned(),
            title: "Removed Cover".to_owned(),
            artists: "Tester".to_owned(),
            thumbnail: Some(format!("{}/cover.jpg", server.url)),
            ..Default::default()
        };
        enqueue_items(&env.ctx, &[item], "HIGH");
        env.drain().await;
        let art = PathBuf::from(env.row("rrrrrrrrrrr").artwork_path.expect("cover saved"));
        assert!(art.exists());

        action(&env.ctx, "rrrrrrrrrrr", "remove").unwrap();
        assert!(!art.exists(), "the cover goes with the row");
        assert!(!env.dir.join(ARTWORK_DIR).join("rrrrrrrrrrr.jpg").exists());
    }

    #[tokio::test]
    async fn cancel_deletes_the_partial_and_stops_the_worker() {
        let server = serve(3 * 1024 * 1024, Mode::Slow).await;
        let env = TestEnv::new("cancel", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("ggggggggggg", "Cancel Me");

        let worker = tokio::spawn({
            let ctx = Arc::clone(&env.ctx);
            async move { while pump(&ctx).await > 0 {} }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "never saw progress to cancel mid-flight");
            if env.row("ggggggggggg").bytes_done > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        action(&env.ctx, "ggggggggggg", "cancel").unwrap();
        worker.await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let row = env.row("ggggggggggg");
        assert_eq!(row.state, "cancelled");
        assert_eq!(row.bytes_done, 0, "no bytes are claimed after a cancel");
        assert!(
            !env.dir.join(part_name("ggggggggggg", 251)).exists(),
            "the worker deleted its partial"
        );
        // A retry restarts cleanly and succeeds.
        action(&env.ctx, "ggggggggggg", "retry").unwrap();
        env.drain().await;
        assert_eq!(env.row("ggggggggggg").state, "done");
    }

    #[tokio::test]
    async fn remove_deletes_files_only_inside_the_downloads_folder() {
        let len = 1024 * 1024;
        let server = serve(len, Mode::Ranges).await;
        let env = TestEnv::new("remove", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("hhhhhhhhhhh", "Remove Me");
        env.drain().await;
        let path = env.done_path("hhhhhhhhhhh");
        assert!(path.exists());

        action(&env.ctx, "hhhhhhhhhhh", "remove").unwrap();
        assert!(env.db.download("hhhhhhhhhhh").is_none(), "the row is gone");
        assert!(!path.exists(), "the file is gone");

        // A tampered path is refused: the file outside the folder is never touched, and the row
        // survives the failed removal so the bytes it names are never forgotten.
        let outside = env.dir.parent().unwrap().join("outside.txt");
        std::fs::write(&outside, b"keep me").unwrap();
        let item = SongItem {
            video_id: "iiiiiiiiiii".to_owned(),
            title: "Tampered".to_owned(),
            artists: "Tester".to_owned(),
            ..Default::default()
        };
        enqueue_items(&env.ctx, &[item], "HIGH");
        assert!(ctx_claim(&env, "iiiiiiiiiii"));
        assert!(
            env.db.finish_download("iiiiiiiiiii", &outside.to_string_lossy(), 8),
            "the row claims a finished file"
        );
        let err = action(&env.ctx, "iiiiiiiiiii", "remove").unwrap_err();
        assert!(err.contains("outside the downloads folders"), "the refusal is explained: {err}");
        assert!(
            env.db.download("iiiiiiiiiii").is_some(),
            "a row whose file could not be deleted keeps its row"
        );
        assert!(outside.exists(), "a file outside the downloads folder is never deleted");

        // The same row, its path now inside a folder the engine really owns, removes cleanly.
        std::fs::create_dir_all(&env.dir).unwrap();
        let inside_path = env.dir.join("Movable [iiiiiiiiiii].m4a");
        std::fs::write(&inside_path, b"mine").unwrap();
        assert!(env.db.set_download_state_if("iiiiiiiiiii", &["done"], "downloading", None));
        assert!(env.db.finish_download("iiiiiiiiiii", &inside_path.to_string_lossy(), 4));
        action(&env.ctx, "iiiiiiiiiii", "remove").unwrap();
        assert!(env.db.download("iiiiiiiiiii").is_none(), "the row goes once its file can go");
        assert!(!inside_path.exists());
    }

    #[tokio::test]
    async fn removing_after_a_folder_change_still_deletes_the_file_it_wrote() {
        let server = serve(512 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("dirchange", server.url.clone(), 251, "audio/webm", 160_000);
        let old = env.dir.parent().unwrap().join("old-downloads");
        env.db.set_setting("download_dir", &old.to_string_lossy());
        env.enqueue_one("lllllllllll", "Old Folder");
        env.drain().await;
        let path = env.done_path("lllllllllll");
        assert!(path.starts_with(&old), "written where the setting pointed then");

        // The user points the app somewhere else; the file already on disk must still be
        // removable, or `remove` deletes the row and leaks the bytes it names.
        let new_dir = env.dir.parent().unwrap().join("new-downloads");
        env.db.set_setting("download_dir", &new_dir.to_string_lossy());
        action(&env.ctx, "lllllllllll", "remove").unwrap();
        assert!(env.db.download("lllllllllll").is_none(), "the row goes");
        assert!(!path.exists(), "and the file in the folder it was written to goes with it");
    }

    #[tokio::test]
    async fn a_folder_change_carries_the_partial_it_left_behind() {
        let len = 3 * 1024 * 1024;
        let server = serve(len, Mode::Slow).await;
        let env = TestEnv::new("dirpartial", server.url.clone(), 251, "audio/webm", 160_000);
        let old = env.dir.parent().unwrap().join("old-downloads");
        env.db.set_setting("download_dir", &old.to_string_lossy());
        env.enqueue_one("mmmmmmmmmmm", "Carry Over");

        let worker = tokio::spawn({
            let ctx = Arc::clone(&env.ctx);
            async move { while pump(&ctx).await > 0 {} }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "never saw progress to pause mid-flight");
            let row = env.row("mmmmmmmmmmm");
            if row.state == "downloading" && row.bytes_done > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        action(&env.ctx, "mmmmmmmmmmm", "pause").unwrap();
        worker.await.unwrap();

        let part = old.join(part_name("mmmmmmmmmmm", 251));
        let frozen = std::fs::metadata(&part).expect("the partial stayed in the old folder").len();
        assert!(frozen > 0 && frozen < len as u64, "a real mid-file partial: {frozen}");

        // Move the folder, resume: the bytes are carried over, not restarted and not stranded.
        let new_dir = env.dir.parent().unwrap().join("new-downloads");
        env.db.set_setting("download_dir", &new_dir.to_string_lossy());
        action(&env.ctx, "mmmmmmmmmmm", "resume").unwrap();
        env.drain().await;

        let row = env.row("mmmmmmmmmmm");
        assert_eq!(row.state, "done", "error: {:?}", row.error);
        let path = env.done_path("mmmmmmmmmmm");
        assert!(path.starts_with(&new_dir), "finished in the folder configured now");
        assert_eq!(std::fs::read(&path).unwrap(), data(len), "byte-exact across the change");
        assert!(!part.exists(), "the old partial was carried over, not stranded");
        let ranges = server.ranges.lock().unwrap().clone();
        assert!(
            ranges.iter().any(|(s, _)| *s == frozen as u64),
            "the resume continued from the bytes the old folder held: {ranges:?}"
        );
    }

    #[tokio::test]
    async fn a_restart_requeues_interrupted_rows_and_sweeps_orphans() {
        let server = serve(1024 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("recover", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("jjjjjjjjjjj", "Interrupted");
        assert!(ctx_claim(&env, "jjjjjjjjjjj"), "claimed, now 'downloading'");
        assert_eq!(env.row("jjjjjjjjjjj").state, "downloading");
        // The process dies with the claim: only the database survives, not the engine's in-memory
        // bookkeeping (which is what `recover` is about).
        env.ctx.running.lock().unwrap().remove("jjjjjjjjjjj");

        // A partial nothing references, and one an error row could still resume.
        std::fs::create_dir_all(&env.dir).unwrap();
        let orphan = env.dir.join(part_name("zzzzzzzzzzz", 251));
        std::fs::write(&orphan, b"orphan").unwrap();

        recover(&env.ctx);
        assert_eq!(
            env.row("jjjjjjjjjjj").state,
            "queued",
            "an interrupted row goes back to the queue"
        );
        assert!(!orphan.exists(), "an orphaned partial is swept");

        env.drain().await;
        assert_eq!(env.row("jjjjjjjjjjj").state, "done");
    }

    #[tokio::test]
    async fn re_adding_a_done_track_does_not_download_it_twice() {
        let server = serve(512 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("dedupe", server.url.clone(), 251, "audio/webm", 160_000);
        env.enqueue_one("kkkkkkkkkkk", "Once Only");
        env.drain().await;
        let requests = server.ranges.lock().unwrap().len();

        let again = env.enqueue_one("kkkkkkkkkkk", "Once Only");
        assert_eq!(again.state, "done", "a done row is returned as it stands");
        env.drain().await;
        assert_eq!(server.ranges.lock().unwrap().len(), requests, "nothing was fetched again");

        // An error row re-enqueued restarts; a retry gets its own state transition.
        assert!(env.db.set_download_state_if("kkkkkkkkkkk", &["done"], "error", Some("boom")));
        let restarted = env.enqueue_one("kkkkkkkkkkk", "Once Only");
        assert_eq!(restarted.state, "queued");
        env.drain().await;
        assert_eq!(env.row("kkkkkkkkkkk").state, "done");
    }

    #[tokio::test]
    async fn an_unwritable_downloads_folder_is_a_clear_error() {
        let server = serve(4096, Mode::Ranges).await;
        let env = TestEnv::new("unwritable", server.url.clone(), 251, "audio/webm", 160_000);
        // A file where the folder should be: create_dir_all cannot win.
        std::fs::write(&env.dir, b"file").unwrap();
        env.enqueue_one("lllllllllll", "No Folder");
        env.drain().await;
        let row = env.row("lllllllllll");
        assert_eq!(row.state, "error");
        assert!(
            row.error.as_deref().unwrap_or_default().contains("downloads folder"),
            "{:?}",
            row.error
        );

        // Remove the obstacle: a retry succeeds.
        std::fs::remove_file(&env.dir).unwrap();
        action(&env.ctx, "lllllllllll", "retry").unwrap();
        env.drain().await;
        assert_eq!(env.row("lllllllllll").state, "done");
    }

    #[tokio::test]
    async fn offline_playback_uses_the_file_and_forgets_a_missing_one() {
        let server = serve(256 * 1024, Mode::Ranges).await;
        let env = TestEnv::new("offline", server.url.clone(), 140, "audio/mp4", 128_000);
        env.enqueue_one("mmmmmmmmmmm", "Offline");
        env.drain().await;
        let path = env.done_path("mmmmmmmmmmm");

        let data = offline_playback(&env.db, "mmmmmmmmmmm").expect("a done row plays from disk");
        assert_eq!(data.stream_url, path.to_string_lossy());
        assert_eq!(data.stream_client, "download");
        assert!(data.playback_ping.is_none(), "a local file must not ping YouTube");
        assert_eq!(data.itag, 140);

        std::fs::remove_file(&path).unwrap();
        assert!(offline_playback(&env.db, "mmmmmmmmmmm").is_none());
        let row = env.row("mmmmmmmmmmm");
        assert_eq!(row.state, "error", "the missing file is reported, not played as if present");
        assert!(row.error.as_deref().unwrap_or_default().contains("missing"));
    }

    #[tokio::test]
    async fn playlist_collection_reads_a_local_playlist_and_on_repeat() {
        let db = Db::open(std::path::Path::new(":memory:")).unwrap();
        let song = |v: &str, title: &str| SongItem {
            video_id: v.to_owned(),
            title: title.to_owned(),
            ..Default::default()
        };
        let key = db.create_local_playlist("Road trip", 10).unwrap();
        let rows: Vec<(String, String)> = [song("nnnnnnnnnnn", "One"), song("ooooooooooo", "Two")]
            .iter()
            .map(|s| (s.video_id.clone(), serde_json::to_string(s).unwrap()))
            .collect();
        db.add_local_playlist_tracks(key, &rows, 20).unwrap();
        let got = local_playlist_songs(&db, key);
        assert_eq!(
            got.iter().map(|s| s.video_id.as_str()).collect::<Vec<_>>(),
            ["nnnnnnnnnnn", "ooooooooooo"]
        );

        let play = serde_json::to_string(&song("ppppppppppp", "Played")).unwrap();
        db.record_play(
            "ppppppppppp",
            &play,
            crate::db::now_secs(),
            crate::state::ON_REPEAT_WINDOW_SECS,
        );
        let repeat = on_repeat_songs(&db);
        assert_eq!(repeat.first().map(|s| s.video_id.as_str()), Some("ppppppppppp"));
    }

    // --- naming and settings ------------------------------------------------------------------

    #[test]
    fn file_names_are_safe_and_identifiable() {
        let stem = file_stem("AC/DC: Back in Black? <Live>", "abc-123_XYZ");
        assert_eq!(stem, "AC DC Back in Black Live [abc-123_XYZ]");
        assert!(!stem.contains(['/', '\\', ':', '?', '*', '<', '>', '"', '|']));

        assert_eq!(
            file_stem("", "abcdefghijk"),
            "abcdefghijk",
            "a blank title falls back to the id"
        );
        assert_eq!(file_stem("..", "abcdefghijk"), "abcdefghijk", "dots alone are not a name");
        let long = file_stem(&"x".repeat(500), "abcdefghijk");
        assert!(long.len() < 160, "long titles are capped: {}", long.len());
        assert!(long.ends_with("[abcdefghijk]"));

        assert_eq!(part_stem_of("A Song [abc].itag251.part"), Some("A Song [abc]"));
        assert_eq!(part_stem_of("A Song [abc].part"), None);
        assert_eq!(part_stem_of("A Song [abc].itagX.part"), None);
        assert_eq!(part_stem_of("random.bin"), None);
    }

    #[test]
    fn extensions_and_labels_name_the_real_format() {
        assert_eq!(ext_for_mime("audio/webm; codecs=opus"), "webm");
        assert_eq!(ext_for_mime("audio/mp4; codecs=mp4a.40.2"), "m4a");
        assert_eq!(ext_for_mime("audio/mpeg"), "mp3");
        assert_eq!(ext_for_mime(""), "m4a");

        let src = |mime: &str, bitrate: Option<i64>| Source {
            url: String::new(),
            headers: HashMap::new(),
            itag: 251,
            mime: Some(mime.to_owned()),
            bitrate,
        };
        assert_eq!(actual_label(&src("audio/webm; codecs=opus", Some(160_000))), "160 kbps Opus");
        assert_eq!(
            actual_label(&src("audio/mp4; codecs=mp4a.40.2", Some(128_000))),
            "128 kbps AAC"
        );
        assert_eq!(actual_label(&src("audio/mpeg", None)), "MP3");
    }

    #[test]
    fn settings_are_validated_where_they_land() {
        let cache = std::env::temp_dir().join("limusic-cache-for-download-tests");
        std::fs::create_dir_all(&cache).unwrap();

        assert!(validate_setting("download_quality", "HIGH", &cache).is_ok());
        assert!(validate_setting("download_quality", "loud", &cache).is_err());
        assert!(validate_setting("download_concurrency", "1", &cache).is_ok());
        assert!(validate_setting("download_concurrency", "4", &cache).is_ok());
        assert!(validate_setting("download_concurrency", "0", &cache).is_err());
        assert!(validate_setting("download_concurrency", "5", &cache).is_err());
        assert!(validate_setting("download_concurrency", "two", &cache).is_err());
        assert!(validate_setting("download_dir", "", &cache).is_ok(), "empty clears");
        assert!(
            validate_setting("download_dir", "relative/dir", &cache).is_err(),
            "a relative dir is refused"
        );
        assert!(
            validate_setting("download_dir", &cache.join("inside").to_string_lossy(), &cache)
                .is_err(),
            "inside the audio cache is refused"
        );
        let other = std::env::temp_dir().join("limusic-elsewhere");
        assert!(validate_setting("download_dir", &other.to_string_lossy(), &cache).is_ok());
    }

    #[test]
    fn quality_requests_are_picked_and_refused() {
        let db = Db::open(std::path::Path::new(":memory:")).unwrap();
        assert_eq!(picked_quality(&db, None).unwrap(), "HIGH", "the default setting");
        assert_eq!(picked_quality(&db, Some("low")).unwrap(), "LOW", "case folds");
        db.set_setting("download_quality", "AUTO");
        assert_eq!(picked_quality(&db, None).unwrap(), "AUTO");
        db.set_setting("download_quality", "nonsense");
        assert_eq!(picked_quality(&db, None).unwrap(), "HIGH", "a bad stored value falls back");
        assert!(picked_quality(&db, Some("lossless")).is_err(), "no lossless tier exists");
    }

    #[test]
    fn concurrency_is_bounded_even_from_a_hand_edited_database() {
        let db = Db::open(std::path::Path::new(":memory:")).unwrap();
        assert_eq!(concurrency(&db), 2, "the default");
        db.set_setting("download_concurrency", "4");
        assert_eq!(concurrency(&db), 4);
        db.set_setting("download_concurrency", "99");
        assert_eq!(concurrency(&db), 2, "out of range reads as the default");
    }

    #[test]
    fn video_ids_are_whitelisted_before_they_reach_a_file_name() {
        assert!(valid_video_id("dQw4w9WgXcQ"));
        assert!(valid_video_id("abc-123_XYZ"));
        assert!(!valid_video_id(""));
        assert!(!valid_video_id("LOCAL:/music/a.mp3"), "a file on disk needs no download");
        assert!(!valid_video_id("../../etc/passwd"));
        assert!(!valid_video_id(&"x".repeat(65)));
    }

    // --- anonymous identity (downloads must never carry the account) --------------------------

    /// The request builder for a download's byte fetch refuses credentials outright — even if a
    /// resolver (or an echo) hands over account headers. Everything else travels untouched, so
    /// transport correctness (UA, ranges) is untouched too.
    #[test]
    fn the_download_request_builder_refuses_account_headers() {
        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_owned(), "UA/1".to_owned());
        headers.insert("Range".to_owned(), "bytes=0-4194303".to_owned());
        headers.insert("Cookie".to_owned(), "SAPISID=secret; SID=also-secret".to_owned());
        headers.insert("authorization".to_owned(), "SAPISIDHASH 1712_x".to_owned());
        headers.insert("PROXY-Authorization".to_owned(), "Basic z".to_owned());

        let out = anonymous_fetch_headers(headers);
        assert_eq!(out.get("User-Agent").map(String::as_str), Some("UA/1"));
        assert_eq!(out.get("Range").map(String::as_str), Some("bytes=0-4194303"));
        assert!(!out.keys().any(|k| k.trim().eq_ignore_ascii_case("cookie")), "no cookie");
        assert!(
            !out.keys().any(|k| k.trim().eq_ignore_ascii_case("authorization")),
            "no SAPISIDHASH"
        );
        assert!(
            !out.keys().any(|k| k.trim().eq_ignore_ascii_case("proxy-authorization")),
            "no proxy credentials"
        );
    }

    /// Anonymous (the default) says what would actually fix it; the playback wording would tell
    /// a signed-in user to sign in. With the setting on, the account path is unchanged.
    #[test]
    fn an_anonymous_failure_says_sign_in_required_and_never_the_playback_sentence() {
        use crate::orchestrator::ResolveError as E;
        let id = || "dQw4w9WgXcQ".to_owned();

        assert_eq!(
            failure_message(&E::SignInRequired(id()), true),
            "Sign-in required for this track"
        );
        assert_eq!(
            failure_message(&E::UploadUnavailable(id()), true),
            "Sign-in required for this track",
            "one of the user's own uploads can only ever stream to the account"
        );
        // Network trouble is not a sign-in problem, with or without the setting.
        assert_eq!(failure_message(&E::Unreachable(id()), true), E::Unreachable(id()).to_string());
        // Setting on: the orchestrator's own wording stands, byte for byte.
        assert_eq!(
            failure_message(&E::SignInRequired(id()), false),
            E::SignInRequired(id()).to_string()
        );
        assert_eq!(
            failure_message(&E::UploadUnavailable(id()), false),
            E::UploadUnavailable(id()).to_string()
        );
    }

    /// `download_use_account` is the only way a download touches the account, and it starts off.
    /// A hand-edited non-boolean never opts in, and the UI write path refuses it outright.
    #[test]
    fn downloads_are_anonymous_until_the_setting_says_otherwise() {
        let db = Db::open(std::path::Path::new(":memory:")).unwrap();
        assert!(!use_account(&db), "the default is anonymous");
        db.set_setting("download_use_account", "false");
        assert!(!use_account(&db));
        db.set_setting("download_use_account", "true");
        assert!(use_account(&db), "the escape hatch opts in");
        db.set_setting("download_use_account", "yes");
        assert!(!use_account(&db), "hand-edited garbage never opts in");

        let cache = std::path::Path::new(":memory:");
        assert!(validate_setting("download_use_account", "true", cache).is_ok());
        assert!(validate_setting("download_use_account", "false", cache).is_ok());
        assert!(validate_setting("download_use_account", "on", cache).is_err());
        assert!(validate_setting("download_use_account", "1", cache).is_err());
    }

    /// A resolve that fails anonymously fails the queue item with that sentence, once: the
    /// worker does not get a second — authenticated — attempt behind it.
    #[tokio::test]
    async fn a_failed_anonymous_resolve_fails_the_item_once_with_no_authenticated_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = Arc::clone(&calls);
        let resolve: Resolver = Arc::new(move |_video, _quality, _upload| {
            let spy = Arc::clone(&spy);
            Box::pin(async move {
                spy.fetch_add(1, Ordering::SeqCst);
                // Exactly what `prod_resolver` hands back for an anonymous age/members refusal.
                Err("Sign-in required for this track".to_owned())
            })
        });
        let env = TestEnv::with_resolver("resolve-spy", resolve, free_space);
        env.enqueue_one("aaaaaaaaaaa", "Members only");
        env.drain().await;

        let row = env.row("aaaaaaaaaaa");
        assert_eq!(row.state, "error", "the item fails instead of stalling or silently retrying");
        assert_eq!(row.error.as_deref(), Some("Sign-in required for this track"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "one attempt: a second call here would be the authenticated fallback"
        );
    }
}
