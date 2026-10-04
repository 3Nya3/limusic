//! Spotify import (#375): match a Spotify library to YouTube Music and build the playlists.
//!
//! [`crate::spotify`] reads the source; this decides what each track is on YouTube Music and does
//! the writing. One import at a time, in the background, in three phases:
//!
//! 1. **Matching.** One `FILTER_SONG` search per distinct track (a song in five playlists is one
//!    search), scored by [`score`], plus a video search when no song scores well. The searches
//!    are unrecorded, so a 2,000-track import doesn't bury the user's search history, and paced,
//!    because they leave from the user's own IP and a burst is what gets an IP bot-flagged. Every
//!    answer lands in `import_matches`, so a second import, a retry after a failure and "Update
//!    from Spotify" only search for what is new.
//! 2. **Review.** The job waits while the user changes any pick. Nothing has been written yet.
//! 3. **Creating.** One playlist per list, on the account or on this machine, then the opt-in
//!    extras: likes for Liked Songs, follows for followed artists, saves for saved albums.
//!
//! The UI follows along through `import-progress` events, each carrying a [`Snapshot`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use innertube::{BrowseItem, Rating, SongItem};
use serde::{Deserialize, Serialize};
use tauri::Emitter;
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

use crate::db::{now_secs, ImportMatch};
use crate::spotify::{self, LinkKind, ListKind, SavedAlbum, SourceList, SourceTrack};
use crate::state::{is_local_playlist, AppState, LOCAL_PLAYLIST_PREFIX};

// --- matching ------------------------------------------------------------------------------------

/// At or above: taken without a second look.
const MATCHED: f64 = 0.80;
/// At or above: taken as a best guess, and listed for review.
const CHECK: f64 = 0.55;
/// YouTube Music's playlist cap. A longer list is split into "Name", "Name (2)", ...
const YTM_PLAYLIST_MAX: usize = 5000;

/// Words in a title's dressing that make it a different recording, so both sides have to agree on
/// them. "Remastered", "radio edit", "mono" and the like are the same song and stay out.
const VERSIONS: &[&str] = &[
    "live",
    "acoustic",
    "remix",
    "instrumental",
    "karaoke",
    "cover",
    "demo",
    "sped",
    "slowed",
    "reverb",
    "nightcore",
    "8d",
    "acapella",
    "cappella",
    "unplugged",
    "orchestral",
    "piano",
    "lofi",
    "extended",
    "reprise",
    "taylors",
];

/// Lowercase, accents off, apostrophes gone ("don't" is "dont"), `&` as "and", anything else that
/// isn't a letter or digit as a space. Letters of every script survive.
fn norm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.nfkd() {
        match c {
            _ if is_combining_mark(c) => {}
            '\'' | '\u{2019}' | '`' => {}
            '&' => out.push_str(" and "),
            _ if c.is_alphanumeric() => out.extend(c.to_lowercase()),
            _ => out.push(' '),
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The title without its dressing, and the dressing: bracketed parts, a Spotify-style
/// " - 2011 Remaster" tail and a bare "feat. X" tail. Both as written, not normalized.
fn strip_dressing(title: &str) -> (String, String) {
    let mut base = String::new();
    let mut dressing = String::new();
    let mut depth = 0u32;
    for c in title.chars() {
        match c {
            '(' | '[' | '{' => {
                depth += 1;
                dressing.push(' ');
            }
            ')' | ']' | '}' => {
                depth = depth.saturating_sub(1);
                dressing.push(' ');
            }
            _ if depth > 0 => dressing.push(c),
            _ => base.push(c),
        }
    }
    for sep in [" - ", " \u{2013} ", " \u{2014} "] {
        if let Some(i) = base.find(sep) {
            dressing.push(' ');
            dressing.push_str(&base[i + sep.len()..]);
            base.truncate(i);
        }
    }
    let feat = base.char_indices().filter(|(_, c)| *c == ' ').map(|(i, _)| i).find(|&i| {
        let word = base[i + 1..].split(' ').next().unwrap_or("").to_lowercase();
        matches!(word.trim_end_matches('.'), "feat" | "ft" | "featuring")
    });
    if let Some(i) = feat {
        dressing.push_str(&base[i..]);
        base.truncate(i);
    }
    let base = base.trim();
    // All dressing ("(Intro)"): the dressing is the title.
    if base.is_empty() {
        return (title.trim().to_owned(), String::new());
    }
    (base.to_owned(), dressing)
}

fn versions(dressing: &str) -> Vec<&'static str> {
    let words = norm(dressing);
    let words: HashSet<&str> = words.split(' ').collect();
    VERSIONS.iter().copied().filter(|v| words.contains(v)).collect()
}

/// "3:45" or "1:02:03" in seconds.
fn secs(d: &str) -> Option<f64> {
    d.split(':').try_fold(0.0, |acc, p| p.trim().parse::<f64>().ok().map(|n| acc * 60.0 + n))
}

/// How much of the source's artist list the candidate names. The first artist counts most; a
/// featured artist is looked for in the title too, which is where YouTube often puts one.
fn artist_score(src: &[String], cand: &SongItem) -> f64 {
    let Some((first, rest)) = src.split_first() else {
        return 0.5;
    };
    let hay = format!(" {} {} ", norm(&cand.artists), norm(&cand.title));
    let pieces: Vec<String> =
        cand.artists.split([',', '&']).map(norm).filter(|p| !p.is_empty()).collect();
    let found = |a: &String| {
        let a = norm(a);
        !a.is_empty()
            && (hay.contains(&format!(" {a} "))
                || pieces.iter().any(|p| strsim::jaro_winkler(p, &a) >= 0.92))
    };
    let others = (!rest.is_empty())
        .then(|| rest.iter().filter(|a| found(a)).count() as f64 / rest.len() as f64);
    if found(first) {
        0.7 + 0.3 * others.unwrap_or(1.0)
    } else {
        // The order differs between the two (a collab listed the other way round): partial credit.
        0.35 * others.unwrap_or(0.0)
    }
}

fn same_album(src: Option<&str>, cand: Option<&str>) -> bool {
    let (Some(a), Some(b)) = (src, cand) else {
        return false;
    };
    let (a, b) = (norm(&strip_dressing(a).0), norm(&strip_dressing(b).0));
    !a.is_empty() && !b.is_empty() && (a == b || a.contains(&b) || b.contains(&a))
}

fn title_score(a: &str, b: &str) -> f64 {
    strsim::sorensen_dice(&norm(a), &norm(b))
}

/// How sure we are that `cand` is `src`, 0 to 1. Title, artists and duration carry it; the album
/// is a small bonus (a single and its album cut are the same song); a version word on one side
/// only (live, remix, sped up) is a heavy penalty, and so is a duration off by more than 20 s,
/// which is how a cover or an extended mix with the right name gives itself away.
pub fn score(src: &SourceTrack, cand: &SongItem) -> f64 {
    let (src_base, src_dressing) = strip_dressing(&src.title);
    let (cand_base, cand_dressing) = strip_dressing(&cand.title);
    let title = title_score(&src_base, &cand_base).max(title_score(&src.title, &cand.title));
    let mut num = 0.45 * title + 0.35 * artist_score(&src.artists, cand);
    let mut den = 0.8;
    let mut penalty = 0.0;
    if let (Some(ms), Some(c)) = (src.duration_ms, cand.duration.as_deref().and_then(secs)) {
        let off = (ms as f64 / 1000.0 - c).abs();
        num += 0.2 * (1.0 - (off - 3.0).max(0.0) / 17.0).max(0.0);
        den += 0.2;
        if off > 20.0 {
            penalty += 0.15;
        }
    }
    let mut s = num / den;
    if same_album(src.album.as_deref(), cand.album.as_deref()) {
        s += 0.05;
    }
    if versions(&src_dressing) != versions(&cand_dressing) {
        penalty += 0.35;
    }
    if src.explicit.is_some_and(|e| e != cand.explicit) {
        penalty += 0.04;
    }
    (s - penalty).clamp(0.0, 1.0)
}

/// What to type into the search box: the bare title, any version word (searching "Song" for
/// "Song (Live)" finds the studio cut), and the first artist.
fn query(t: &SourceTrack) -> String {
    let (base, dressing) = strip_dressing(&t.title);
    let mut q = base;
    for v in versions(&dressing) {
        q.push(' ');
        q.push_str(v);
    }
    if let Some(a) = t.artists.first() {
        q.push(' ');
        q.push_str(a);
    }
    q
}

fn rank(src: &SourceTrack, items: Vec<SongItem>, out: &mut Vec<(f64, SongItem)>) {
    for (i, c) in items.into_iter().enumerate() {
        if c.video_id.is_empty() || out.iter().any(|(_, o)| o.video_id == c.video_id) {
            continue;
        }
        // YouTube's own order breaks a near tie: its first answer is usually the canonical one.
        let s = score(src, &c) + if i == 0 { 0.02 } else { 0.0 };
        out.push((s.min(1.0), c));
    }
}

/// Between two searches. The same rhythm as someone working through a list by hand.
fn pace() -> Duration {
    Duration::from_millis(450 + rand::random::<u64>() % 300)
}

fn metadata_client(state: &AppState) -> Result<&innertube::YouTubeClient, String> {
    state.clients.get(innertube::METADATA_CLIENT).ok_or_else(|| "metadata client missing".into())
}

/// Search for one track: songs first, then videos when no song scored well (covers, live sets
/// and releases that never got an official upload only exist as videos; the video search answers
/// nothing when the user hides music videos). Best first, at most six.
async fn search(
    state: &AppState,
    src: &SourceTrack,
) -> Result<Vec<(f64, SongItem)>, innertube::Error> {
    let client = metadata_client(state).map_err(innertube::Error::Other)?;
    let q = query(src);
    let mut out = Vec::new();
    rank(src, state.it.search_songs(client, &q, false).await?.items, &mut out);
    if out.iter().all(|(s, _)| *s < MATCHED) {
        tokio::time::sleep(pace()).await;
        if let Ok(r) = state.it.search_videos(client, &q).await {
            rank(src, r.items, &mut out);
        }
    }
    out.sort_by(|a, b| b.0.total_cmp(&a.0));
    out.truncate(6);
    Ok(out)
}

/// YouTube telling us to slow down. Everything matched so far is cached, so stopping costs
/// nothing but time; pushing on is how the IP ends up flagged.
fn throttled(e: &innertube::Error) -> bool {
    matches!(e, innertube::Error::Http(h) if matches!(h.status().map(|s| s.as_u16()), Some(429 | 403)))
}

// --- rows and the cache --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Pending,
    Matched,
    Check,
    Missing,
}

impl Tier {
    fn as_str(self) -> &'static str {
        match self {
            Tier::Pending => "pending",
            Tier::Matched => "matched",
            Tier::Check => "check",
            Tier::Missing => "missing",
        }
    }

    fn parse(s: &str) -> Option<Tier> {
        Some(match s {
            "matched" => Tier::Matched,
            "check" => Tier::Check,
            "missing" => Tier::Missing,
            _ => return None,
        })
    }
}

/// The cache key: Spotify's track id, or for a track with none (a local file, a CSV row) its
/// normalized title and first artist.
fn key(t: &SourceTrack) -> String {
    match &t.id {
        Some(id) => id.clone(),
        None => format!(
            "~{}|{}",
            norm(&t.title),
            t.artists.first().map(|a| norm(a)).unwrap_or_default()
        ),
    }
}

fn classify(ranked: Vec<(f64, SongItem)>) -> (Tier, Option<SongItem>, Vec<SongItem>) {
    let best = ranked.first().map_or(0.0, |(s, _)| *s);
    let tier = if best >= MATCHED {
        Tier::Matched
    } else if best >= CHECK {
        Tier::Check
    } else {
        Tier::Missing
    };
    let candidates: Vec<SongItem> = ranked.into_iter().map(|(_, c)| c).collect();
    let pick = (tier != Tier::Missing).then(|| candidates[0].clone());
    (tier, pick, candidates)
}

/// A song YouTube Music didn't have a week ago may be there now.
const MISSING_TTL: i64 = 7 * 86_400;

type Answer = (Tier, Option<SongItem>, Vec<SongItem>);

fn cached(state: &AppState, key: &str) -> Option<Answer> {
    let m = state.db.get_import_match(key)?;
    let tier = Tier::parse(&m.tier)?;
    if tier == Tier::Missing && !m.manual && now_secs() - m.updated_at > MISSING_TTL {
        return None;
    }
    let pick: Option<SongItem> = m.song_json.and_then(|j| serde_json::from_str(&j).ok());
    // A row whose song no longer parses (a `SongItem` shape change) is asked again.
    if tier != Tier::Missing && pick.is_none() {
        return None;
    }
    let candidates =
        m.candidates_json.and_then(|j| serde_json::from_str(&j).ok()).unwrap_or_default();
    Some((tier, pick, candidates))
}

fn remember(state: &AppState, key: &str, (tier, pick, candidates): &Answer, manual: bool) {
    let song_json = pick.as_ref().and_then(|p| serde_json::to_string(p).ok());
    let candidates_json = (*tier != Tier::Matched && !candidates.is_empty())
        .then(|| serde_json::to_string(candidates).ok())
        .flatten();
    state.db.put_import_match(
        key,
        &ImportMatch {
            video_id: pick.as_ref().map(|p| p.video_id.clone()),
            song_json,
            tier: tier.as_str().to_owned(),
            candidates_json,
            manual,
            updated_at: now_secs(),
        },
    );
}

// --- the job -------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Matching,
    Review,
    Creating,
    Done,
    Failed,
    Cancelled,
}

struct Row {
    key: String,
    track: SourceTrack,
    tier: Tier,
    pick: Option<SongItem>,
    candidates: Vec<SongItem>,
}

/// One list being imported, its tracks as row keys in the source's order.
struct Picked {
    kind: ListKind,
    name: String,
    cover: Option<String>,
    url: Option<String>,
    keys: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResult {
    kind: ListKind,
    name: String,
    /// The browse id the UI opens: `VL…` on the account, `LOCALPLAYLIST:<n>` on this machine.
    id: String,
    local: bool,
    added: usize,
    missing: usize,
    removed: usize,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Recent {
    title: String,
    artists: String,
    tier: Tier,
    thumbnail: Option<String>,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Extras {
    liked: usize,
    followed: usize,
    saved: usize,
}

struct Job {
    gen: u64,
    phase: Phase,
    lists: Vec<Picked>,
    rows: Vec<Row>,
    index: HashMap<String, usize>,
    artists: Vec<String>,
    albums: Vec<SavedAlbum>,
    /// An "Update from Spotify" of this playlist: no review, and the writes are a diff.
    update_of: Option<String>,
    step: (usize, usize),
    message: Option<String>,
    results: Vec<ListResult>,
    extras: Extras,
    recent: VecDeque<Recent>,
    last_emit: Option<Instant>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBrief {
    kind: ListKind,
    name: String,
    count: usize,
    cover: Option<String>,
}

/// Everything the UI draws from, small enough to send four times a second.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    phase: Phase,
    total: usize,
    done: usize,
    matched: usize,
    check: usize,
    missing: usize,
    lists: Vec<ListBrief>,
    artists: usize,
    albums: usize,
    recent: Vec<Recent>,
    step: [usize; 2],
    message: Option<String>,
    results: Vec<ListResult>,
    extras: Extras,
    update: Option<String>,
}

impl Job {
    fn new(gen: u64, lists: Vec<SourceList>, artists: Vec<String>, albums: Vec<SavedAlbum>) -> Job {
        let mut rows: Vec<Row> = Vec::new();
        let mut index = HashMap::new();
        let lists = lists
            .into_iter()
            .map(|l| {
                let mut keys = Vec::new();
                let mut seen = HashSet::new();
                for track in l.tracks {
                    let k = key(&track);
                    // Twice in one playlist goes in once: YouTube refuses the second copy anyway.
                    if !seen.insert(k.clone()) {
                        continue;
                    }
                    index.entry(k.clone()).or_insert_with(|| {
                        rows.push(Row {
                            key: k.clone(),
                            track,
                            tier: Tier::Pending,
                            pick: None,
                            candidates: Vec::new(),
                        });
                        rows.len() - 1
                    });
                    keys.push(k);
                }
                Picked { kind: l.kind, name: l.name, cover: l.cover, url: l.url, keys }
            })
            .collect();
        Job {
            gen,
            phase: Phase::Matching,
            lists,
            rows,
            index,
            artists,
            albums,
            update_of: None,
            step: (0, 0),
            message: None,
            results: Vec::new(),
            extras: Extras::default(),
            recent: VecDeque::new(),
            last_emit: None,
        }
    }

    fn resolve(&mut self, i: usize, (tier, pick, candidates): Answer) {
        let row = &mut self.rows[i];
        self.recent.push_front(Recent {
            title: row.track.title.clone(),
            artists: row.track.artists.join(", "),
            tier,
            thumbnail: pick.as_ref().and_then(|p| p.thumbnail.clone()),
        });
        self.recent.truncate(5);
        row.tier = tier;
        row.pick = pick;
        row.candidates = candidates;
    }

    fn snapshot(&self) -> Snapshot {
        let count = |t: Tier| self.rows.iter().filter(|r| r.tier == t).count();
        let pending = count(Tier::Pending);
        Snapshot {
            phase: self.phase,
            total: self.rows.len(),
            done: self.rows.len() - pending,
            matched: count(Tier::Matched),
            check: count(Tier::Check),
            missing: count(Tier::Missing),
            lists: self
                .lists
                .iter()
                .map(|l| ListBrief {
                    kind: l.kind,
                    name: l.name.clone(),
                    count: l.keys.len(),
                    cover: l.cover.clone(),
                })
                .collect(),
            artists: self.artists.len(),
            albums: self.albums.len(),
            recent: self.recent.iter().cloned().collect(),
            step: [self.step.0, self.step.1],
            message: self.message.clone(),
            results: self.results.clone(),
            extras: self.extras.clone(),
            update: self.update_of.clone(),
        }
    }

    fn running(&self) -> bool {
        matches!(self.phase, Phase::Matching | Phase::Creating)
    }
}

struct Importer {
    /// The last source read, kept until the next one so a failed import can be started again.
    pending: Option<spotify::Library>,
    job: Option<Job>,
    gen: u64,
}

// ponytail: one import per process, in a static rather than an AppState field. There is one
// window and one dialog; a second concurrent import is refused (`busy`), not queued.
static IMPORT: Mutex<Importer> = Mutex::new(Importer { pending: None, job: None, gen: 0 });

/// Run `f` on the job if it is still generation `gen` and still running (not cancelled, not
/// replaced by a newer import).
fn with_job<R>(gen: u64, f: impl FnOnce(&mut Job) -> R) -> Option<R> {
    let mut g = IMPORT.lock().unwrap();
    g.job.as_mut().filter(|j| j.gen == gen && j.running()).map(f)
}

fn alive(gen: u64) -> bool {
    with_job(gen, |_| ()).is_some()
}

fn emit(state: &AppState, gen: u64, force: bool) {
    let snapshot = {
        let mut g = IMPORT.lock().unwrap();
        let Some(j) = g.job.as_mut().filter(|j| j.gen == gen) else {
            return;
        };
        if !force && j.last_emit.is_some_and(|t| t.elapsed() < Duration::from_millis(250)) {
            return;
        }
        j.last_emit = Some(Instant::now());
        j.snapshot()
    };
    let _ = state.app.emit("import-progress", snapshot);
}

fn finish(state: &AppState, gen: u64, phase: Phase, message: Option<String>) {
    if with_job(gen, |j| {
        j.phase = phase;
        j.message = message;
    })
    .is_some()
    {
        emit(state, gen, true);
    }
}

// --- reading -------------------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPreview {
    kind: ListKind,
    name: String,
    owner: Option<String>,
    cover: Option<String>,
    count: usize,
    skipped: usize,
    truncated: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preview {
    lists: Vec<ListPreview>,
    artists: usize,
    albums: usize,
}

fn keep(lib: spotify::Library) -> Preview {
    let preview = Preview {
        lists: lib
            .lists
            .iter()
            .map(|l| ListPreview {
                kind: l.kind,
                name: l.name.clone(),
                owner: l.owner.clone(),
                cover: l.cover.clone(),
                count: l.tracks.len(),
                skipped: l.skipped,
                truncated: l.truncated,
            })
            .collect(),
        artists: lib.artists.len(),
        albums: lib.albums.len(),
    };
    IMPORT.lock().unwrap().pending = Some(lib);
    preview
}

pub async fn read_link(link: &str) -> Result<Preview, String> {
    let (kind, id) = spotify::parse_link(link).ok_or("not_spotify")?;
    let list = spotify::read_link(kind, &id).await?;
    Ok(keep(spotify::Library { lists: vec![list], ..Default::default() }))
}

pub fn read_file(bytes: &[u8], name: &str) -> Result<Preview, String> {
    Ok(keep(spotify::read_file(bytes, name)?))
}

// --- matching phase ------------------------------------------------------------------------------

/// Start matching the picked lists (indices into the last [`Preview`]).
pub fn start(state: &Arc<AppState>, picked: Vec<usize>) -> Result<Snapshot, String> {
    let mut g = IMPORT.lock().unwrap();
    if g.job.as_ref().is_some_and(Job::running) {
        return Err("busy".into());
    }
    let lib = g.pending.as_ref().ok_or("nothing_read")?;
    let lists: Vec<SourceList> = picked.iter().filter_map(|&i| lib.lists.get(i).cloned()).collect();
    let job = Job::new(g.gen + 1, lists, lib.artists.clone(), lib.albums.clone());
    g.gen += 1;
    let gen = g.gen;
    let snapshot = job.snapshot();
    g.job = Some(job);
    drop(g);
    tauri::async_runtime::spawn(run_matching(Arc::clone(state), gen));
    Ok(snapshot)
}

async fn run_matching(state: Arc<AppState>, gen: u64) {
    let todo: Vec<(usize, String, SourceTrack)> = with_job(gen, |j| {
        j.rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.tier == Tier::Pending)
            .map(|(i, r)| (i, r.key.clone(), r.track.clone()))
            .collect()
    })
    .unwrap_or_default();

    // Everything already known first, so a re-import fills in at once.
    let mut network = Vec::new();
    for (i, key, track) in todo {
        match cached(&state, &key) {
            Some(answer) => {
                with_job(gen, |j| j.resolve(i, answer));
            }
            None => network.push((i, key, track)),
        }
    }
    emit(&state, gen, true);

    let mut failures = 0;
    for (n, (i, key, track)) in network.into_iter().enumerate() {
        if !alive(gen) {
            return;
        }
        if n > 0 {
            tokio::time::sleep(pace()).await;
        }
        match search(&state, &track).await {
            Ok(ranked) => {
                failures = 0;
                let answer = classify(ranked);
                remember(&state, &key, &answer, false);
                with_job(gen, |j| j.resolve(i, answer));
            }
            Err(e) => {
                tracing::warn!(error = %e, "import: search failed");
                failures += 1;
                if throttled(&e) {
                    return finish(&state, gen, Phase::Failed, Some("rate_limited".into()));
                }
                if failures >= 3 {
                    return finish(&state, gen, Phase::Failed, Some(e.to_string()));
                }
                // Not remembered: a failed search is not an answer.
                with_job(gen, |j| j.resolve(i, (Tier::Missing, None, Vec::new())));
            }
        }
        emit(&state, gen, false);
    }

    let update = with_job(gen, |j| j.update_of.clone()).flatten();
    match update {
        Some(playlist_id) => {
            with_job(gen, |j| j.phase = Phase::Creating);
            emit(&state, gen, true);
            run_update(state, gen, playlist_id).await;
        }
        None => {
            // Review isn't "running", so this is written directly rather than through `with_job`.
            let mut g = IMPORT.lock().unwrap();
            if let Some(j) = g.job.as_mut().filter(|j| j.gen == gen && j.running()) {
                j.phase = Phase::Review;
            }
            drop(g);
            emit(&state, gen, true);
        }
    }
}

// --- review --------------------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewRow {
    key: String,
    title: String,
    artists: String,
    album: Option<String>,
    duration_ms: Option<u64>,
    tier: Tier,
    pick: Option<SongItem>,
    /// Left empty for matched rows: there can be thousands, and they rarely need a second look.
    candidates: Vec<SongItem>,
}

pub fn rows(tier: Tier) -> Vec<ReviewRow> {
    let g = IMPORT.lock().unwrap();
    let Some(j) = g.job.as_ref() else {
        return Vec::new();
    };
    j.rows
        .iter()
        .filter(|r| r.tier == tier)
        .map(|r| ReviewRow {
            key: r.key.clone(),
            title: r.track.title.clone(),
            artists: r.track.artists.join(", "),
            album: r.track.album.clone(),
            duration_ms: r.track.duration_ms,
            tier: r.tier,
            pick: r.pick.clone(),
            candidates: if tier == Tier::Matched { Vec::new() } else { r.candidates.clone() },
        })
        .collect()
}

/// The user's call on one row: a song (theirs from now on, in every later import too) or `None`
/// to leave the track out.
pub fn pick(state: &AppState, key: &str, song: Option<SongItem>) -> Result<Snapshot, String> {
    let mut g = IMPORT.lock().unwrap();
    let j = g.job.as_mut().filter(|j| j.phase == Phase::Review).ok_or("gone")?;
    let i = *j.index.get(key).ok_or("gone")?;
    let row = &mut j.rows[i];
    row.tier = if song.is_some() { Tier::Matched } else { Tier::Missing };
    row.pick = song;
    if row.pick.is_some() {
        let answer = (row.tier, row.pick.clone(), row.candidates.clone());
        remember(state, key, &answer, true);
    }
    Ok(j.snapshot())
}

pub fn status() -> Option<Snapshot> {
    IMPORT.lock().unwrap().job.as_ref().map(Job::snapshot)
}

/// Stop a running import (what was written stays written), or put away a finished one.
pub fn cancel(state: &AppState) {
    let mut g = IMPORT.lock().unwrap();
    let gen = match g.job.as_mut() {
        Some(j) if j.running() => {
            j.phase = Phase::Cancelled;
            j.gen
        }
        _ => {
            g.job = None;
            return;
        }
    };
    drop(g);
    emit(state, gen, true);
}

// --- creating ------------------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateOptions {
    /// A new name per list (its index in the job), when the user changed one. The UI also sends
    /// one for Liked Songs, whose name Rust doesn't know in the user's language.
    names: HashMap<usize, String>,
    /// On this machine rather than the account. Forced when signed out.
    local: bool,
    /// Like the Liked Songs tracks on YouTube Music.
    like: bool,
    /// Subscribe to the followed artists.
    follow: bool,
    /// Save the saved albums.
    save_albums: bool,
}

pub fn create(state: &Arc<AppState>, opts: CreateOptions) -> Result<(), String> {
    let gen = {
        let mut g = IMPORT.lock().unwrap();
        let j = g.job.as_mut().filter(|j| j.phase == Phase::Review).ok_or("gone")?;
        j.phase = Phase::Creating;
        j.gen
    };
    tauri::async_runtime::spawn(run_create(Arc::clone(state), gen, opts));
    Ok(())
}

struct Plan {
    kind: ListKind,
    name: String,
    cover: Option<String>,
    url: Option<String>,
    keys: Vec<String>,
    songs: Vec<SongItem>,
    missing: usize,
}

/// The songs a list becomes, in order, without a video twice (two Spotify versions of one song
/// can land on the same upload), and how many of its tracks have none.
fn songs_for(j: &Job, keys: &[String]) -> (Vec<SongItem>, usize) {
    let mut seen = HashSet::new();
    let mut songs = Vec::new();
    let mut missing = 0;
    for k in keys {
        match j.index.get(k).map(|&i| &j.rows[i]) {
            Some(Row { tier: Tier::Matched | Tier::Check, pick: Some(p), .. }) => {
                if seen.insert(p.video_id.clone()) {
                    songs.push(p.clone());
                }
            }
            _ => missing += 1,
        }
    }
    (songs, missing)
}

async fn run_create(state: Arc<AppState>, gen: u64, opts: CreateOptions) {
    let signed_in = state.it.is_logged_in();
    let local = opts.local || !signed_in;
    let plan = with_job(gen, |j| {
        let mut plan = Vec::new();
        for (i, l) in j.lists.iter().enumerate() {
            let (songs, missing) = songs_for(j, &l.keys);
            let name = opts
                .names
                .get(&i)
                .map(|n| n.trim())
                .filter(|n| !n.is_empty())
                .unwrap_or(&l.name)
                .to_owned();
            let parts: Vec<Vec<SongItem>> = if local || songs.len() <= YTM_PLAYLIST_MAX {
                vec![songs]
            } else {
                songs.chunks(YTM_PLAYLIST_MAX).map(<[SongItem]>::to_vec).collect()
            };
            for (n, songs) in parts.into_iter().enumerate() {
                plan.push(Plan {
                    kind: l.kind,
                    name: if n == 0 { name.clone() } else { format!("{name} ({})", n + 1) },
                    cover: l.cover.clone(),
                    // A split list can't be updated as one, so it isn't offered.
                    url: l
                        .url
                        .clone()
                        .filter(|_| n == 0 && missing + songs.len() <= YTM_PLAYLIST_MAX),
                    keys: l.keys.clone(),
                    songs,
                    missing: if n == 0 { missing } else { 0 },
                });
            }
        }
        let liked: Vec<String> = if opts.like && signed_in {
            plan.iter()
                .filter(|p| p.kind == ListKind::Liked)
                .flat_map(|p| p.songs.iter().map(|s| s.video_id.clone()))
                .collect()
        } else {
            Vec::new()
        };
        let artists = if opts.follow && signed_in { j.artists.clone() } else { Vec::new() };
        let albums = if opts.save_albums && signed_in { j.albums.clone() } else { Vec::new() };
        j.step = (0, plan.len() + liked.len() + artists.len() + albums.len());
        (plan, liked, artists, albums)
    });
    let Some((plan, liked, artists, albums)) = plan else {
        return;
    };
    emit(&state, gen, true);

    for p in plan {
        if !alive(gen) {
            return;
        }
        if !p.songs.is_empty() {
            match make_playlist(&state, &p.name, &p.songs, local).await {
                Ok((id, added)) => {
                    if let Some(url) = &p.cover {
                        set_cover(&state, &id, url).await;
                    }
                    if let Some(url) = &p.url {
                        save_source(&state, &id, url, &p.keys);
                    }
                    let result = ListResult {
                        kind: p.kind,
                        name: p.name,
                        id,
                        local,
                        added,
                        missing: p.missing + p.songs.len() - added,
                        removed: 0,
                    };
                    with_job(gen, |j| j.results.push(result));
                }
                Err(e) => return finish(&state, gen, Phase::Failed, Some(e)),
            }
        }
        with_job(gen, |j| j.step.0 += 1);
        emit(&state, gen, true);
    }

    let client = metadata_client(&state).ok();
    for video_id in liked {
        if !alive(gen) {
            return;
        }
        let Some(client) = client else { break };
        if state.it.rate(client, &video_id, Rating::Like).await.is_ok() {
            with_job(gen, |j| j.extras.liked += 1);
        }
        with_job(gen, |j| j.step.0 += 1);
        emit(&state, gen, false);
        tokio::time::sleep(pace()).await;
    }
    for name in artists {
        if !alive(gen) {
            return;
        }
        if follow_artist(&state, &name).await {
            with_job(gen, |j| j.extras.followed += 1);
        }
        with_job(gen, |j| j.step.0 += 1);
        emit(&state, gen, false);
        tokio::time::sleep(pace()).await;
    }
    for album in albums {
        if !alive(gen) {
            return;
        }
        if save_album(&state, &album).await {
            with_job(gen, |j| j.extras.saved += 1);
        }
        with_job(gen, |j| j.step.0 += 1);
        emit(&state, gen, false);
        tokio::time::sleep(pace()).await;
    }
    finish(&state, gen, Phase::Done, None);
}

/// Add `ids` to an account playlist in batches, answering the ones that went in. YouTube applies a
/// batch whole or not at all, so a refused one is retried a track at a time: a video it won't take
/// (taken down, blocked in the region) then costs only itself. An error only when nothing went in.
async fn add_all(
    state: &AppState,
    playlist_id: &str,
    ids: &[String],
) -> Result<Vec<String>, String> {
    let client = metadata_client(state)?;
    let mut added = Vec::new();
    let mut last_err = None;
    for (n, chunk) in ids.chunks(100).enumerate() {
        if n > 0 {
            tokio::time::sleep(pace()).await;
        }
        let Err(e) = state.it.playlist_add_many(client, playlist_id, chunk).await else {
            added.extend_from_slice(chunk);
            continue;
        };
        tracing::warn!(error = %e, "import: batch refused, adding one at a time");
        for v in chunk {
            tokio::time::sleep(pace()).await;
            match state.it.playlist_add(client, playlist_id, v, false).await {
                Ok(true) => added.push(v.clone()),
                Ok(false) => {}
                // No point asking a hundred more times.
                Err(e @ innertube::Error::SessionExpired) => return Err(e.to_string()),
                Err(e) => last_err = Some(e.to_string()),
            }
        }
    }
    match last_err {
        Some(e) if added.is_empty() && !ids.is_empty() => Err(e),
        _ => Ok(added),
    }
}

/// Create the playlist and fill it. Answers the browse id the UI opens and how many went in.
async fn make_playlist(
    state: &Arc<AppState>,
    name: &str,
    songs: &[SongItem],
    local: bool,
) -> Result<(String, usize), String> {
    if local {
        let id = state.db.create_local_playlist(name, now_secs()).map_err(|e| e.to_string())?;
        let rows = songs
            .iter()
            .map(|s| {
                let s = crate::commands::playlist_row(s.clone());
                serde_json::to_string(&s).map(|json| (s.video_id, json))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        state.db.add_local_playlist_tracks(id, &rows, now_secs()).map_err(|e| e.to_string())?;
        return Ok((format!("{LOCAL_PLAYLIST_PREFIX}{id}"), rows.len()));
    }
    let client = metadata_client(state)?;
    let id = state.it.create_playlist(client, name).await.map_err(|e| e.to_string())?;
    let browse_id = format!("VL{id}");
    let ids: Vec<String> = songs.iter().map(|s| s.video_id.clone()).collect();
    let added = add_all(state, &browse_id, &ids).await?;
    state.db.set_playlist_tracks(&browse_id, &added);
    Ok((browse_id, added.len()))
}

/// Carry the Spotify cover over. Best effort: a playlist without it is still the playlist.
async fn set_cover(state: &Arc<AppState>, playlist_id: &str, url: &str) {
    let resp = crate::http::client().get(url).timeout(Duration::from_secs(20)).send().await;
    let Ok(bytes) = (match resp {
        Ok(r) => r.bytes().await,
        Err(e) => Err(e),
    }) else {
        return;
    };
    // Spotify's image CDN serves JPEG; anything else YouTube's uploader would refuse anyway.
    let ext = if bytes.starts_with(&[0xFF, 0xD8]) {
        "jpg"
    } else if bytes.starts_with(b"\x89PNG") {
        "png"
    } else {
        return;
    };
    let tmp = std::env::temp_dir().join(format!("limusic-cover-{}.{ext}", rand::random::<u32>()));
    if std::fs::write(&tmp, &bytes).is_ok() {
        if let Err(e) = crate::commands::store_cover(&state.app, state, playlist_id, &tmp) {
            tracing::warn!(error = %e, "import: cover not set");
        }
    }
    let _ = std::fs::remove_file(&tmp);
}

async fn follow_artist(state: &AppState, name: &str) -> bool {
    let Ok(client) = metadata_client(state) else { return false };
    let Ok(cards) = state.it.search_cards(client, name, "artists").await else { return false };
    let want = norm(name);
    let Some(card) = cards.into_iter().find(|c| norm(&c.title) == want) else { return false };
    state.it.subscribe(client, &card.id, true).await.is_ok()
}

fn album_score(want: &SavedAlbum, card: &BrowseItem) -> f64 {
    let title = title_score(&strip_dressing(&want.title).0, &strip_dressing(&card.title).0);
    let artist = norm(&want.artist);
    let by = !artist.is_empty()
        && format!(" {} ", norm(card.subtitle.as_deref().unwrap_or_default()))
            .contains(&format!(" {artist} "));
    0.7 * title + if by { 0.3 } else { 0.0 }
}

async fn find_album(state: &AppState, want: &SavedAlbum) -> Option<BrowseItem> {
    let client = metadata_client(state).ok()?;
    let q = format!("{} {}", want.title, want.artist);
    let cards = state.it.search_cards(client, q.trim(), "albums").await.ok()?;
    cards
        .into_iter()
        .map(|c| (album_score(want, &c), c))
        .filter(|(s, _)| *s >= 0.75)
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, c)| c)
}

async fn save_album(state: &AppState, want: &SavedAlbum) -> bool {
    let Some(card) = find_album(state, want).await else { return false };
    let Ok(client) = metadata_client(state) else { return false };
    let Ok(page) = state.it.album(client, &card.id).await else { return false };
    let Some(playlist_id) = page.playlist_id else { return false };
    state.it.like_playlist(client, &playlist_id, true).await.is_ok()
}

// --- "Update from Spotify" -----------------------------------------------------------------------

/// What a playlist imported from a link remembers: the link, and the Spotify tracks it held, so an
/// update knows what is new and what left.
#[derive(Serialize, Deserialize)]
struct Source {
    url: String,
    keys: Vec<String>,
}

fn source_key(playlist_id: &str) -> String {
    format!("spotify_source:{}", playlist_id.strip_prefix("VL").unwrap_or(playlist_id))
}

fn save_source(state: &AppState, playlist_id: &str, url: &str, keys: &[String]) {
    if let Ok(json) = serde_json::to_string(&Source { url: url.to_owned(), keys: keys.to_vec() }) {
        state.db.set_setting(&source_key(playlist_id), &json);
    }
}

fn source(state: &AppState, playlist_id: &str) -> Option<Source> {
    serde_json::from_str(&state.db.get_setting(&source_key(playlist_id))?).ok()
}

/// The Spotify link a playlist was imported from, for its "Update from Spotify" banner.
pub fn source_url(state: &AppState, playlist_id: &str) -> Option<String> {
    source(state, playlist_id).map(|s| s.url)
}

pub async fn update(state: &Arc<AppState>, playlist_id: String) -> Result<Snapshot, String> {
    if IMPORT.lock().unwrap().job.as_ref().is_some_and(Job::running) {
        return Err("busy".into());
    }
    let src = source(state, &playlist_id).ok_or("gone")?;
    let (kind, id) = spotify::parse_link(&src.url).ok_or("not_spotify")?;
    let list = spotify::read_link(kind, &id).await?;
    let mut g = IMPORT.lock().unwrap();
    if g.job.as_ref().is_some_and(Job::running) {
        return Err("busy".into());
    }
    g.gen += 1;
    let mut job = Job::new(g.gen, vec![list], Vec::new(), Vec::new());
    job.update_of = Some(playlist_id);
    let gen = g.gen;
    let snapshot = job.snapshot();
    g.job = Some(job);
    drop(g);
    tauri::async_runtime::spawn(run_matching(Arc::clone(state), gen));
    Ok(snapshot)
}

/// What the playlist holds right now: each track's video id and its row handle (the
/// `set_video_id` a removal needs; for a playlist on this machine, the row id).
async fn current_rows(
    state: &AppState,
    playlist_id: &str,
) -> Result<Vec<(String, String)>, String> {
    if is_local_playlist(playlist_id) {
        let key: i64 = playlist_id
            .strip_prefix(LOCAL_PLAYLIST_PREFIX)
            .and_then(|n| n.parse().ok())
            .ok_or("gone")?;
        return Ok(state
            .db
            .local_playlist_tracks(key)
            .into_iter()
            .filter_map(|(row, json)| {
                let s: SongItem = serde_json::from_str(&json).ok()?;
                Some((s.video_id, row.to_string()))
            })
            .collect());
    }
    let client = metadata_client(state)?;
    let page = state.it.playlist(client, playlist_id, None).await.map_err(|e| e.to_string())?;
    let mut out: Vec<(String, String)> = Vec::new();
    let mut push = |items: Vec<SongItem>| {
        out.extend(items.into_iter().map(|s| (s.video_id, s.set_video_id.unwrap_or_default())))
    };
    push(page.items);
    let mut token = page.continuation;
    while let Some(next) = token.take() {
        let more =
            state.it.playlist_continuation(client, &next).await.map_err(|e| e.to_string())?;
        push(more.items);
        token = more.continuation;
    }
    Ok(out)
}

/// Apply an update: append what is new on Spotify, take out what left it. Spotify reordering
/// its playlist is not mirrored, and neither is anything the user added here by hand.
async fn run_update(state: Arc<AppState>, gen: u64, playlist_id: String) {
    let Some(old) = source(&state, &playlist_id) else {
        return finish(&state, gen, Phase::Failed, Some("gone".into()));
    };
    let Some((name, new_keys, songs, missing)) = with_job(gen, |j| {
        let l = &j.lists[0];
        let (songs, missing) = songs_for(j, &l.keys);
        (l.name.clone(), l.keys.clone(), songs, missing)
    }) else {
        return;
    };
    let current = match current_rows(&state, &playlist_id).await {
        Ok(rows) => rows,
        Err(e) => return finish(&state, gen, Phase::Failed, Some(e)),
    };
    let have: HashSet<&str> = current.iter().map(|(v, _)| v.as_str()).collect();
    let old_keys: HashSet<&str> = old.keys.iter().map(String::as_str).collect();
    let kept: HashSet<&str> = new_keys.iter().map(String::as_str).collect();

    // New on Spotify and not here already, in Spotify's order.
    let add: Vec<SongItem> = with_job(gen, |j| {
        let mut seen = HashSet::new();
        new_keys
            .iter()
            .filter(|k| !old_keys.contains(k.as_str()))
            .filter_map(|k| j.index.get(k).map(|&i| &j.rows[i]))
            .filter(|r| matches!(r.tier, Tier::Matched | Tier::Check))
            .filter_map(|r| r.pick.clone())
            .filter(|p| !have.contains(p.video_id.as_str()) && seen.insert(p.video_id.clone()))
            .collect()
    })
    .unwrap_or_default();

    // Gone from Spotify: their videos, unless a track still there maps to the same one.
    let still: HashSet<&str> = songs.iter().map(|s| s.video_id.as_str()).collect();
    let gone_videos: HashSet<String> = old
        .keys
        .iter()
        .filter(|k| !kept.contains(k.as_str()))
        .filter_map(|k| state.db.get_import_match(k)?.video_id)
        .filter(|v| !still.contains(v.as_str()))
        .collect();
    let remove: Vec<(String, String)> =
        current.iter().filter(|(v, _)| gone_videos.contains(v)).cloned().collect();

    let applied = if is_local_playlist(&playlist_id) {
        apply_local(&state, &playlist_id, &add, &remove)
    } else {
        apply_account(&state, &playlist_id, &add, &remove).await
    };
    let added = match applied {
        Ok(n) => n,
        Err(e) => return finish(&state, gen, Phase::Failed, Some(e)),
    };
    save_source(&state, &playlist_id, &old.url, &new_keys);
    let result = ListResult {
        kind: ListKind::Playlist,
        name,
        id: playlist_id.clone(),
        local: is_local_playlist(&playlist_id),
        added,
        missing,
        removed: remove.len(),
    };
    with_job(gen, |j| j.results.push(result));
    finish(&state, gen, Phase::Done, None);
}

fn apply_local(
    state: &AppState,
    playlist_id: &str,
    add: &[SongItem],
    remove: &[(String, String)],
) -> Result<usize, String> {
    let key: i64 = playlist_id
        .strip_prefix(LOCAL_PLAYLIST_PREFIX)
        .and_then(|n| n.parse().ok())
        .ok_or("gone")?;
    let rows: Vec<i64> = remove.iter().filter_map(|(_, r)| r.parse().ok()).collect();
    if !rows.is_empty() {
        state.db.remove_local_playlist_tracks(key, &rows, now_secs()).map_err(|e| e.to_string())?;
    }
    let add = add
        .iter()
        .map(|s| {
            let s = crate::commands::playlist_row(s.clone());
            serde_json::to_string(&s).map(|json| (s.video_id, json))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if add.is_empty() {
        return Ok(0);
    }
    let went_in =
        state.db.add_local_playlist_tracks(key, &add, now_secs()).map_err(|e| e.to_string())?;
    Ok(went_in.into_iter().filter(|&b| b).count())
}

async fn apply_account(
    state: &AppState,
    playlist_id: &str,
    add: &[SongItem],
    remove: &[(String, String)],
) -> Result<usize, String> {
    let client = metadata_client(state)?;
    // A row without its handle can't be removed, and one bad entry would sink the whole batch.
    let remove: Vec<(String, String)> =
        remove.iter().filter(|(_, h)| !h.is_empty()).cloned().collect();
    if !remove.is_empty() {
        state
            .it
            .playlist_remove_many(client, playlist_id, &remove)
            .await
            .map_err(|e| e.to_string())?;
        for (v, _) in &remove {
            state.db.remove_playlist_track(playlist_id, v);
        }
    }
    let ids: Vec<String> = add.iter().map(|s| s.video_id.clone()).collect();
    let added = add_all(state, playlist_id, &ids).await?;
    for v in &added {
        state.db.add_playlist_track(playlist_id, v);
    }
    Ok(added.len())
}

// --- Spotify links anywhere ----------------------------------------------------------------------

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Resolved {
    Song {
        song: Box<SongItem>,
    },
    Album {
        id: String,
    },
    Artist {
        id: String,
    },
    /// A playlist is imported, not opened: the UI takes it to the import dialog.
    Playlist,
}

/// What a pasted Spotify link is on YouTube Music: the song a track link plays, the album or
/// artist page an album or artist link opens.
pub async fn resolve(state: &AppState, link: &str) -> Result<Resolved, String> {
    let (kind, id) = spotify::parse_link(link).ok_or("not_spotify")?;
    match kind {
        LinkKind::Playlist => Ok(Resolved::Playlist),
        LinkKind::Track => {
            let track = spotify::read_track(&id).await?;
            let k = key(&track);
            let answer = match cached(state, &k) {
                Some(a) => a,
                None => {
                    let a = classify(search(state, &track).await.map_err(|e| e.to_string())?);
                    remember(state, &k, &a, false);
                    a
                }
            };
            answer
                .1
                .map(|song| Resolved::Song { song: Box::new(song) })
                .ok_or_else(|| "not_found".into())
        }
        LinkKind::Album => {
            let (title, artist) = spotify::read_name(kind, &id).await?;
            let want = SavedAlbum { title, artist: artist.unwrap_or_default() };
            let card = find_album(state, &want).await.ok_or("not_found")?;
            Ok(Resolved::Album { id: card.id })
        }
        LinkKind::Artist => {
            let (name, _) = spotify::read_name(kind, &id).await?;
            let client = metadata_client(state)?;
            let cards =
                state.it.search_cards(client, &name, "artists").await.map_err(|e| e.to_string())?;
            let want = norm(&name);
            let card = cards
                .iter()
                .find(|c| norm(&c.title) == want)
                .or_else(|| cards.first())
                .ok_or("not_found")?;
            Ok(Resolved::Artist { id: card.id.clone() })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(title: &str, artists: &[&str], album: Option<&str>, secs: Option<u64>) -> SourceTrack {
        SourceTrack {
            id: None,
            title: title.into(),
            artists: artists.iter().map(|a| a.to_string()).collect(),
            album: album.map(Into::into),
            duration_ms: secs.map(|s| s * 1000),
            explicit: None,
        }
    }

    fn song(id: &str, title: &str, artists: &str, album: Option<&str>, duration: &str) -> SongItem {
        serde_json::from_value(serde_json::json!({
            "video_id": id, "title": title, "artists": artists, "album": album, "duration": duration,
        }))
        .unwrap()
    }

    /// The candidate `src` would pick out of `cands`, and its tier.
    fn best(src: &SourceTrack, cands: Vec<SongItem>) -> (Tier, Option<String>) {
        let mut ranked = Vec::new();
        rank(src, cands, &mut ranked);
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        let (tier, pick, _) = classify(ranked);
        (tier, pick.map(|p| p.video_id))
    }

    #[test]
    fn normalizing() {
        assert_eq!(norm("Beyoncé \u{2013} Don't Stop (Me Now)!"), "beyonce dont stop me now");
        assert_eq!(norm("Simon & Garfunkel"), "simon and garfunkel");
        assert_eq!(norm("夜に駆ける"), "夜に駆ける");
        assert_eq!(strip_dressing("Panama - 2015 Remaster").0, "Panama");
        assert_eq!(strip_dressing("Get Lucky (feat. Pharrell Williams)").0, "Get Lucky");
        assert_eq!(strip_dressing("Mask Off ft. Kendrick Lamar").0, "Mask Off");
        assert_eq!(strip_dressing("(Intro)").0, "(Intro)");
        assert_eq!(versions(&strip_dressing("Creep - Acoustic").1), ["acoustic"]);
        assert_eq!(versions(&strip_dressing("Love Story (Taylor's Version)").1), ["taylors"]);
        assert!(versions(&strip_dressing("Jump - 2015 Remaster").1).is_empty());
        assert_eq!(
            query(&src("Creep - Acoustic", &["Radiohead"], None, None)),
            "Creep acoustic Radiohead"
        );
    }

    #[test]
    fn remaster_matches_the_plain_upload() {
        let s = src("Panama - 2015 Remaster", &["Van Halen"], Some("1984 (Remastered)"), Some(210));
        assert_eq!(
            best(&s, vec![song("a", "Panama", "Van Halen", Some("1984"), "3:31")]),
            (Tier::Matched, Some("a".into()))
        );
    }

    #[test]
    fn live_loses_to_studio() {
        let s = src("Creep", &["Radiohead"], Some("Pablo Honey"), Some(238));
        let cands = vec![
            song("live", "Creep (Live)", "Radiohead", None, "4:20"),
            song("studio", "Creep", "Radiohead", Some("Pablo Honey"), "3:59"),
        ];
        assert_eq!(best(&s, cands), (Tier::Matched, Some("studio".into())));
    }

    #[test]
    fn cover_by_someone_else_is_not_a_match() {
        let s = src("Hallelujah", &["Jeff Buckley"], Some("Grace"), Some(414));
        let (tier, _) = best(&s, vec![song("c", "Hallelujah", "Pentatonix", None, "4:29")]);
        assert_ne!(tier, Tier::Matched);
    }

    #[test]
    fn featured_artist_in_the_title() {
        let s =
            src("Get Lucky", &["Daft Punk", "Pharrell Williams", "Nile Rodgers"], None, Some(369));
        let cands = vec![song(
            "g",
            "Get Lucky (feat. Pharrell Williams & Nile Rodgers)",
            "Daft Punk",
            Some("Random Access Memories"),
            "6:09",
        )];
        assert_eq!(best(&s, cands), (Tier::Matched, Some("g".into())));
    }

    #[test]
    fn wrong_length_needs_a_look() {
        // Right name, right artist, a minute too long: an extended mix or a video with an intro.
        let s = src("Blinding Lights", &["The Weeknd"], None, Some(200));
        let (tier, _) = best(&s, vec![song("x", "Blinding Lights", "The Weeknd", None, "4:22")]);
        assert_eq!(tier, Tier::Check);
    }

    #[test]
    fn explicit_breaks_the_tie() {
        let mut s = src("HUMBLE.", &["Kendrick Lamar"], None, Some(177));
        s.explicit = Some(true);
        let mut clean = song("clean", "HUMBLE.", "Kendrick Lamar", None, "2:57");
        clean.explicit = false;
        let mut dirty = song("dirty", "HUMBLE.", "Kendrick Lamar", None, "2:57");
        dirty.explicit = true;
        assert_eq!(best(&s, vec![clean, dirty]).1.as_deref(), Some("dirty"));
    }

    #[test]
    fn non_latin_titles() {
        let s = src("夜に駆ける", &["YOASOBI"], None, Some(261));
        assert_eq!(
            best(&s, vec![song("y", "夜に駆ける", "YOASOBI", None, "4:21")]),
            (Tier::Matched, Some("y".into()))
        );
    }

    #[test]
    fn nothing_close_is_missing() {
        let s = src("Some Obscure Demo", &["Nobody Known"], None, Some(100));
        assert_eq!(
            best(&s, vec![song("z", "Bohemian Rhapsody", "Queen", None, "5:55")]).0,
            Tier::Missing
        );
        assert_eq!(best(&s, vec![]).0, Tier::Missing);
    }

    #[test]
    fn album_cards() {
        let want = SavedAlbum { title: "1984 (Remastered)".into(), artist: "Van Halen".into() };
        let card = |title: &str, sub: &str| BrowseItem {
            kind: "album",
            id: "MPRE".into(),
            title: title.into(),
            subtitle: Some(sub.into()),
            thumbnail: None,
            duration: None,
            album_id: None,
            artist_runs: Vec::new(),
            play_count: None,
            is_video: false,
            is_upload: false,
            explicit: false,
        };
        assert!(
            album_score(&want, &card("1984", "Album \u{2022} Van Halen \u{2022} 1984")) >= 0.75
        );
        assert!(
            album_score(&want, &card("1984", "Album \u{2022} Some Tribute Band \u{2022} 2004"))
                < 0.75
        );
    }
}
