//! PoToken / BotGuard generator (context/04, context/13).
//!
//! Two tokens per session/video (context/04): a session token (minted from visitorData, cached in
//! Rust with its TTL — [`PoTokenGenerator::get_session_po_token`]) → the `/player` request body; a
//! per-video streaming token (minted from videoId, lazy —
//! [`PoTokenGenerator::get_streaming_po_token`]) → `&pot=` on the stream URL. Everything is wrapped
//! so a failure or timeout returns `None` and the orchestrator falls through to the non-PoToken
//! clients (graceful degradation — context/06 §5).
//!
//! BotGuard itself runs in [`botguard`], on `rustypipe-botguard` (deno_core + JSDOM), **not** in a
//! hidden webview. That is the 2026-08-25 fix for KI-1: googlevideo rejects every token a real
//! browser engine mints, whatever origin, engine, user agent or settings it is given, because
//! `/GenerateIT` hands that attestation an integrity token of the wrong class. See `botguard.rs`
//! and `progress/KNOWN-ISSUES.md` KI-1.

mod botguard;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::db::{now_secs, Db};
use crate::http::WEB_UA;

/// Overall budget for a full bootstrap. Generous because it covers up to `MAX_BOOTSTRAPS` BotGuard
/// solves (each one a `/Create`, a VM run and a `/GenerateIT`, measured at ~0.8s) while it hunts
/// for the accepted integrity-token class.
const MINT_BUDGET: Duration = Duration::from_secs(60);
/// Budget for one mint on an already-bootstrapped runtime. Pure JS, no network.
const CALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Safety margin before the integrity token's TTL (context/04 §GenerateIT).
const EXPIRY_MARGIN: Duration = Duration::from_secs(10 * 60);

struct Minter {
    session_id: String,
    expires_at: Instant,
    inner: botguard::Minter,
    last_used: Instant,
}

impl Minter {
    /// Still good: same process, integrity token not yet expired. Deliberately not keyed by
    /// identity — see `PoTokenGenerator::ensure_minter`. `session_id` records which identity
    /// bootstrapped it, for the log only.
    fn live(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

/// Where a session token is persisted between runs. Internal: not in `UI_SETTINGS`, so the
/// webview can neither read nor write it.
///
/// Legacy single-identity key: still read when the current one is absent (see
/// [`load_session_tokens`]), never written any more.
pub(crate) const SESSION_TOKEN_KEY: &str = "potoken_session";
/// Current key: one entry per identity. The account's login-bound visitorData and the anonymous
/// one downloads mint against are two different sessions, and a single-slot cache made every
/// alternating playback/download resolve pay a whole BotGuard bootstrap (measured: one per
/// resolve while a batch ran alongside the music) — and evict the other identity's token while
/// it was at it.
pub(crate) const SESSION_TOKENS_KEY: &str = "potoken_sessions";
/// How many identities keep a session token in memory. Two in practice (the session's visitor and
/// the per-process anonymous one); the bound is what keeps a burst of refreshes from growing it.
const MAX_SESSION_TOKENS: usize = 4;

/// Cached session token (context/04: minted from visitorData, ~12h TTL). Lives OUTSIDE the
/// [`Minter`] so the mint-and-drop idle teardown doesn't force a full BotGuard bootstrap on the
/// next track start just to re-learn a string we already had.
///
/// Also survives the process, in `settings`. Google's own `/GenerateIT` says how long it is good
/// for (43200s, ~12h), and re-minting means standing up a V8 isolate and solving BotGuard several
/// times over to re-learn a string we were told stays valid until tomorrow. `expires_at` is
/// therefore wall-clock, not an `Instant`, which cannot outlive the process.
#[derive(serde::Serialize, serde::Deserialize)]
struct SessionToken {
    session_id: String,
    token: String,
    expires_at: i64,
}

impl SessionToken {
    fn valid_for(&self, session_id: &str, now: i64) -> bool {
        self.session_id == session_id && now < self.expires_at
    }
}

pub struct PoTokenGenerator {
    db: Arc<Db>,
    /// The one live BotGuard runtime. Single-slot on purpose: the host thread holds at most one
    /// isolate, so a second one would replace the first and invalidate its generation. It is NOT
    /// bound to an identity any more — `mint(ident)` binds whatever identifier it is handed at
    /// mint time — which is what lets the account and the anonymous downloads share one bootstrap
    /// instead of evicting each other's runtime on every alternation.
    minter: Mutex<Option<Minter>>,
    /// Session token cache, one entry per identity — see [`SessionToken`] and
    /// [`SESSION_TOKENS_KEY`].
    session_tokens: Mutex<Vec<SessionToken>>,
    /// Latched once the JS runtime proves unusable — thereafter always degrade to the non-PoToken
    /// clients (context/04 §BadWebViewException).
    runtime_bad: AtomicBool,
}

/// Tokens stored by previous runs: the current multi-identity row first, else the legacy
/// single-identity one. An unreadable row reads as "no token" rather than panicking the
/// constructor, and an expired one is simply never returned by `cached_session_token`.
fn load_session_tokens(db: &Db) -> Vec<SessionToken> {
    if let Some(raw) = db.get_setting(SESSION_TOKENS_KEY) {
        if let Ok(tokens) = serde_json::from_str::<Vec<SessionToken>>(&raw) {
            if !tokens.is_empty() {
                return tokens;
            }
        }
    }
    db.get_setting(SESSION_TOKEN_KEY)
        .and_then(|raw| serde_json::from_str::<SessionToken>(&raw).ok())
        .into_iter()
        .collect()
}

impl PoTokenGenerator {
    pub fn new(db: Arc<Db>) -> Self {
        // A token stored by a previous run is as good as one minted now, right up to its expiry.
        // A wrong-session or expired one is simply never returned by `cached_session_token`, so it
        // costs nothing to load it optimistically and let the normal validity check reject it.
        let stored = load_session_tokens(&db);
        if let Some(t) = stored.iter().max_by_key(|t| t.expires_at) {
            tracing::debug!(
                expires_in = t.expires_at - now_secs(),
                identities = stored.len(),
                "loaded stored PoToken session"
            );
        }
        PoTokenGenerator {
            db,
            minter: Mutex::new(None),
            session_tokens: Mutex::new(stored),
            runtime_bad: AtomicBool::new(false),
        }
    }

    /// Forget one identity's session token (`Some(visitorData)`), or every identity's (`None`).
    ///
    /// This is the orchestrator's off-hot-path self-heal: a web-client stream that failed
    /// validation means Google stopped honouring that identity's token, and without this it would
    /// be replayed for the rest of its nominal 12 hours instead of only until the next mint.
    /// Only the token goes: a live runtime stays warm for the identities that were not implicated
    /// — an anonymous download being refused is no reason to make playback re-solve BotGuard.
    pub async fn invalidate_token_for(&self, session_id: Option<&str>) {
        let mut guard = self.session_tokens.lock().await;
        match session_id {
            Some(id) => guard.retain(|t| t.session_id != id),
            None => guard.clear(),
        }
        self.persist_tokens(&guard);
    }

    /// Write the cache to `settings` under the current key and retire the legacy one (whose
    /// content, if it was the only copy, is already in this cache — [`load_session_tokens`]).
    /// Caller holds `session_tokens`.
    fn persist_tokens(&self, tokens: &[SessionToken]) {
        if let Ok(json) = serde_json::to_string(tokens) {
            self.db.set_setting(SESSION_TOKENS_KEY, &json);
        }
        self.db.delete_setting(SESSION_TOKEN_KEY);
    }

    /// Session token for the `/player` request body (context/04). Cheap when cached; otherwise
    /// performs the full bootstrap (and leaves the minter warm for streaming-token mints).
    pub async fn get_session_po_token(&self, visitor_data: &str) -> Option<String> {
        if self.runtime_bad.load(Ordering::SeqCst) {
            return None;
        }
        if let Some(token) = self.cached_session_token(visitor_data).await {
            return Some(token);
        }
        match timeout(MINT_BUDGET, self.mint_session(visitor_data)).await {
            Ok(Ok(token)) => Some(token),
            Ok(Err(e)) => {
                self.on_failure("session", &e).await;
                None
            }
            Err(_) => {
                tracing::warn!("PoToken session mint timed out — degrading");
                self.teardown().await;
                None
            }
        }
    }

    /// Per-video streaming token for the `&pot=` URL param (context/04). Builds/reuses the
    /// minter; call ONLY when a web-client stream URL actually resolved (post-decipher).
    pub async fn get_streaming_po_token(
        &self,
        video_id: &str,
        visitor_data: &str,
    ) -> Option<String> {
        if self.runtime_bad.load(Ordering::SeqCst) {
            return None;
        }
        match timeout(MINT_BUDGET, self.mint_streaming(video_id, visitor_data)).await {
            Ok(Ok(pot)) => Some(pot),
            Ok(Err(e)) => {
                self.on_failure(video_id, &e).await;
                None
            }
            Err(_) => {
                tracing::warn!(video_id, "PoToken streaming mint timed out — degrading");
                self.teardown().await;
                None
            }
        }
    }

    async fn on_failure(&self, what: &str, e: &botguard::Error) {
        tracing::warn!(what, error = %e, "PoToken mint failed — degrading");
        if matches!(e, botguard::Error::Fatal(_)) {
            self.runtime_bad.store(true, Ordering::SeqCst);
        }
        self.teardown().await;
    }

    async fn cached_session_token(&self, visitor_data: &str) -> Option<String> {
        let now = now_secs();
        self.session_tokens
            .lock()
            .await
            .iter()
            .find(|t| t.valid_for(visitor_data, now))
            .map(|t| t.token.clone())
    }

    /// Ensure `self.minter` holds a live runtime, (re)building it if there is none or its
    /// integrity token has expired. Returns the locked guard so callers needing the minter
    /// (streaming mint) can keep using it without a re-lock race.
    ///
    /// Deliberately NOT keyed by identity: the host thread holds one isolate, so bootstrapping a
    /// second one would only invalidate the first's generation — which is exactly what the old
    /// identity check made every alternating playback/download resolve do, at a full BotGuard
    /// solve (~0.4–2 s, re-rolled up to `MAX_BOOTSTRAPS` times) per resolve. What is keyed by
    /// identity is the session token (`session_tokens`); a token missing for this visitor is
    /// minted on the live runtime by [`Self::mint_session`].
    async fn ensure_minter<'a>(
        &'a self,
        visitor_data: &str,
    ) -> Result<tokio::sync::MutexGuard<'a, Option<Minter>>, botguard::Error> {
        let mut guard = self.minter.lock().await;
        match guard.as_ref() {
            Some(m) if m.live() => {
                // Same runtime, possibly minted for the other identity: `mint(ident)` binds the
                // identifier at mint time, so no bootstrap is owed — that is the whole point.
                tracing::debug!(
                    other_identity = m.session_id != visitor_data,
                    "reusing the live BotGuard runtime"
                );
            }
            _ => *guard = Some(self.create_minter(visitor_data).await?),
        }
        Ok(guard)
    }

    /// Session token for `visitor_data`, off a warm runtime when there is one.
    ///
    /// `create_minter` caches one on the way for the identity it bootstrapped, but the
    /// orchestrator's self-heal drops cached tokens and leaves the runtime alive
    /// (`invalidate_token_for`, called from orchestrator.rs when a web-client stream fails HEAD
    /// validation), and a second identity (the anonymous downloads) starts with no token of its
    /// own. Minting here is what stops either from leaving that identity with no session token
    /// until the idle teardown: while a queue keeps playing, `last_used` keeps refreshing and the
    /// runtime never ages out.
    async fn mint_session(&self, visitor_data: &str) -> Result<String, botguard::Error> {
        let mut guard = self.ensure_minter(visitor_data).await?;
        if let Some(token) = self.cached_session_token(visitor_data).await {
            return Ok(token); // ensure_minter built one, which caches a token on the way
        }
        let minter = guard.as_mut().expect("minter present");
        minter.last_used = Instant::now();
        // The token is only good while the integrity token behind it is, which is what the
        // minter's own expiry tracks.
        let good_for = minter.expires_at.saturating_duration_since(Instant::now());
        let token = timeout(CALL_TIMEOUT, minter.inner.mint(visitor_data))
            .await
            .map_err(|_| botguard::Error::Transient("session mint timed out".into()))??;
        self.store_session_token(visitor_data, token.clone(), good_for).await;
        Ok(token)
    }

    /// Per-video token (identifier = videoId). One retry with a fresh minter on failure.
    async fn mint_streaming(
        &self,
        video_id: &str,
        visitor_data: &str,
    ) -> Result<String, botguard::Error> {
        let mut guard = self.ensure_minter(visitor_data).await?;
        let minter = guard.as_mut().expect("minter present");
        minter.last_used = Instant::now();
        match timeout(CALL_TIMEOUT, minter.inner.mint(video_id)).await {
            Ok(Ok(pot)) => Ok(pot),
            other => {
                let why = match other {
                    Ok(Err(e)) => e.to_string(),
                    _ => "mint timed out".to_owned(),
                };
                tracing::debug!(error = why, "per-video mint failed, rebuilding minter once");
                let fresh = self.create_minter(visitor_data).await?;
                let pot = timeout(CALL_TIMEOUT, fresh.inner.mint(video_id))
                    .await
                    .map_err(|_| botguard::Error::Transient("mint timed out".into()))??;
                *guard = Some(fresh);
                Ok(pot)
            }
        }
    }

    /// Full bootstrap: a BotGuard runtime that landed in the accepted integrity-token class, plus
    /// the session token it minted on the way (see [`botguard::Minter::spawn`]).
    async fn create_minter(&self, session_id: &str) -> Result<Minter, botguard::Error> {
        let b = botguard::Minter::spawn(WEB_UA.to_owned(), session_id.to_owned()).await?;
        // One lifetime, two clocks: the minter never outlives this process, so its expiry stays an
        // `Instant`, while the session token is written to disk and needs wall-clock.
        let good_for = Duration::from_secs(b.lifetime_secs).saturating_sub(EXPIRY_MARGIN);

        self.store_session_token(session_id, b.session_token, good_for).await;

        Ok(Minter {
            session_id: session_id.to_owned(),
            expires_at: Instant::now() + good_for,
            inner: b.minter,
            last_used: Instant::now(),
        })
    }

    /// Cache a session token good for `good_for`, in memory and on disk.
    ///
    /// Locks `session_tokens` and nothing else: every caller already holds the `minter` lock, so
    /// the one lock order in this file is minter → session_tokens.
    async fn store_session_token(&self, session_id: &str, token: String, good_for: Duration) {
        let token = SessionToken {
            session_id: session_id.to_owned(),
            token,
            expires_at: now_secs() + good_for.as_secs() as i64,
        };
        let mut guard = self.session_tokens.lock().await;
        // One entry per identity: replacing this identity's, never evicting another's.
        guard.retain(|t| t.session_id != token.session_id && t.expires_at > now_secs());
        guard.push(token);
        while guard.len() > MAX_SESSION_TOKENS {
            let oldest = guard.iter().enumerate().min_by_key(|(_, t)| t.expires_at);
            match oldest.map(|(i, _)| i) {
                Some(i) => {
                    guard.remove(i);
                }
                None => break,
            }
        }
        self.persist_tokens(&guard);
    }

    /// Warm the session token for `visitor_data` (context/04 §startup). Non-fatal.
    pub async fn prewarm(&self, visitor_data: &str) {
        if self.runtime_bad.load(Ordering::SeqCst) {
            return;
        }
        // The only reason to prewarm is to have the session token ready before the first /player
        // call, and one stored by a previous run is exactly that. The per-video streaming path
        // builds its own minter on demand if it ever needs one, so there is nothing else to warm.
        if self.cached_session_token(visitor_data).await.is_some() {
            tracing::info!("PoToken session token still valid — skipping the BotGuard bootstrap");
            return;
        }
        // A live runtime mints this identity's token without a new bootstrap; `mint_session`
        // only stands BotGuard up when there is no runtime to mint on.
        match timeout(MINT_BUDGET, self.mint_session(visitor_data)).await {
            Ok(Ok(_)) => tracing::debug!("PoToken session token minted (prewarm)"),
            Ok(Err(e)) => tracing::warn!(error = %e, "PoToken prewarm failed"),
            Err(_) => tracing::warn!("PoToken prewarm timed out"),
        }
    }

    /// Drop the runtime if it's been idle longer than `idle` — the mint-and-drop memory policy
    /// (Phase-0 decision): keep it warm while the queue mints, drop it when idle. Dropping the
    /// minter ends its thread and frees the V8 isolate.
    // ponytail: called from a periodic task in lib.rs; no self-spawned monitor.
    pub async fn teardown_if_idle(&self, idle: Duration) {
        let mut guard = self.minter.lock().await;
        if guard.as_ref().is_some_and(|m| m.last_used.elapsed() >= idle) {
            *guard = None;
            tracing::debug!("BotGuard runtime torn down (idle)");
        }
    }

    async fn teardown(&self) {
        *self.minter.lock().await = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(session_id: &str, expires_at: i64) -> SessionToken {
        SessionToken { session_id: session_id.to_owned(), token: "tok".to_owned(), expires_at }
    }

    /// The self-heal sequence: orchestrator.rs invalidates the session token while the minter is
    /// still alive, and something has to be able to put one back (before this, nothing but
    /// `create_minter` ever wrote one, so the session stayed tokenless until the idle teardown).
    /// The mint itself needs V8 and the network, so the end-to-end proof is the `live` test in
    /// botguard.rs; this pins the cache and the `settings` row either side of it.
    #[tokio::test]
    async fn a_session_token_can_be_stored_again_after_invalidation() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        let g = PoTokenGenerator::new(db);

        g.store_session_token("vd123", "tok".to_owned(), Duration::from_secs(3600)).await;
        assert_eq!(g.cached_session_token("vd123").await.as_deref(), Some("tok"));
        assert!(g.db.get_setting(SESSION_TOKENS_KEY).is_some());
        assert_eq!(g.db.get_setting(SESSION_TOKEN_KEY), None, "the legacy row is retired");

        g.invalidate_token_for(None).await;
        assert_eq!(g.cached_session_token("vd123").await, None);
        assert_eq!(g.db.get_setting(SESSION_TOKENS_KEY).as_deref(), Some("[]"));

        g.store_session_token("vd123", "tok2".to_owned(), Duration::from_secs(3600)).await;
        assert_eq!(g.cached_session_token("vd123").await.as_deref(), Some("tok2"));
    }

    /// The downloads regression this whole rework exists for: the account's session token and the
    /// anonymous one are two identities, and a single-slot cache made every alternating
    /// playback/download resolve re-solve BotGuard — while evicting the other identity's token.
    #[tokio::test]
    async fn each_identity_keeps_its_own_session_token() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        let g = PoTokenGenerator::new(db);

        g.store_session_token(
            "account-visitor",
            "tok-account".to_owned(),
            Duration::from_secs(3600),
        )
        .await;
        g.store_session_token("anon-visitor", "tok-anon".to_owned(), Duration::from_secs(3600))
            .await;

        assert_eq!(g.cached_session_token("account-visitor").await.as_deref(), Some("tok-account"));
        assert_eq!(g.cached_session_token("anon-visitor").await.as_deref(), Some("tok-anon"));

        // And across a restart: both rows are what `new` reads back.
        let reloaded = PoTokenGenerator::new(Arc::clone(&g.db));
        assert_eq!(
            reloaded.cached_session_token("account-visitor").await.as_deref(),
            Some("tok-account")
        );
        assert_eq!(
            reloaded.cached_session_token("anon-visitor").await.as_deref(),
            Some("tok-anon")
        );
    }

    /// The self-heal drops the token of the identity whose stream failed. The other identity's
    /// token — and the warm runtime behind it — must be untouched: an anonymous download being
    /// refused is no reason to make playback re-solve BotGuard.
    #[tokio::test]
    async fn invalidating_one_identity_leaves_the_other_alone() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        let g = PoTokenGenerator::new(db);
        g.store_session_token(
            "account-visitor",
            "tok-account".to_owned(),
            Duration::from_secs(3600),
        )
        .await;
        g.store_session_token("anon-visitor", "tok-anon".to_owned(), Duration::from_secs(3600))
            .await;

        g.invalidate_token_for(Some("anon-visitor")).await;
        assert_eq!(g.cached_session_token("anon-visitor").await, None);
        assert_eq!(g.cached_session_token("account-visitor").await.as_deref(), Some("tok-account"));
        assert!(g.db.get_setting(SESSION_TOKENS_KEY).unwrap().contains("tok-account"));

        // Re-storing one identity replaces only that identity's row.
        g.store_session_token("anon-visitor", "tok-anon-2".to_owned(), Duration::from_secs(3600))
            .await;
        assert_eq!(g.cached_session_token("anon-visitor").await.as_deref(), Some("tok-anon-2"));
        assert_eq!(g.cached_session_token("account-visitor").await.as_deref(), Some("tok-account"));
    }

    /// An install that has only ever written the legacy single-identity row must still load it:
    /// otherwise every upgrade would silently pay a fresh BotGuard bootstrap at first launch.
    #[tokio::test]
    async fn a_legacy_single_token_row_still_loads() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        db.set_setting(
            SESSION_TOKEN_KEY,
            &serde_json::to_string(&token("vd-legacy", now_secs() + 3_600)).unwrap(),
        );

        let g = PoTokenGenerator::new(Arc::clone(&db));
        assert_eq!(g.cached_session_token("vd-legacy").await.as_deref(), Some("tok"));

        // The first write migrates it: current key written, legacy key gone.
        g.store_session_token("vd-new", "tok".to_owned(), Duration::from_secs(3600)).await;
        assert!(db.get_setting(SESSION_TOKENS_KEY).is_some());
        assert_eq!(db.get_setting(SESSION_TOKEN_KEY), None);
        assert_eq!(g.cached_session_token("vd-legacy").await.as_deref(), Some("tok"));
    }

    /// The cache is keyed by identity but bounded: a burst of visitor refreshes must not grow it
    /// without limit, and the entry that goes must never be a fresher one.
    #[tokio::test]
    async fn the_token_cache_stays_bounded() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        let g = PoTokenGenerator::new(db);
        for i in 0..(MAX_SESSION_TOKENS + 3) {
            g.store_session_token(
                &format!("vd-{i}"),
                "tok".to_owned(),
                Duration::from_secs(3600 + i as u64),
            )
            .await;
        }
        let cached = g.session_tokens.lock().await;
        assert_eq!(cached.len(), MAX_SESSION_TOKENS);
        assert!(cached.iter().any(|t| t.session_id == format!("vd-{}", MAX_SESSION_TOKENS + 2)));
        assert!(!cached.iter().any(|t| t.session_id == "vd-0"), "the least long-lived went first");
    }

    /// An expired token is never handed out, even though the row is still there — `valid_for` is
    /// the one gate, so an expiry that reads as "still mine" would replay a dead pot for hours.
    #[tokio::test]
    async fn an_expired_token_is_never_handed_out() {
        let db = Arc::new(Db::open(std::path::Path::new(":memory:")).unwrap());
        let g = PoTokenGenerator::new(db);
        g.store_session_token("vd123", "tok".to_owned(), Duration::ZERO).await;
        assert_eq!(g.cached_session_token("vd123").await, None);
    }

    #[test]
    fn session_token_valid_for_matching_id_and_future_expiry() {
        assert!(token("vd123", 1_000).valid_for("vd123", 900));
    }

    #[test]
    fn session_token_invalid_for_wrong_id() {
        // Signing in or out changes visitorData, so a token minted for the old one must not be
        // replayed against the new one.
        assert!(!token("vd123", 1_000).valid_for("other", 900));
    }

    #[test]
    fn session_token_invalid_when_expired() {
        assert!(!token("vd123", 1_000).valid_for("vd123", 1_000), "expiry is exclusive");
        assert!(!token("vd123", 1_000).valid_for("vd123", 1_001));
    }

    #[test]
    fn session_token_survives_a_round_trip_through_settings() {
        // What is stored between runs. A shape change here silently turns every launch back into
        // a full BotGuard bootstrap (the load is a best-effort `ok()`), so pin it.
        let json = serde_json::to_string(&token("vd123", 4_000)).unwrap();
        let back: SessionToken = serde_json::from_str(&json).unwrap();
        assert!(back.valid_for("vd123", 3_999));
        assert_eq!(back.token, "tok");

        // Anything unreadable must read as "no token" rather than panic the constructor.
        assert!(serde_json::from_str::<SessionToken>("{}").is_err());
        assert!(serde_json::from_str::<SessionToken>("not json").is_err());
    }
}
