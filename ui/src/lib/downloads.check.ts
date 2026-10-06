// Self-check for the download helpers (`downloads.ts`). Same deal as `queue.check.ts`: no test
// runner in `ui/`, node runs TypeScript directly:
//
//     node --experimental-strip-types ui/src/lib/downloads.check.ts
//
// Prints "ok" and exits 0, or throws on the first broken invariant.
// The catalog comes in as a module (resolveJsonModule), not via node:fs — this file is type-checked
// by svelte-check against a DOM-only tsconfig, where `node:` imports have no types (see menu.check.ts).
import en from './locales/en.json' with { type: 'json' };
import {
	DOWNLOAD_BADGE_KEYS,
	DOWNLOAD_STATE_KEYS,
	canDownload,
	clampDownloadConcurrency,
	collectionDownloads,
	collectionSubtitle,
	downloadFor,
	downloadItemToSong,
	formatBytes,
	isImported,
	orderedDownloads,
	parseAutoLiked,
	parseAutoLikedRemove,
	parseCount,
	parseDownloadQuality,
	playableCollectionSongs,
	playlistTotal,
	progressFraction,
	retryableDownloads,
	sizeOf,
	sortDone,
	splitDownloads,
	storedBytes,
	summarize,
	type DownloadAction,
	type CollectionRemovalPreview,
	type DownloadCollection,
	type DownloadItem,
	type DownloadSnapshot,
	type DownloadState
} from './downloads.ts';

function ok(cond: boolean, what: string): void {
	if (!cond) throw new Error(`FAIL: ${what}`);
}

const item = (id: string, extra: Partial<DownloadItem> = {}): DownloadItem => ({
	id,
	video_id: `v${id}`,
	title: `Track ${id}`,
	artists: 'Artist',
	album: null,
	thumbnail: null,
	state: 'done',
	itag: null,
	mime: null,
	quality_label: '256 kbps',
	path: `/music/${id}.m4a`,
	bytes_done: 100,
	bytes_total: 100,
	error: null,
	added_at: 1,
	updated_at: 1,
	duration_secs: null,
	...extra
});

const collection: DownloadCollection = {
	id: 'VLabc',
	kind: 'playlist',
	title: 'Offline Mix',
	subtitle: '4 songs',
	artwork_path: '/downloads/artwork/VLabc.jpg',
	track_count: 4,
	downloaded_count: 2,
	size_bytes: 200,
	video_ids: ['v1', 'v2', 'v3', 'v4']
};
const snapshot: DownloadSnapshot = { items: [item('snapshot')], collections: [collection] };
const removeCollection: DownloadAction = 'remove_collection';
const previewAction: DownloadAction = 'collection_removal_preview';
const preview: CollectionRemovalPreview = { delete_count: 1, keep_count: 2 };
ok(
	snapshot.collections[0].id === 'VLabc' &&
		snapshot.collections[0].track_count === 4 &&
		snapshot.collections[0].downloaded_count === 2 &&
		snapshot.collections[0].size_bytes === 200 &&
		snapshot.collections[0].video_ids.join() === 'v1,v2,v3,v4',
	'download snapshot carries collection fields, the backend-summed size, and ordered members'
);
ok(
	snapshot.items.length === 1 &&
		removeCollection === 'remove_collection' &&
		previewAction === 'collection_removal_preview' &&
		preview.delete_count === 1 &&
		preview.keep_count === 2,
	'snapshot and collection removal preview contracts'
);
const memberRows = collectionDownloads(collection, [item('4'), item('2'), item('1')]);
ok(memberRows.map((d) => d.video_id).join() === 'v1,v2,v4', 'collection rows follow membership order and skip missing rows');

// --- per-collection play: exactly the downloaded members, in the collection's own order ----------
// The rows arrive in arbitrary payload order; the queue the Play button builds must follow
// `video_ids` (= collection_tracks.position) and keep only rows that are actually finished.
{
	const rows = [
		item('x3', { video_id: 'v3', state: 'queued' }), // member, not downloadable yet
		item('x2', { video_id: 'v2' }), // done member
		item('x1', { video_id: 'v1' }), // done member
		item('x9', { video_id: 'v9' }) // done but NOT a member
	];
	const songs = playableCollectionSongs(collection, rows);
	ok(songs.map((s) => s.video_id).join() === 'v1,v2', 'playable = done members only, in position order');
	ok(!songs.some((s) => s.video_id === 'v9'), 'a finished row outside the collection never joins this queue');
	ok(
		playableCollectionSongs(collection, [item('x3', { video_id: 'v3', state: 'queued' })]).length === 0,
		'a collection with nothing downloaded yet answers an empty list (disabled Play)'
	);
	// Order is the membership's, not the input's: shuffled input, same queue.
	const shuffled = [...rows].reverse();
	ok(
		playableCollectionSongs(collection, shuffled).map((s) => s.video_id).join() === 'v1,v2',
		'order does not depend on payload order'
	);
}

// --- the count a collection row shows: one source of truth per number ---------------------------
// Evidence (live VLLM walk, 2026-10-12, signed-in): 7 pages, 655 raw rows, 629 unique videoIds,
// 26 duplicated rows — while YouTube's subtitle claimed "680 songs". The subtitle's count is
// YouTube's own bookkeeping (rows it never renders); the row's counts come from our membership.
// collectionSubtitle rewrites only the leading count so both numbers on the row agree.
ok(
	collectionSubtitle('680 songs • 5+ hours', 629) === '629 songs • 5+ hours',
	'the live discrepancy: subtitle count replaced by the members we hold'
);
ok(
	collectionSubtitle('681 songs • 5+ hours', 630) === '630 songs • 5+ hours',
	'after a new like lands, the row still counts only what it holds'
);
ok(
	collectionSubtitle('15 tracks • 1 hour, 3 minutes', 15) === '15 tracks • 1 hour, 3 minutes',
	'an agreeing subtitle passes through unchanged'
);
ok(
	collectionSubtitle('1,024 songs', 998) === '998 songs',
	'comma-grouped counts are replaced too'
);
ok(collectionSubtitle('Riley', 629) === 'Riley', 'an owner-name subtitle has no count and passes through');
ok(collectionSubtitle(null, 10) === null, 'a missing subtitle stays missing');
// The progress number next to it is computed from the same membership:
ok(
	`${collection.downloaded_count} of ${collection.track_count} downloaded` === '2 of 4 downloaded',
	'progress counts come from track_count/downloaded_count — never from the subtitle'
);

// --- sort order: Recently downloaded (default) vs Size (largest first) ---------------------------
{
	const big = item('big', { bytes_total: 90_000_000, bytes_done: 90_000_000, updated_at: 1 });
	const small = item('small', { bytes_total: 500_000, bytes_done: 500_000, updated_at: 3 });
	const mid = item('mid', { bytes_total: 21_700_000, bytes_done: 21_700_000, updated_at: 2 });
	const done = [mid, small, big]; // "recent" order: mid newest… as the section orders them
	ok(
		sortDone(done, 'recent').map((d) => d.id).join() === 'mid,small,big',
		'recent keeps the section order'
	);
	ok(
		sortDone(done, 'size').map((d) => d.id).join() === 'big,mid,small',
		'size puts the biggest file first'
	);
	ok(
		sortDone([...done].reverse(), 'size').map((d) => d.id).join() === 'big,mid,small',
		'size order does not depend on input order'
	);
	// The user-approved bare style: "20.7 MB" — one digit, no unit flourish.
	ok(formatBytes(sizeOf(mid)) === '20.7 MB', "the size chip reads '20.7 MB'");
	ok(sizeOf({ bytes_total: null, bytes_done: 42 }) === 42, 'no total yet: the partial is the size');
	ok(sizeOf({ bytes_total: 10, bytes_done: 999 }) === 10, 'a finished row reports its total');
	ok(sizeOf({ bytes_total: null, bytes_done: -1 }) === 0, 'nonsense cannot go negative');
	// Sorting never moves the section's count: the length is invariant under both orders.
	ok(sortDone(done, 'size').length === done.length && sortDone(done, 'recent').length === done.length,
		'the section count is the same in every order');
}

// --- the count picker (positive integer, or All) -------------------------------------------------
ok(parseCount('7').count === 7 && parseCount('7').error === null, 'a positive integer goes through');
ok(parseCount(' 12 ').count === 12, 'whitespace is trimmed');
ok(parseCount('007').count === 7, 'leading zeros are still a number');
ok(parseCount('').error === 'empty', 'an empty box is not a number yet');
ok(parseCount('0').error === 'invalid', 'zero is not positive');
ok(parseCount('-3').error === 'invalid', 'negative numbers are refused');
ok(parseCount('1.5').error === 'invalid', 'fractions are refused');
ok(parseCount('12abc').error === 'invalid', 'junk after the digits is refused');
ok(parseCount('999', 100).error === 'too_big', 'more than the playlist holds is refused');
ok(parseCount('100', 100).count === 100, 'the whole list is a valid count');
ok(parseCount('5', 100).count === 5, 'within the list is fine');

// --- what a subtitle says the playlist holds ------------------------------------------------------
ok(playlistTotal('190 songs') === 190, 'plain count');
ok(playlistTotal('1,024 songs') === 1024, 'comma-grouped count');
ok(playlistTotal('Someone • 12 songs') === 12, 'owner-prefixed count');
ok(playlistTotal('12 Titel') === 12, 'a localized word still leaves the number');
ok(playlistTotal('A mixtape') === undefined, 'no number, no total');
ok(playlistTotal(null) === undefined, 'null subtitle');

// --- progress -------------------------------------------------------------------------------------
ok(progressFraction(item('a', { bytes_done: 50, bytes_total: 200 })) === 0.25, 'half way is a quarter');
ok(progressFraction(item('a')) === 1, 'a full file is 1');
ok(progressFraction(item('a', { bytes_done: 300, bytes_total: 200 })) === 1, 'over-count clamps to 1');
ok(progressFraction(item('a', { bytes_total: null })) === null, 'no size, no bar');
ok(progressFraction(item('a', { bytes_total: 0 })) === null, 'zero size is no size');
ok(formatBytes(0) === '0 B' && formatBytes(-5) === '0 B', 'nothing is nothing');
ok(formatBytes(512) === '512 B', 'bytes');
ok(formatBytes(1536) === '1.5 KB', 'kilobytes');
ok(formatBytes(26214400) === '25 MB', 'whole megabytes lose the point');
ok(formatBytes(2621440) === '2.5 MB', 'fractions keep one digit');
ok(formatBytes(5 * 1024 ** 3) === '5 GB', 'gigabytes');

// --- lookup and the offline song ------------------------------------------------------------------
const items = [item('1'), item('2'), item('3', { video_id: 'v3', state: 'downloading' })];
ok(downloadFor(items, 'v2')?.id === '2', 'found by video id');
ok(downloadFor(items, 'nope') === undefined, 'missing track');
const song = downloadItemToSong(item('9', { video_id: 'v9', album: 'Record', thumbnail: 'https://t' }));
ok(song.video_id === 'v9' && song.title === 'Track 9' && song.album === 'Record', 'song keeps metadata');
ok(song.thumbnail === 'https://t', 'artwork carried over');
ok(downloadItemToSong(item('9')).album === undefined, 'a missing album is absent, not null');
ok(
	canDownload({ video_id: 'dQw4w9WgXcQ', title: 't', artists: 'a' }),
	'a YouTube track can be downloaded'
);
ok(!canDownload({ video_id: 'LOCAL:C:/music/a.flac', title: 't', artists: 'a' }), 'a local file cannot');
ok(!canDownload({ video_id: 'LOCALPLAYLIST:3', title: 't', artists: 'a' }), 'nor a local playlist id');

// --- summary, ordering, sections -------------------------------------------------------------------
const mixed = [
	item('q', { state: 'queued' }),
	item('d1', { state: 'downloading', updated_at: 10 }),
	item('d2', { state: 'done', updated_at: 20 }),
	item('e', { state: 'error' }),
	item('p', { state: 'paused' }),
	item('d3', { state: 'done', updated_at: 5 }),
	item('c', { state: 'cancelled' })
];
const sum = summarize(mixed);
ok(sum.total === 7, 'total counts every row');
ok(sum.done === 2 && sum.queued === 1 && sum.downloading === 1 && sum.paused === 1, 'per-state counts');
ok(sum.error === 1 && sum.cancelled === 1, 'failures count too');
ok(sum.unfinished === 5, 'everything that is not done is unfinished');
ok(summarize([]).total === 0 && summarize([]).done === 0, 'empty list');

const order = orderedDownloads(mixed).map((d) => d.id).join();
ok(order === 'd1,q,p,e,d2,d3,c', 'downloading, queued, paused, failed, done (newest first), cancelled');
const parts = splitDownloads(mixed);
ok(parts.queue.length === 5 && parts.done.length === 2, 'the page splits done out of the queue');
ok(parts.done[0].id === 'd2', 'done rows keep the newest-first order');

// A shuffled input must not change the order (the page sorts the backend's arbitrary list).
const reversed = [...mixed].reverse();
ok(
	orderedDownloads(reversed).map((d) => d.id).join() === order,
	'order does not depend on the input order'
);

// --- storage on disk and the bulk actions ----------------------------------------------------------
// The header's storage chip: every byte the feature holds, whatever state its row is in.
ok(storedBytes([item('a'), item('b')]) === 200, 'storage sums the finished files');
ok(
	storedBytes([
		item('a', { state: 'downloading', bytes_done: 40, bytes_total: 100 }),
		item('b', { state: 'cancelled', bytes_done: 0, bytes_total: null }),
		item('c', { state: 'queued', bytes_done: 0, bytes_total: null })
	]) === 40,
	'a partial counts, a cancelled row and a queued one hold nothing'
);
ok(storedBytes([]) === 0, 'an empty list stores nothing');
ok(storedBytes([item('a', { bytes_done: -1 })]) === 0, 'nonsense sizes cannot inflate the total');

// Bulk retry: only the ends that did not finish, in list order.
ok(
	retryableDownloads(mixed)
		.map((d) => d.id)
		.join() === 'e,c',
	'failed and cancelled rows are retryable; queued, paused, running and done are not'
);
ok(retryableDownloads([item('d', { state: 'downloading' })]).length === 0, 'a running row is not');
ok(retryableDownloads([]).length === 0, 'nothing to retry in an empty list');

// --- files imported from this device ride the same list --------------------------------------------
const importedRow = item('imp', {
	video_id: 'LOCAL:C:/Music/Song.mp3',
	duration_secs: 213,
	quality_label: '320 kbps MP3',
	bytes_done: 400,
	bytes_total: 400
});
ok(isImported(importedRow), 'the local prefix marks an imported row');
ok(!isImported(item('dl')), 'a downloaded row is not marked');
ok(summarize([item('a'), importedRow]).done === 2, 'an imported row counts as finished, like any other');
ok(
	splitDownloads([item('q', { state: 'queued' }), importedRow])
		.done.map((d) => d.id)
		.join() === 'imp',
	'and it lands in the finished section beside them'
);
ok(orderedDownloads([item('d2'), importedRow])[0]?.id === 'd2', 'ordering treats it as an ordinary done row');
ok(storedBytes([item('a'), importedRow]) === 100, 'the storage chip counts only what the app put on disk');
ok(
	downloadItemToSong(importedRow).video_id === 'LOCAL:C:/Music/Song.mp3',
	'play keeps the id the local playback path already handles'
);
ok(
	importedRow.duration_secs === 213 && importedRow.quality_label === '320 kbps MP3',
	'the payload carries the duration and the real format'
);
ok(retryableDownloads([importedRow]).length === 0, 'an imported row has nothing to retry');

// --- settings parsing ------------------------------------------------------------------------------
ok(parseDownloadQuality('HIGH') === 'HIGH', 'stored HIGH');
ok(parseDownloadQuality('low') === 'LOW', 'lowercase still parses');
ok(parseDownloadQuality(undefined) === 'HIGH', 'default is HIGH (best available)');
ok(parseDownloadQuality('nonsense') === 'HIGH', 'junk falls back to HIGH');
ok(clampDownloadConcurrency('2') === 2, 'stored 2');
ok(clampDownloadConcurrency('0') === 1, 'below range clamps up');
ok(clampDownloadConcurrency('9') === 4, 'above range clamps down');
ok(clampDownloadConcurrency('x') === 2, 'junk falls back to the 2 default');
ok(clampDownloadConcurrency(undefined) === 2, 'unset is 2');
// Auto-download of newly liked songs: opt-in, so anything that is not "true" is off.
ok(parseAutoLiked('true') === true, 'stored true turns it on');
ok(parseAutoLiked(' true ') === true, 'trimmed like the backend reads it');
ok(parseAutoLiked('false') === false, 'stored false');
ok(parseAutoLiked(undefined) === false, 'unset is off — nothing wakes for it');
ok(parseAutoLiked('yes') === false, 'hand-edited garbage never opts in');
ok(parseAutoLiked('1') === false, 'a truthy-looking 1 is still not the opt-in');
// The unlike twin is a file-deleting opt-in, so it is at least as strict: same rule, same default.
ok(parseAutoLikedRemove('true') === true, 'stored true turns unlike-removal on');
ok(parseAutoLikedRemove(' true ') === true, 'trimmed, same as the backend reader');
ok(parseAutoLikedRemove('false') === false, 'stored false');
ok(parseAutoLikedRemove(undefined) === false, 'unset is off — a sync never removes anything');
ok(parseAutoLikedRemove('yes') === false, 'garbage never opts in to deleting files');
ok(parseAutoLikedRemove('1') === false, 'a truthy-looking 1 is still not the opt-in');

// --- the auto-like copy, in the downloads namespace ------------------------------------------------
// Straight from the English catalog, which svelte-check also type-checks every `t()` call against:
// the Settings row and the Liked playlist's dialog share this namespace, and the exact wording is
// the copy the feature shipped with.
ok(en.downloads.auto_liked === 'Auto-download new liked songs', 'the Settings row copy');
ok(
	en.downloads.dialog.auto_liked === 'Download new likes automatically',
	'the Liked playlist dialog checkbox copy'
);
ok(
	en.downloads.auto_liked_remove === 'Remove downloads for unliked songs',
	'the unlike option copy, directly under the auto-liked row'
);
ok(en.downloads.sort_recent === 'Recently downloaded', 'the sort default label');
ok(en.downloads.sort_size === 'Size (largest first)', 'the size sort label');
ok(
	en.downloads.play_collection_none === 'Nothing from this collection is downloaded yet',
	'the disabled Play state stays readable'
);

// --- translation keys exist for every state ---------------------------------------------------------
const states: DownloadState[] = ['queued', 'downloading', 'paused', 'done', 'error', 'cancelled'];
ok(states.every((s) => DOWNLOAD_STATE_KEYS[s].startsWith('downloads.states.')), 'state chips have keys');
ok(states.every((s) => DOWNLOAD_BADGE_KEYS[s].startsWith('downloads.badge_')), 'badges have keys');

console.log('ok');