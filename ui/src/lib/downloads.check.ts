// Self-check for the download helpers (`downloads.ts`). Same deal as `queue.check.ts`: no test
// runner in `ui/`, node runs TypeScript directly:
//
//     node --experimental-strip-types ui/src/lib/downloads.check.ts
//
// Prints "ok" and exits 0, or throws on the first broken invariant.
import {
	DOWNLOAD_BADGE_KEYS,
	DOWNLOAD_STATE_KEYS,
	canDownload,
	clampDownloadConcurrency,
	collectionDownloads,
	downloadFor,
	downloadItemToSong,
	formatBytes,
	orderedDownloads,
	parseCount,
	parseDownloadQuality,
	playlistTotal,
	progressFraction,
	retryableDownloads,
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
		snapshot.collections[0].video_ids.join() === 'v1,v2,v3,v4',
	'download snapshot carries collection fields and ordered members'
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

// --- translation keys exist for every state ---------------------------------------------------------
const states: DownloadState[] = ['queued', 'downloading', 'paused', 'done', 'error', 'cancelled'];
ok(states.every((s) => DOWNLOAD_STATE_KEYS[s].startsWith('downloads.states.')), 'state chips have keys');
ok(states.every((s) => DOWNLOAD_BADGE_KEYS[s].startsWith('downloads.badge_')), 'badges have keys');

console.log('ok');