// Offline downloads: the shapes the backend reports and the pure
// helpers every surface shares. Kept out of the `.svelte.ts` store so the logic is testable under
// node (`downloads.check.ts`), the same split as `queue.ts` / `queue.svelte.ts`.
import type { SongItem } from './api';

/** Where one queued download is in its life. Mirrors the backend's `DownloadItem.state`. */
export type DownloadState = 'queued' | 'downloading' | 'paused' | 'done' | 'error' | 'cancelled';

/**
 * One download the backend knows about. Field names are snake_case on purpose: the payload is
 * handed straight to `downloads_list` / the events, and re-spelling it here would be one more
 * place a rename can silently drop a field.
 */
export interface DownloadItem {
	id: string;
	video_id: string;
	title: string;
	artists: string;
	album: string | null;
	thumbnail: string | null;
	state: DownloadState;
	itag: number | null;
	mime: string | null;
	/** What the backend actually got, for the row's quality chip ("AAC 256kbps"). */
	quality_label: string;
	/** Absolute path of the saved file; null while there is none yet. */
	path: string | null;
	bytes_done: number;
	bytes_total: number | null;
	error: string | null;
	added_at: number;
	updated_at: number;
	/** Length in seconds when known; null for a network download, which never had one to read. */
	duration_secs: number | null;
}

/** One album or playlist stored as a single offline object. */
export type DownloadCollectionKind = 'album' | 'playlist';
export interface DownloadCollection {
	id: string;
	kind: DownloadCollectionKind;
	title: string;
	subtitle: string | null;
	artwork_path: string | null;
	track_count: number;
	downloaded_count: number;
	/** Bytes the finished members hold on disk, summed by the backend over the membership. */
	size_bytes: number;
	/** Member video ids in the collection's original order. */
	video_ids: string[];
}

export interface CollectionRemovalPreview {
	delete_count: number;
	keep_count: number;
}

/** `downloads_list` initial snapshot. Change events retain their existing track-array payload. */
export interface DownloadSnapshot {
	items: DownloadItem[];
	collections: DownloadCollection[];
}

/** Actions accepted by `downloads_action`. */
export type DownloadAction =
	| 'pause'
	| 'resume'
	| 'cancel'
	| 'retry'
	| 'remove'
	| 'remove_collection'
	| 'collection_removal_preview';

export type DownloadQuality = 'HIGH' | 'LOW' | 'AUTO';
export const DEFAULT_DOWNLOAD_QUALITY: DownloadQuality = 'HIGH';
export const DEFAULT_DOWNLOAD_CONCURRENCY = 2;
export const MIN_DOWNLOAD_CONCURRENCY = 1;
export const MAX_DOWNLOAD_CONCURRENCY = 4;

export function parseDownloadQuality(v: string | null | undefined): DownloadQuality {
	const up = (v ?? '').toUpperCase();
	return up === 'LOW' || up === 'AUTO' ? up : DEFAULT_DOWNLOAD_QUALITY;
}

/** A stored concurrency from anywhere (a hand-edited setting, an old build) lands in 1..4. */
export function clampDownloadConcurrency(v: string | number | null | undefined): number {
	const n = typeof v === 'number' ? v : Number.parseInt(v ?? '', 10);
	if (!Number.isFinite(n)) return DEFAULT_DOWNLOAD_CONCURRENCY;
	return Math.min(MAX_DOWNLOAD_CONCURRENCY, Math.max(MIN_DOWNLOAD_CONCURRENCY, Math.round(n)));
}

/**
 * Whether auto-download of newly liked songs is on (`download_auto_liked`). Off unless the stored
 * value reads as "true": an unset key or a hand-edited value never opts in, the same rule the
 * backend's `auto_liked` plays by (trim included, so the two cannot disagree).
 */
export function parseAutoLiked(v: string | null | undefined): boolean {
	return (v ?? '').trim() === 'true';
}

/**
 * The unlike twin (`download_auto_liked_remove`): when on, a like-sync removes the liked
 * collection's members that are no longer in the listing. Same opt-in rule as
 * [`parseAutoLiked`] — an unset or hand-edited value never opts in to deleting files.
 */
export function parseAutoLikedRemove(v: string | null | undefined): boolean {
	return (v ?? '').trim() === 'true';
}

/** The download record for a track, when it has one. */
export function downloadFor(
	items: readonly DownloadItem[],
	videoId: string
): DownloadItem | undefined {
	return items.find((d) => d.video_id === videoId);
}

/** Member rows in collection order, skipping ids that no longer have a download row. */
export function collectionDownloads(
	collection: Pick<DownloadCollection, 'video_ids'>,
	items: readonly DownloadItem[]
): DownloadItem[] {
	const byVideoId = new Map(items.map((item) => [item.video_id, item]));
	return collection.video_ids.flatMap((videoId) => {
		const item = byVideoId.get(videoId);
		return item ? [item] : [];
	});
}

/**
 * The songs a collection's own Play starts: its members that are actually finished, in the
 * collection's original order (which is `collection_tracks.position`). Only downloaded members
 * are playable — a row still queued or failed has no file to hand the player — so a collection
 * with nothing downloaded yet answers an empty list and its Play control disables.
 */
export function playableCollectionSongs(
	collection: Pick<DownloadCollection, 'video_ids'>,
	items: readonly DownloadItem[]
): SongItem[] {
	return collectionDownloads(collection, items)
		.filter((d) => d.state === 'done')
		.map(downloadItemToSong);
}

/** Same test as `isLocalId` (api.ts), inlined: this module is loaded by `downloads.check.ts` under
 *  plain node, and api.ts pulls in SvelteKit's virtual modules. A song's id is either a YouTube id
 *  or one of the local prefixes — all of which mean the file is already on this machine. */
const LOCAL_RE = /^LOCAL(ALBUM|ARTIST|PLAYLIST)?:/;

/**
 * Did this row come from a file on this device rather than off the network? The payload carries
 * no source flag on purpose — the two are the same shape — so the id prefix is the only marker,
 * and it exists purely for the few aggregates that count bytes the app itself put on disk.
 */
export function isImported(item: Pick<DownloadItem, 'video_id'>): boolean {
	return LOCAL_RE.test(item.video_id);
}

/** Can this song be handed to `download_items` at all? Local files are already on disk. */
export function canDownload(song: SongItem): boolean {
	return !!song.video_id && !LOCAL_RE.test(song.video_id);
}

/** 0..1 while the total is known; null when the backend has not reported a size (yet). */
export function progressFraction(item: DownloadItem): number | null {
	if (item.bytes_total == null || item.bytes_total <= 0) return null;
	return Math.min(1, Math.max(0, item.bytes_done / item.bytes_total));
}

/** A byte count fit for a progress line ("12.3 MB"). */
export function formatBytes(n: number): string {
	if (!Number.isFinite(n) || n <= 0) return '0 B';
	const units = ['B', 'KB', 'MB', 'GB'];
	let v = n;
	let i = 0;
	while (v >= 1024 && i < units.length - 1) {
		v /= 1024;
		i++;
	}
	return `${i === 0 ? String(Math.round(v)) : String(Number(v.toFixed(1)))} ${units[i]}`;
}

/**
 * The playlist picker's count box: "All" is a mode of its own, and in the number mode only a
 * positive whole number goes through. `max` (a playlist total that is actually known) turns a
 * number larger than the list into an error rather than a silent no-op in the backend.
 */
export function parseCount(
	raw: string,
	max?: number
): { count: number | null; error: 'empty' | 'invalid' | 'too_big' | null } {
	const text = raw.trim();
	if (!text) return { count: null, error: 'empty' };
	if (!/^\d+$/.test(text)) return { count: null, error: 'invalid' };
	const n = Number(text);
	if (n < 1) return { count: null, error: 'invalid' };
	if (max !== undefined && n > max) return { count: null, error: 'too_big' };
	return { count: n, error: null };
}

/** Per-state counts for the header line and the section headings. */
export interface DownloadSummary {
	total: number;
	queued: number;
	downloading: number;
	paused: number;
	done: number;
	error: number;
	cancelled: number;
	/** Everything that is not finished: downloading + queued + paused (+ failed/cancelled). */
	unfinished: number;
}

export function summarize(items: readonly DownloadItem[]): DownloadSummary {
	const s: DownloadSummary = {
		total: items.length,
		queued: 0,
		downloading: 0,
		paused: 0,
		done: 0,
		error: 0,
		cancelled: 0,
		unfinished: 0
	};
	for (const d of items) {
		s[d.state]++;
		if (d.state !== 'done') s.unfinished++;
	}
	return s;
}

const STATE_ORDER: Record<DownloadState, number> = {
	downloading: 0,
	queued: 1,
	paused: 2,
	error: 3,
	done: 4,
	cancelled: 5
};

/**
 * The page's order: what is moving first, then what is waiting, what needs attention, and the
 * finished library last — newest activity first inside each run.
 */
export function orderedDownloads(items: readonly DownloadItem[]): DownloadItem[] {
	return [...items].sort(
		(a, b) => STATE_ORDER[a.state] - STATE_ORDER[b.state] || b.updated_at - a.updated_at
	);
}

/** How the Downloaded songs section is ordered. */
export type DownloadSort = 'recent' | 'size';

/** The file size a finished row holds: what its own chip and the size sort use. */
export function sizeOf(item: Pick<DownloadItem, 'bytes_total' | 'bytes_done'>): number {
	return Math.max(0, item.bytes_total ?? item.bytes_done);
}

/**
 * The finished library in the chosen order. `recent` is the section's own order (newest
 * activity first, exactly what [`orderedDownloads`] produces for done rows); `size` is the
 * biggest file first. Size ties fall back to the recent order so the list never scrambles
 * between renders, and a size sort is stable under a shuffled input for the same reason.
 */
export function sortDone(downloaded: readonly DownloadItem[], sort: DownloadSort): DownloadItem[] {
	if (sort !== 'size') return [...downloaded];
	const rank = new Map(downloaded.map((d, i) => [d.id, i]));
	return [...downloaded].sort((a, b) => sizeOf(b) - sizeOf(a) || (rank.get(a.id) ?? 0) - (rank.get(b.id) ?? 0));
}

/**
 * Bytes the downloads are holding on disk right now: finished files at their full size, partials
 * at however far they got (cancelled rows report 0, so they cannot inflate the total). This is
 * what the Downloads header's storage chip shows, and it counts only what the feature put there —
 * music imported from the device was already on disk before this app ever saw it.
 */
export function storedBytes(items: readonly DownloadItem[]): number {
	return items.reduce((total, d) => total + (d.bytes_done > 0 && !isImported(d) ? d.bytes_done : 0), 0);
}

/**
 * The rows a bulk "Retry" may take: the two ends that stopped without finishing. Queued and
 * paused rows have their own controls, and a finished one has nothing to retry.
 */
export function retryableDownloads(items: readonly DownloadItem[]): DownloadItem[] {
	return items.filter((d) => d.state === 'error' || d.state === 'cancelled');
}

/** The two page sections, in that same order. */
export function splitDownloads(items: readonly DownloadItem[]): {
	queue: DownloadItem[];
	done: DownloadItem[];
} {
	const ordered = orderedDownloads(items);
	return {
		queue: ordered.filter((d) => d.state !== 'done'),
		done: ordered.filter((d) => d.state === 'done')
	};
}

/** A done download, as the SongItem worth playing offline: the original metadata, no stream. */
export function downloadItemToSong(item: DownloadItem): SongItem {
	return {
		video_id: item.video_id,
		title: item.title,
		artists: item.artists,
		album: item.album ?? undefined,
		thumbnail: item.thumbnail ?? undefined
	};
}

/** The t() key for a state chip. A map, not a template, so a new state is a type error here. */
export const DOWNLOAD_STATE_KEYS = {
	queued: 'downloads.states.queued',
	downloading: 'downloads.states.downloading',
	paused: 'downloads.states.paused',
	done: 'downloads.states.done',
	error: 'downloads.states.error',
	cancelled: 'downloads.states.cancelled'
} as const satisfies Record<DownloadState, string>;

/** The t() key for the badge tooltip (with the row's quality where that is the useful part). */
export const DOWNLOAD_BADGE_KEYS = {
	queued: 'downloads.badge_queued',
	downloading: 'downloads.badge_downloading',
	paused: 'downloads.badge_paused',
	done: 'downloads.badge_done',
	error: 'downloads.badge_error',
	cancelled: 'downloads.badge_cancelled'
} as const satisfies Record<DownloadState, string>;