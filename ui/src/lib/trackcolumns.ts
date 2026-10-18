// The optional columns of a playlist or album track list, and which of them a list can fill. Pure,
// so it can be checked without a DOM (`trackcolumns.check.ts`). The chosen set and the header that
// picks it live in `TrackColumns.svelte`.
//
// Only what the rows really carry is offered: a column every row would leave blank is worse than
// no column. Date added comes off YouTube's playlist rows (`voteSortValue`, see `added_at` in
// metadata.rs) and off the rows of a playlist kept on this machine. Nothing offers a release date:
// no row carries one.
import type { SongItem } from './api';

export type TrackColumn = 'album' | 'added' | 'by' | 'plays' | 'mine' | 'duration';

/** Picker order, which is also the cells' order left to right. Duration stays last, by the ⋯. */
export const COLUMNS: TrackColumn[] = ['album', 'added', 'by', 'plays', 'mine', 'duration'];

/** A fresh install: the rows exactly as they were before columns existed. Added by only ever shows
 *  on a collaborative playlist, where the avatars were always on. */
export const DEFAULT_COLUMNS: TrackColumn[] = ['by', 'plays', 'duration'];

/*
 * The row, left to right, laid out the same way by the header and by every row, so the labels sit
 * over their cells without a grid (the rows are separate elements, and windowed):
 *
 *   # | art + title / artist  (TITLE) | album (shares the rest 3:4 with the title) | fixed cells | END
 *
 * Title and album split what is left in proportion, so the album starts at the same place on every
 * row however long the names run. Everything that varies row to row (the saved mark, the thumbs, a
 * local file having neither) lives in END, which is one fixed width with its contents pushed right,
 * so it can never shift a column. Duration and the ⋯ end every row there, as before.
 *
 * The `@` variants read the list's own width (`@container` on its wrapper), so they follow the
 * sidebar as well as the window. They keep the title readable with every column on, and give way in
 * the order the columns matter: Date added first, then Your plays, then Album.
 */
export const TITLE = 'flex-[4_1_0%]';
export const END = 'w-44 justify-end';
export const CELL: Record<Exclude<TrackColumn, 'duration'>, string> = {
	album: 'hidden min-w-0 flex-[3_1_0%] @min-[40rem]:block',
	added: 'hidden w-28 shrink-0 @min-[52rem]:block',
	by: 'w-16 shrink-0',
	plays: 'w-14 shrink-0 text-right',
	mine: 'hidden w-16 shrink-0 text-right @min-[44rem]:block'
};

/**
 * The columns this list can fill. An album page leaves Album out: every row would repeat the title
 * above it.
 */
export function availableColumns(items: SongItem[], albumPage = false): TrackColumn[] {
	return COLUMNS.filter((c) => {
		if (c === 'album') return !albumPage && items.some((i) => i.album);
		if (c === 'added') return items.some((i) => i.added_at);
		if (c === 'by') return items.some((i) => i.added_by_avatars?.length);
		if (c === 'plays') return items.some((i) => i.play_count);
		return true; // Your plays is our own history, and every row has a length
	});
}

/** A stored choice, read back defensively: an unknown id is dropped, the order is ours. */
export function parseColumns(raw: string | null): TrackColumn[] {
	try {
		const v: unknown = JSON.parse(raw ?? 'null');
		if (Array.isArray(v)) return COLUMNS.filter((c) => v.includes(c));
	} catch {
		/* a broken value falls back to the default */
	}
	return DEFAULT_COLUMNS;
}

const formats = new Map<string, Intl.DateTimeFormat | Intl.RelativeTimeFormat>();
function format<T extends Intl.DateTimeFormat | Intl.RelativeTimeFormat>(
	key: string,
	make: () => T
): T {
	let f = formats.get(key);
	if (!f) formats.set(key, (f = make()));
	return f as T;
}

const STEPS: [Intl.RelativeTimeFormatUnit, number][] = [
	['minute', 60],
	['hour', 3600],
	['day', 86400],
	['week', 7 * 86400]
];

/**
 * "3 days ago" for the last four weeks, a plain date after that: a recent add reads better as a
 * distance, an old one as the day it happened. `at` and `now` are unix seconds.
 */
export function addedLabel(at: number, now: number, tag: string): string {
	const ago = Math.max(0, now - at);
	if (ago < 28 * 86400) {
		const rel = format(`r:${tag}`, () => new Intl.RelativeTimeFormat(tag, { numeric: 'auto' }));
		// The largest unit that fits at least once: 90 minutes is "1 hour ago", not "90 minutes ago".
		// Under a minute is "now".
		let [unit, size]: [Intl.RelativeTimeFormatUnit, number] = ['second', Infinity];
		for (const s of STEPS) if (ago >= s[1]) [unit, size] = s;
		return rel.format(-Math.floor(ago / size) || 0, unit);
	}
	return format(`d:${tag}`, () => new Intl.DateTimeFormat(tag, { dateStyle: 'medium' })).format(
		at * 1000
	);
}

/** The full date and time, for the cell's tooltip. */
export const addedTitle = (at: number, tag: string): string =>
	format(`f:${tag}`, () => new Intl.DateTimeFormat(tag, { dateStyle: 'long', timeStyle: 'short' }))
		.format(at * 1000);
