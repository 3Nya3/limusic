// Self-check for the track list columns (`trackcolumns.ts`), run by `pnpm test`:
//
//     node --experimental-strip-types ui/src/lib/trackcolumns.check.ts
//
// Prints "ok" and exits 0, or throws on the first broken invariant.
import { addedLabel, availableColumns, DEFAULT_COLUMNS, parseColumns } from './trackcolumns.ts';
import type { SongItem } from './api.ts';

function eq<T>(got: T, want: T, what: string): void {
	const [g, w] = [JSON.stringify(got), JSON.stringify(want)];
	if (g !== w) throw new Error(`FAIL: ${what}: got ${g}, want ${w}`);
}

const song = (extra: Partial<SongItem> = {}): SongItem => ({
	video_id: 'v',
	title: 't',
	artists: 'a',
	...extra
});

// Only what some row can fill.
eq(availableColumns([song()]), ['mine', 'duration'], 'bare rows');
eq(
	availableColumns([song(), song({ album: 'X', added_at: 1, play_count: '5M' })]),
	['album', 'added', 'plays', 'mine', 'duration'],
	'one full row is enough'
);
eq(availableColumns([song({ album: 'X', play_count: '5M' })], true), ['plays', 'mine', 'duration'], 'album page drops Album');
eq(availableColumns([]), ['mine', 'duration'], 'empty list');

// Stored choice: unknown ids dropped, our order, broken values fall back.
eq(parseColumns('["duration","album","bogus"]'), ['album', 'duration'], 'reordered + unknown');
eq(parseColumns('[]'), [], 'everything off stays off');
eq(parseColumns(null), DEFAULT_COLUMNS, 'nothing stored');
eq(parseColumns('{oops'), DEFAULT_COLUMNS, 'broken JSON');
eq(parseColumns('"album"'), DEFAULT_COLUMNS, 'not an array');

// Date added: distance for four weeks, then the date.
const now = 1_760_000_000;
eq(addedLabel(now - 30, now, 'en'), 'now', 'seconds ago');
eq(addedLabel(now - 120, now, 'en'), '2 minutes ago', 'minutes');
eq(addedLabel(now - 5400, now, 'en'), '1 hour ago', '90 minutes is an hour');
eq(addedLabel(now - 86400, now, 'en'), 'yesterday', 'one day');
eq(addedLabel(now - 3 * 86400, now, 'en'), '3 days ago', 'days');
eq(addedLabel(now - 15 * 86400, now, 'en'), '2 weeks ago', 'weeks');
eq(addedLabel(now - 40 * 86400, now, 'en-US'), 'Aug 30, 2025', 'old enough for a date');
eq(addedLabel(now + 60, now, 'en'), 'now', 'a clock behind the stamp');

console.log('ok');
