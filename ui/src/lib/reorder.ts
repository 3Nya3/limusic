// Pure logic for reordering playlist rows.
import type { SongItem } from './api.ts';
import { moveTarget } from './queue.ts';

// `dropAt` is the index the dragged row goes in front of.
export function reorder<T>(items: T[], from: number, dropAt: number): T[] {
	const to = moveTarget(from, dropAt);
	if (to === null) return items;
	const copy = [...items];
	const [item] = copy.splice(from, 1);
	copy.splice(to, 0, item);
	return copy;
}

// The `set_video_id` of the row the dragged item lands in front of, telling YouTube where to move it.
export function successorId(items: SongItem[], from: number, dropAt: number): string | null {
	if (dropAt >= items.length) return null;
	return items[dropAt]?.set_video_id ?? null;
}

// Forbids dropping at the end of loaded rows while more pages exist, because the true tail is still unfetched.
export function canDropAt(dropAt: number, total: number, hasMore: boolean): boolean {
	if (hasMore && dropAt === total) return false;
	return true;
}
