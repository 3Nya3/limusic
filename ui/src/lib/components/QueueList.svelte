<script lang="ts">
	import { onMount, untrack } from 'svelte';
	import { flip } from 'svelte/animate';
	import { cubicOut } from 'svelte/easing';
	import { MediaQuery } from 'svelte/reactivity';
	import { HugeiconsIcon } from '@hugeicons/svelte';
	import { ArrowTurnBackwardIcon, InfinityIcon } from '@hugeicons/core-free-icons';
	import TrackRow from '$lib/components/TrackRow.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Switch } from '$lib/components/ui/switch';
	import * as api from '$lib/api';
	import { followPlaying, moveTarget, queueView, type QueueRow } from '$lib/queue';
	import { blockWindows, fullWindow, type RowWindow } from '$lib/rows';
	import { rowScroller } from '$lib/rows.svelte';
	import { dragScroll, QUEUE_ROW_MIME } from '$lib/dnd';
	import { playback, prefs, setAutoplay, openAddToPlaylist } from '$lib/player.svelte';
	import { lt } from '$lib/lt.svelte';
	import { t } from '$lib/i18n.svelte';

	const reducedMotion = new MediaQuery('(prefers-reduced-motion: reduce)');

	// Guests are add-only in a session: no removing, reordering, clearing or autoplay of their own.
	// The playing row can't be removed either (backend guards it too).
	const canEdit = $derived(lt.role !== 'guest');

	// --- drag to reorder ---------------------------------------------------------------------
	// Upcoming rows only: the playing track and what came before it stay put (the backend clamps to
	// the same range). `dropAt` is the queue index the dragged row goes *in front of*.
	let dragFrom = $state<number | null>(null);
	let dropAt = $state<number | null>(null);
	const canDrag = (i: number) => canEdit && i > playback.queue.currentIndex;

	function onDragStart(e: DragEvent, i: number) {
		if (!e.dataTransfer) return;
		// Our own type, so a card dragged in from a page (`ITEM_MIME`) can't be read as a row index.
		e.dataTransfer.setData(QUEUE_ROW_MIME, String(i));
		e.dataTransfer.effectAllowed = 'move';
		dragFrom = i;
	}

	function onDragOver(e: DragEvent, i: number) {
		if (dragFrom === null || !e.dataTransfer?.types.includes(QUEUE_ROW_MIME)) return;
		e.preventDefault(); // without this the drop never fires
		e.dataTransfer.dropEffect = 'move';
		const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
		dropAt = e.clientY < r.top + r.height / 2 ? i : i + 1;
	}

	function onDrop() {
		const to = dragFrom !== null && dropAt !== null ? moveTarget(dragFrom, dropAt) : null;
		if (to !== null) api.moveInQueue(dragFrom!, to);
		dragFrom = null;
		dropAt = null;
	}

	// One list in play order, autoplay's continuation under its own divider (`queue.ts`).
	const view = $derived(queueView(playback.queue));
	// The tail of the queue, for the one drop position no row can mark from its own top edge.
	const lastIndex = $derived((view.autoplay.at(-1) ?? view.rows.at(-1))?.i ?? -1);

	// Playing a playlist queues the whole playlist, so this list can be handed five figures of rows
	// the moment it opens, at roughly 165 KB of web-process memory each (`rows.ts`). Past a couple
	// of hundred it renders only what is near the viewport, and drops the reorder animation (flip
	// measures against the viewport, so it would fight the scroll).
	const WINDOW_ABOVE = 200;
	const sc = rowScroller();
	// `blockWindows` charges each block a heading. Only the autoplay block draws one; the extra
	// 40px on the first shifts which slice is picked, never where a row lands, and the overscan
	// absorbs it.
	const counts = $derived([view.rows.length, view.autoplay.length]);
	const windowed = $derived(playback.queue.items.length > WINDOW_ABOVE);
	const wins = $derived(
		windowed
			? blockWindows(sc.scrollTop, sc.viewportPx, counts, sc.rowPx)
			: counts.map(fullWindow)
	);

	let el: HTMLElement;
	let rowsEl: HTMLElement | undefined = $state();
	/** Where row 0 sits in the scroll content, px. Every row is `sc.rowPx` tall, rendered or not
	 *  (the window pads the rest), so row `i` is at `rowTop() + i * sc.rowPx`. */
	const rowTop = () =>
		rowsEl!.getBoundingClientRect().top - el.getBoundingClientRect().top + el.scrollTop;

	/** The playing row at the top, with what plays next under it. */
	function toPlaying() {
		if (!el?.isConnected || !rowsEl?.isConnected) return;
		el.scrollTop = rowTop() + playback.queue.currentIndex * sc.rowPx - 4;
	}

	onMount(() => {
		toPlaying();
		// Again once the row height is measured and the window has moved onto the playing row.
		let frame = requestAnimationFrame(() => (frame = requestAnimationFrame(toPlaying)));
		return () => cancelAnimationFrame(frame);
	});

	// Follow the play pointer. A track change keeps the playing row where it was on screen, as long
	// as it was on screen (`followPlaying`); a different queue altogether opens on its playing row.
	let at = untrack(() => playback.queue.currentIndex);
	let held = untrack(() => playback.queue.items);
	/** The row the user just clicked: it is already under their pointer, so the list stays put. */
	let clicked = -1;
	$effect(() => {
		const { items, currentIndex } = playback.queue;
		untrack(() => {
			const from = at;
			const fromId = held[from]?.video_id;
			at = currentIndex;
			const sameList = items === held;
			held = items;
			const click = clicked === currentIndex;
			clicked = -1;
			if (!el || !rowsEl || click) return;
			if (!sameList && items[currentIndex]?.video_id !== fromId) {
				toPlaying();
				requestAnimationFrame(toPlaying); // the window has to move onto it first
				return;
			}
			const to = followPlaying(el.scrollTop, el.clientHeight, sc.rowPx, rowTop(), from, currentIndex);
			if (to === null) return;
			const smooth = Math.abs(currentIndex - from) === 1 && !reducedMotion.current;
			el.scrollTo({ top: to, behavior: smooth ? 'smooth' : 'instant' });
		});
	});

	function play(i: number) {
		clicked = i;
		api.playIndex(i);
	}
</script>

{#snippet rows(list: QueueRow[], w: RowWindow)}
	<!-- The padding stands in for the rows outside the window, so this block is exactly as tall as
	     all of its rows. -->
	<div role="list" style="padding-top:{w.padTop}px;padding-bottom:{w.padBottom}px">
		{#each list.slice(w.start, w.end) as { item, key, i } (key)}
			<!-- data-row: what the scroller measures a row's real height from. -->
			<div
				data-row
				role="listitem"
				class="relative"
				animate:flip={{ duration: windowed || reducedMotion.current ? 0 : 200, easing: cubicOut }}
				draggable={canDrag(i)}
				ondragstart={(e) => onDragStart(e, i)}
				ondragover={(e) => onDragOver(e, i)}
				ondrop={onDrop}
			>
				<!-- Where the drop lands: a bar across the edge of the row it goes in front of. The
				     last row also draws one below itself, nothing else can show a drop at the end. -->
				{#if dropAt === i}
					<div
						class="pointer-events-none absolute inset-x-2 top-0 z-10 h-0.5 rounded-full bg-primary"
					></div>
				{:else if dropAt === i + 1 && i === lastIndex}
					<div
						class="pointer-events-none absolute inset-x-2 bottom-0 z-10 h-0.5 rounded-full bg-primary"
					></div>
				{/if}
				<TrackRow
					song={item}
					index={i}
					active={i === playback.queue.currentIndex}
					hideRating
					onplay={() => play(i)}
					onAdd={() => openAddToPlaylist(item)}
					onRemove={canEdit && i !== playback.queue.currentIndex
						? () => api.removeFromQueue(i)
						: undefined}
					removeLabel={t('player.remove_from_queue')}
					playlistId={playback.queue.sourceId}
					queueIndex={i}
				/>
			</div>
		{/each}
	</div>
{/snippet}

<!-- A drag cancelled with Esc, or dropped outside the list, never reaches `drop`: without this the
     bar stays painted and the next dragover thinks a drag is still in flight. -->
<svelte:window
	ondragend={() => {
		dragFrom = null;
		dropAt = null;
	}}
/>

<!-- The list on its own, so the side panel, the now-playing view's Queue tab and theater mode
     render the same one instead of drifting apart. -->
{#if view.rows.length}
	<div class="flex shrink-0 items-center gap-3 px-4 pt-3 pb-2">
		<div class="min-w-0 flex-1">
			{#if playback.queue.sourceName}
				<p class="text-xs text-muted-foreground">{t('player.playing_from')}</p>
				<p class="truncate text-sm font-semibold" title={playback.queue.sourceName}>
					{playback.queue.sourceName}
				</p>
			{/if}
		</div>
		{#if canEdit}
			<!-- Right where its tracks are drawn, rather than three levels into Settings. -->
			<label
				class="flex shrink-0 cursor-pointer items-center gap-2 text-xs font-medium text-muted-foreground"
				title={t('settings.playback.autoplay_hint')}
			>
				{t('player.autoplay')}
				<Switch size="sm" checked={prefs.autoplay} onCheckedChange={setAutoplay} />
			</label>
		{/if}
	</div>
	{#if playback.queue.prevTrack || (canEdit && view.queued)}
		<div class="flex shrink-0 items-center gap-2 px-2 pb-1">
			<!-- Clicking a song throws the queue away, so the tracks that were actually just played
			     are in the queue we kept. One line for the whole of it: they are not rows of this
			     queue. The backend only sends a title while the restore is still reachable. -->
			{#if playback.queue.prevTrack}
				<Button
					variant="ghost"
					size="xs"
					class="h-7 min-w-0 shrink cursor-pointer gap-1.5 rounded-md px-2 text-muted-foreground hover:text-foreground"
					onkeydown={(event) => {
						if (event.key === ' ') event.stopPropagation();
					}}
					onclick={() => api.backToPrevious()}
				>
					<HugeiconsIcon icon={ArrowTurnBackwardIcon} class="size-3.5 shrink-0" />
					<span class="truncate">{t('player.back_to', { title: playback.queue.prevTrack })}</span>
				</Button>
			{/if}
			{#if canEdit && view.queued}
				<!-- Only what was added by hand, which always sits right under the playing track. -->
				<Button
					variant="ghost"
					size="xs"
					class="ml-auto h-7 shrink-0 cursor-pointer rounded-md px-2 text-muted-foreground hover:text-foreground"
					onkeydown={(event) => {
						if (event.key === ' ') event.stopPropagation();
					}}
					onclick={() => api.clearQueued()}
				>
					{t('player.clear_queue')}
				</Button>
			{/if}
		</div>
	{/if}
{/if}
<!-- dragScroll: reordering across a queue taller than the panel needs the edges to pull. -->
<div
	class="min-h-0 flex-1 overflow-y-auto px-2 pt-1 pb-2"
	bind:this={el}
	{@attach sc.attach}
	{@attach (node) => dragScroll(node, QUEUE_ROW_MIME)}
>
	{#if view.rows.length}
		<div bind:this={rowsEl}>
			{@render rows(view.rows, wins[0])}
		</div>
		{#if view.autoplay.length}
			<div
				class="mt-2 flex items-center gap-2 border-t px-2 pt-2.5 pb-1.5 text-muted-foreground"
			>
				<HugeiconsIcon icon={InfinityIcon} class="h-3.5 w-3.5" />
				<span class="text-xs font-medium">{t('player.autoplay')}</span>
			</div>
			{@render rows(view.autoplay, wins[1])}
		{/if}
	{:else}
		<p class="p-4 text-sm text-muted-foreground">{t('player.empty_queue')}</p>
	{/if}
</div>
