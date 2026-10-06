<script lang="ts">
	// The Downloads manager: what the backend is fetching, and the finished library to play
	// offline. The list is the shared store (hydrated from `downloads_list()`, kept live by
	// `downloads-changed` / `download-progress`), so a row paused here changes everywhere.
	import { HugeiconsIcon } from '@hugeicons/svelte';
	import {
		ArrowRight01Icon,
		Delete02Icon,
		Download01Icon,
		FileImportIcon,
		HardDriveIcon,
		MusicNote01Icon,
		PlayIcon,
		RefreshIcon,
		Sorting01Icon
	} from '@hugeicons/core-free-icons';
	import { open } from '@tauri-apps/plugin-dialog';
	import { goto } from '$app/navigation';
	import { Button } from '$lib/components/ui/button';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import * as AlertDialog from '$lib/components/ui/alert-dialog';
	import * as Dialog from '$lib/components/ui/dialog';
	import * as RadioGroup from '$lib/components/ui/radio-group';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import DownloadRow from '$lib/components/DownloadRow.svelte';
	import * as api from '$lib/api';
	import {
		downloads,
		refreshDownloads,
		previewDownloadCollectionRemoval,
		removeDownloadCollection,
		removeDownloads,
		retryDownloads
	} from '$lib/downloads.svelte';
	import {
		collectionDownloads,
		collectionSubtitle,
		downloadItemToSong,
		formatBytes,
		playableCollectionSongs,
		retryableDownloads,
		sortDone,
		splitDownloads,
		storedBytes,
		summarize,
		type DownloadItem,
		type DownloadSort
	} from '$lib/downloads';
	import { addLocalFolder, local, openPlayer, toast } from '$lib/player.svelte';
	import { t } from '$lib/i18n.svelte';
	import { thumb } from '$lib/thumb';
	import { anchorMenu, fitMenu, NO_ANCHOR } from '$lib/menu';
	import type { DownloadCollection } from '$lib/downloads';

	const sum = $derived(summarize(downloads.items));
	const parts = $derived(splitDownloads(downloads.items));
	// Covers for the header stack, from the finished files; same idea as the History page, because
	// the page has no artwork of its own.
	const covers = $derived([
		...new Set(parts.done.slice(0, 40).flatMap((d) => (d.thumbnail ? [d.thumbnail] : [])))
	]);
	let artFailed = $state(false);
	let failedCollectionArtwork = $state<Record<string, boolean>>({});
	$effect(() => {
		covers[0]; // re-arm when the artwork changes
		artFailed = false;
	});

	// The finished library's order: "recently downloaded" (newest activity first, the section's
	// own order) or by file size, biggest first — a client-side sort over the payload, so the
	// section count never changes with it.
	let doneSort = $state<DownloadSort>('recent');
	let sortOpen = $state(false);
	let sortAnchor = $state(NO_ANCHOR);
	const sortLabel = $derived(
		doneSort === 'size' ? t('downloads.sort_size') : t('downloads.sort_recent')
	);
	function openSort(e: MouseEvent) {
		sortAnchor = anchorMenu(e, { align: 'right' });
		sortOpen = true;
	}

	// The finished library plays as one queue, in the order it is shown: a click resumes from that
	// row, "Play all" starts at the top.
	const shownDone = $derived(sortDone(parts.done, doneSort));
	const doneSongs = $derived(shownDone.map(downloadItemToSong));
	const subtitle = $derived(
		sum.total === 0
			? t('downloads.subtitle')
			: sum.unfinished > 0
				? t('downloads.summary', { active: sum.unfinished, done: sum.done })
				: t('downloads.downloaded_count', { count: sum.done })
	);
	// The header's two controls over the whole list: how much room the feature is taking, and the
	// one button that puts every stopped row back in flight (per-row retry stays on the rows).
	const stored = $derived(storedBytes(downloads.items));
	const retryable = $derived(retryableDownloads(downloads.items));

	/** One flag for the bulk calls: the rows are individually flagged too, this covers the buttons. */
	let bulkBusy = $state(false);

	/**
	 * Import music from a folder on this device: the same scan the Local tab runs indexes it, so
	 * its tracks come back here as ordinary rows — the picker is the only new surface.
	 */
	let importing = $state(false);
	async function importFromDevice() {
		if (importing) return;
		importing = true;
		try {
			const picked = await open({ title: t('downloads.import_dialog'), directory: true });
			if (typeof picked !== 'string') return;
			await addLocalFolder(picked);
			await refreshDownloads();
			if (local.error) toast.error(local.error);
		} catch (e) {
			toast.error(String(e));
		} finally {
			importing = false;
		}
	}

	async function retryFailed() {
		if (bulkBusy) return;
		bulkBusy = true;
		try {
			await retryDownloads(retryable.map((d) => d.id));
		} finally {
			bulkBusy = false;
		}
	}

	// Removing every finished download deletes every finished file, so it gets the same two-step
	// the single row uses, with the count spelled out before anything is touched.
	let confirmClear = $state(false);
	async function clearFinished() {
		if (bulkBusy) return;
		bulkBusy = true;
		try {
			if (await removeDownloads(parts.done.map((d) => d.id))) confirmClear = false;
		} finally {
			bulkBusy = false;
		}
	}

	let collectionToOpen = $state<DownloadCollection | null>(null);
	const activeCollection = $derived.by(() => {
		const selected = collectionToOpen;
		if (!selected) return null;
		return downloads.collections.find((collection) => collection.id === selected.id) ?? selected;
	});
	const collectionItems = $derived(
		activeCollection ? collectionDownloads(activeCollection, downloads.items) : []
	);
	function openCollection(collection: DownloadCollection) {
		collectionToOpen = collection;
	}

	let collectionToRemove = $state<DownloadCollection | null>(null);
	let confirmCollectionRemoval = $state(false);
	let collectionBusy = $state(false);
	let collectionPreviewBusy = $state(false);
	let collectionRemovalCount = $state<number | null>(null);
	let collectionPreviewRequest = 0;
	async function requestCollectionRemoval(collection: DownloadCollection) {
		if (collectionBusy || collectionPreviewBusy) return;
		const request = ++collectionPreviewRequest;
		collectionToRemove = collection;
		collectionRemovalCount = null;
		collectionPreviewBusy = true;
		try {
			const preview = await previewDownloadCollectionRemoval(collection.id);
			if (request !== collectionPreviewRequest || collectionToRemove?.id !== collection.id) return;
			collectionRemovalCount = preview?.delete_count ?? null;
			confirmCollectionRemoval = true;
		} finally {
			if (request === collectionPreviewRequest) collectionPreviewBusy = false;
		}
	}
	async function removeCollection() {
		const collection = collectionToRemove;
		if (!collection || collectionBusy) return;
		collectionBusy = true;
		try {
			if (await removeDownloadCollection(collection.id)) {
				confirmCollectionRemoval = false;
				collectionToRemove = null;
				collectionRemovalCount = null;
			}
		} finally {
			collectionBusy = false;
		}
	}

	// The finished library plays as one queue, in the order it is shown: a click resumes from that
	// row, "Play all" starts at the top.
	function play(row?: DownloadItem) {
		if (!doneSongs.length) return;
		const at = row ? shownDone.findIndex((d) => d.id === row.id) : 0;
		openPlayer();
		api.playPlaylist(doneSongs, at >= 0 ? at : 0, undefined, t('downloads.library_title'));
	}

	/**
	 * One collection's Play: only that collection's downloaded members, in the collection's own
	 * order — the top "Play all" stays the whole-library queue. An empty member list (nothing
	 * downloaded yet) is a no-op, and the button is disabled with a readable title instead.
	 */
	function playCollection(collection: DownloadCollection) {
		const songs = playableCollectionSongs(collection, downloads.items);
		if (!songs.length) return;
		openPlayer();
		api.playPlaylist(songs, 0, undefined, collection.title);
	}
</script>

<div class="p-6">
	<!-- The same rounded band the History page wears. -->
	<div class="relative mb-6 overflow-hidden rounded-2xl border">
		<div class="art-wash pointer-events-none absolute inset-0 overflow-hidden">
			{#if covers[0] && !artFailed}
				<img
					src={thumb(covers[0], 96)}
					alt=""
					class="absolute inset-0 h-full w-full scale-110 object-cover opacity-60 blur-2xl"
					onerror={() => (artFailed = true)}
				/>
			{/if}
			<div
				class="absolute inset-0 bg-gradient-to-r from-background via-background/80 to-background/40"
			></div>
		</div>
		<div class="relative flex flex-wrap items-center gap-4 p-4">
			{#if covers.length}
				<div class="flex shrink-0 items-center pl-1">
					{#each covers.slice(0, 5) as cover, i (cover)}
						<img
							src={thumb(cover, 400)}
							alt=""
							style="z-index:{5 - i}"
							class="relative -ml-5 h-20 w-20 rounded-xl object-cover shadow-lg ring-2 ring-background first:ml-0"
						/>
					{/each}
				</div>
			{:else}
				<div
					class="flex h-20 w-20 shrink-0 items-center justify-center rounded-xl bg-primary/10 text-primary"
				>
					<HugeiconsIcon icon={Download01Icon} class="h-8 w-8" />
				</div>
			{/if}
			<div class="min-w-0 flex-1">
				<h1 class="font-heading text-2xl font-bold tracking-tight">{t('downloads.title')}</h1>
				<p class="mt-0.5 text-sm text-muted-foreground">{subtitle}</p>
				<div class="mt-3 flex flex-wrap items-center gap-2">
					<Button
						class="gap-2 rounded-full"
						disabled={!parts.done.length}
						onclick={() => play()}
					>
						<HugeiconsIcon icon={PlayIcon} class="h-4 w-4" /> {t('downloads.play_all')}
					</Button>
					<!-- Music from this device joins the same list: pick a folder, the local scan
					     indexes it, and its tracks land below as ordinary finished rows. -->
					<Button
						variant="outline"
						class="gap-2 rounded-full"
						disabled={importing}
						onclick={importFromDevice}
					>
						<HugeiconsIcon icon={FileImportIcon} class="h-4 w-4" /> {t('downloads.action_import')}
					</Button>
					<!-- Bulk retry: only appears once something has actually stopped, and the count in
					     the label is the number of rows it will take. -->
					{#if retryable.length}
						<Button
							variant="outline"
							class="gap-2 rounded-full"
							disabled={bulkBusy}
							onclick={retryFailed}
						>
							<HugeiconsIcon icon={RefreshIcon} class="h-4 w-4" />
							{t('common.retry')} ({retryable.length})
						</Button>
					{/if}
					<!-- What the feature holds on disk, over every state: a finished file at full
					     size, a partial at however far it got. -->
					{#if downloads.items.length}
						<span
							data-storage
							class="inline-flex shrink-0 items-center gap-1.5 rounded-full border bg-muted/40 px-2.5 py-1 text-xs tabular-nums text-muted-foreground"
							title={t('downloads.storage_used', { bytes: formatBytes(stored) })}
						>
							<HugeiconsIcon icon={HardDriveIcon} class="h-3.5 w-3.5" />
							{formatBytes(stored)}
						</span>
					{/if}
				</div>
			</div>
		</div>
	</div>

	{#if !downloads.loaded}
		{#each Array(4) as _, i (i)}
			<Skeleton class="mb-1 h-14 w-full rounded-lg" />
		{/each}
	{:else if downloads.error && !downloads.items.length}
		<ErrorState message={downloads.error} onRetry={refreshDownloads} />
	{:else if !parts.queue.length && !parts.done.length && !downloads.collections.length}
		<div class="rounded-2xl border bg-card/40 p-6">
			<h2 class="font-heading text-lg font-bold tracking-tight">{t('downloads.empty_title')}</h2>
			<p class="mt-1 max-w-prose text-sm text-muted-foreground">{t('downloads.empty_hint')}</p>
			<Button variant="outline" class="mt-4 gap-2" onclick={() => goto('/library')}>
				{t('downloads.browse')}
			</Button>
		</div>
	{:else}
		{#if downloads.collections.length}
			<section class="mb-6">
				<h2 class="mb-1 flex items-baseline gap-3 py-2">
					<span class="font-heading text-lg font-bold tracking-tight">{t('downloads.collections_title')}</span>
					<span class="h-px flex-1 bg-border"></span>
					<span class="self-center text-xs text-muted-foreground">{downloads.collections.length}</span>
				</h2>
				{#each downloads.collections as collection (collection.id)}
					<div
						class="group flex items-center gap-3 rounded-lg p-2 hover:bg-accent/10"
						data-download-collection={collection.id}
					>
						{#if collection.artwork_path && !failedCollectionArtwork[collection.id]}
							<img
								src={thumb(collection.artwork_path, 96)}
								alt=""
								class="h-14 w-14 shrink-0 rounded-lg object-cover"
								loading="lazy"
								onerror={() => (failedCollectionArtwork[collection.id] = true)}
							/>
						{:else}
							<div class="flex h-14 w-14 shrink-0 items-center justify-center rounded-lg bg-muted text-muted-foreground/50">
								<HugeiconsIcon icon={MusicNote01Icon} class="h-5 w-5" />
							</div>
						{/if}
						<div class="min-w-0 flex-1">
							<div class="flex min-w-0 items-center gap-2">
								<span class="min-w-0 truncate text-sm font-medium">{collection.title}</span>
								<span class="shrink-0 rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium text-muted-foreground">
									{collection.kind === 'album' ? t('downloads.collection_album') : t('downloads.collection_playlist')}
								</span>
							</div>
							<div class="mt-0.5 flex min-w-0 items-center gap-x-2 text-xs text-muted-foreground">
								{#if collectionSubtitle(collection.subtitle, collection.track_count)}
									<span class="min-w-0 truncate"
										>{collectionSubtitle(collection.subtitle, collection.track_count)}</span
									>
									<span aria-hidden="true" class="text-muted-foreground/60">•</span>
								{/if}
								<span class="shrink-0 tabular-nums">
									{t('downloads.collection_progress', {
										downloaded: collection.downloaded_count,
										total: collection.track_count
									})}
								</span>
								<!-- The collection's own total, summed by the backend over its finished
								     members — one number, not a loop over every item in the UI. -->
								<span class="shrink-0 tabular-nums" data-collection-size={collection.id}>
									{formatBytes(collection.size_bytes)}
								</span>
							</div>
							</div>
							<div class="flex shrink-0 items-center gap-0.5">
							<!-- Only this collection's downloaded members, in collection order. Disabled
							     (with a readable title) until something in it has finished downloading. -->
							<button
								class="cursor-pointer rounded-md p-1.5 text-muted-foreground hover:bg-accent/20 hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
								title={playableCollectionSongs(collection, downloads.items).length
									? t('downloads.play_collection', { title: collection.title })
									: t('downloads.play_collection_none')}
								aria-label={playableCollectionSongs(collection, downloads.items).length
									? t('downloads.play_collection', { title: collection.title })
									: t('downloads.play_collection_none')}
								disabled={!playableCollectionSongs(collection, downloads.items).length}
								onclick={() => playCollection(collection)}
							>
								<HugeiconsIcon icon={PlayIcon} class="h-4 w-4" />
							</button>
							<button
								class="cursor-pointer rounded-md p-1.5 text-muted-foreground hover:bg-accent/20 hover:text-foreground"
								title={t('downloads.open_collection', { title: collection.title })}
								aria-label={t('downloads.open_collection', { title: collection.title })}
								onclick={() => openCollection(collection)}
							>
								<HugeiconsIcon icon={ArrowRight01Icon} class="h-4 w-4" />
							</button>
							<button
								class="cursor-pointer rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive disabled:opacity-50"
								title={t('downloads.action_remove_collection')}
								aria-label={t('downloads.action_remove_collection')}
								disabled={collectionBusy || collectionPreviewBusy}
								onclick={() => requestCollectionRemoval(collection)}
							>
								<HugeiconsIcon icon={Delete02Icon} class="h-4 w-4" />
							</button>
						</div>
					</div>
				{/each}
			</section>
		{/if}
		{#if parts.queue.length}
			<section class="mb-6">
				<h2 class="mb-1 flex items-baseline gap-3 py-2">
					<span class="font-heading text-lg font-bold tracking-tight">{t('downloads.queue_title')}</span>
					<span class="h-px flex-1 bg-border"></span>
					<span class="self-center text-xs text-muted-foreground">{parts.queue.length}</span>
				</h2>
				{#each parts.queue as item (item.id)}
					<DownloadRow {item} />
				{/each}
			</section>
		{/if}
		{#if parts.done.length}
			<section>
				<h2 class="mb-1 flex items-baseline gap-3 py-2">
					<span class="font-heading text-lg font-bold tracking-tight">{t('downloads.library_title')}</span>
					<span class="h-px flex-1 bg-border"></span>
					<span class="self-center text-xs text-muted-foreground">{parts.done.length}</span>
					<!-- The section's order: the playlist page's sort pattern, client-side over the
					     payload — "recently downloaded" (default) or by file size, biggest first.
					     The count above is the section's, so it never moves with the sort. -->
					<button
						class="-my-1 cursor-pointer self-center rounded-md px-2 py-1 text-xs font-medium text-muted-foreground hover:bg-accent/20 hover:text-foreground"
						title={t('downloads.sort_recent')}
						aria-label={t('downloads.sort_recent')}
						aria-haspopup="menu"
						aria-expanded={sortOpen}
						onclick={openSort}
					>
						<span class="inline-flex items-center gap-1.5">
							<HugeiconsIcon icon={Sorting01Icon} class="h-3.5 w-3.5" />
							{sortLabel}
						</span>
					</button>
					<!-- The finished library in one action, parked at the end of its own heading:
					     it deletes files, so it confirms first (the dialog is at the bottom). -->
					<button
						class="-my-1 cursor-pointer self-center rounded-md p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive disabled:opacity-50"
						title={t('downloads.action_clear_finished')}
						aria-label={t('downloads.action_clear_finished')}
						disabled={bulkBusy || !parts.done.length}
						onclick={() => (confirmClear = true)}
					>
						<HugeiconsIcon icon={Delete02Icon} class="h-4 w-4" />
					</button>
				</h2>
				{#each shownDone as item (item.id)}
					<DownloadRow {item} onplay={play} />
				{/each}
			</section>
		{/if}
	{/if}
</div>

{#if sortOpen}
	<button
		class="fixed inset-0 z-40 cursor-default"
		onclick={() => (sortOpen = false)}
		aria-label={t('a11y.close_menu')}
	></button>
	<div
		class="fixed z-50 min-w-44 animate-in rounded-lg border bg-popover p-1 text-popover-foreground shadow-xl duration-150 fade-in-0 zoom-in-95"
		style={sortAnchor.style}
		{@attach fitMenu(sortAnchor)}
	>
		<RadioGroup.Root
			value={doneSort}
			onValueChange={(v) => {
				doneSort = v as DownloadSort;
				sortOpen = false;
			}}
			class="gap-0"
		>
			<label
				class="flex w-full cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent/10"
			>
				<RadioGroup.Item value="recent" />
				{t('downloads.sort_recent')}
			</label>
			<label
				class="flex w-full cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent/10"
			>
				<RadioGroup.Item value="size" />
				{t('downloads.sort_size')}
			</label>
		</RadioGroup.Root>
	</div>
{/if}

<Dialog.Root open={!!collectionToOpen} onOpenChange={(open) => !open && (collectionToOpen = null)}>
	<Dialog.Content
		data-collection-detail
		class="flex max-h-[85vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-3xl"
	>
		<Dialog.Header class="shrink-0 border-b px-6 py-5 pr-14">
			<Dialog.Title>{activeCollection?.title}</Dialog.Title>
			<Dialog.Description class="flex flex-wrap items-center gap-2">
				{#if activeCollection}
					<span class="rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium text-muted-foreground">
						{activeCollection.kind === 'album' ? t('downloads.collection_album') : t('downloads.collection_playlist')}
					</span>
					<span class="tabular-nums">
						{t('downloads.collection_progress', {
							downloaded: activeCollection.downloaded_count,
							total: activeCollection.track_count
						})}
					</span>
					{#if activeCollection && collectionSubtitle(activeCollection.subtitle, activeCollection.track_count)}
						<span aria-hidden="true" class="text-muted-foreground/60">•</span>
						<span class="max-w-full truncate"
							>{collectionSubtitle(activeCollection.subtitle, activeCollection.track_count)}</span
						>
					{/if}
					{#if activeCollection}
						<span aria-hidden="true" class="text-muted-foreground/60">•</span>
						<span class="tabular-nums shrink-0">{formatBytes(activeCollection.size_bytes)}</span>
					{/if}
				{/if}
			</Dialog.Description>
		</Dialog.Header>
		<div class="min-h-0 overflow-y-auto px-4 py-3">
			{#if collectionItems.length}
				{#each collectionItems as item (item.id)}
					<DownloadRow {item} />
				{/each}
			{:else}
				<p class="px-2 py-8 text-center text-sm text-muted-foreground">{t('common.nothing_here')}</p>
			{/if}
		</div>
	</Dialog.Content>
</Dialog.Root>

<!-- Bulk remove of the finished library: the copy says outright that files leave the machine and
     how many, and — like the single-row dialog — it stays open until the backend has answered.
     `interactOutsideBehavior="close"` makes a click on the backdrop a *cancel* (bits-ui defaults
     an alert dialog to "ignore" outside clicks); the dismiss layer listens on `document`, so the
     click registers wherever the card renders even though an open dialog sets pointer-events:
     none on <body>. Only the Action button below ever confirms. Escape cancels too — bits-ui's
     Escape layer closes the dialog, which is this same `open=false` path. -->
<AlertDialog.Root bind:open={confirmClear}>
	<AlertDialog.Content interactOutsideBehavior="close" data-confirm-card="clear">
		<AlertDialog.Header>
			<AlertDialog.Title>{t('downloads.clear_confirm_title')}</AlertDialog.Title>
			<AlertDialog.Description>
				{t('downloads.clear_confirm_desc', { count: parts.done.length })}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>{t('common.cancel')}</AlertDialog.Cancel>
			<AlertDialog.Action variant="destructive" disabled={bulkBusy} onclick={clearFinished}>
				{t('downloads.action_remove')}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>

<AlertDialog.Root bind:open={confirmCollectionRemoval}>
	<AlertDialog.Content interactOutsideBehavior="close" data-confirm-card="collection">
		<AlertDialog.Header>
			<AlertDialog.Title>{t('downloads.collection_remove_confirm_title')}</AlertDialog.Title>
			<AlertDialog.Description>
				{collectionRemovalCount === null
					? t('downloads.collection_remove_confirm_desc_unavailable')
					: collectionRemovalCount === 0
						? t('downloads.collection_remove_confirm_desc_zero')
						: collectionRemovalCount === 1
							? t('downloads.collection_remove_confirm_desc_one')
							: t('downloads.collection_remove_confirm_desc', {
									count: collectionRemovalCount
								})}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel>{t('common.cancel')}</AlertDialog.Cancel>
			<AlertDialog.Action variant="destructive" disabled={collectionBusy} onclick={removeCollection}>
				{t('downloads.action_remove_collection')}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>