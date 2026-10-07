// The playing cover as light for the room it is shown in: its own colour, and a blurred wash of it.
// Theater mode and the player view paint both as their backdrop, and share the bakes below, so
// opening one from the other costs nothing.
//
// PERFORMANCE: the wash is baked ONCE per track into a small canvas and then upscaled by CSS. A live
// `filter: blur()` on a full-screen element is re-run for the damaged region on every repaint,
// which is what made theater mode crawl; a bitmap upscale is something the compositor does for
// free. Depth comes from radial gradients, which are painted fills and cost nothing.
import { artworkAccent } from './artcolor';
import { hexToHsv } from './color';
import { playback } from './player.svelte';
import { thumb } from './thumb';

// Same CORS story as artcolor: googleusercontent and ytimg send `access-control-allow-origin: *`,
// and a host that doesn't taints the canvas, throws on toDataURL, and lands in the same `null`.
// Bounded: insertion-order eviction, the house pattern from `pagecache.ts`. A long listening
// session is exactly the case that used to keep one base64 PNG (and the CSS image resource WebKit
// decodes from it) per cover for the life of the view.
const MAX_WASHES = 8;
const washes = new Map<string, string>();

// 160px square, blurred at 28px. The size matters in both directions: too small (an earlier
// version used 48) and the upscale to a 1080p-plus screen is 40x, where bilinear interpolation
// draws its own diamond pattern over whatever the cover was; too big and the one-time blur
// starts to cost something. At 160 the source is already smooth, so the upscale has nothing to
// invent. Saturation goes up in the same pass because a heavy blur averages colour away.
// PNG, not JPEG: at this size lossless costs a few KB, and JPEG's 8x8 blocks on a 160px buffer
// arrive on screen as 8x8 *tiles* once CSS has stretched them across the window.
const WASH = 160;
const WASH_BLUR = 28;
async function bake(url: string): Promise<string | null> {
	try {
		const img = new Image();
		img.crossOrigin = 'anonymous';
		img.src = url;
		await img.decode();
		const canvas = document.createElement('canvas');
		canvas.width = canvas.height = WASH;
		const ctx = canvas.getContext('2d');
		if (!ctx) return null;
		ctx.imageSmoothingQuality = 'high';
		// Overdrawn past every edge by more than the blur radius. Without it the blur samples the
		// transparent pixels outside the drawing and leaves a dark frame all the way round, which
		// the upscale then turns into a vignette nobody asked for.
		const over = WASH_BLUR * 1.6;
		if (typeof ctx.filter === 'string') {
			ctx.filter = `blur(${WASH_BLUR}px) saturate(1.5)`;
			ctx.drawImage(img, -over, -over, WASH + over * 2, WASH + over * 2);
		} else {
			// No canvas filters: downscale hard and let the upscale do the smoothing instead.
			// Rougher, but it is a dimmed wash behind a mesh, not the subject.
			const small = document.createElement('canvas');
			small.width = small.height = 20;
			small.getContext('2d')?.drawImage(img, 0, 0, 20, 20);
			ctx.drawImage(small, -over, -over, WASH + over * 2, WASH + over * 2);
		}
		return canvas.toDataURL('image/png');
	} catch {
		return null; // offline, 404, throttled, tainted: the mesh is the backdrop on its own
	}
}

/** Follows the playing track for as long as the calling component lives (call it during init).
 *  While `on` is false nothing is read or baked, and the room is the theme's own hue. */
export function artRoom(on: () => boolean = () => true) {
	// The cover's own colour, read here directly rather than through the "adapt colors to artwork"
	// setting: that setting repaints the whole app, this is one view lit by one album.
	let accent = $state<string | null>(null);
	$effect(() => {
		const url = on() ? thumb(playback.now?.thumbnail, 120) : undefined;
		if (!url) {
			accent = null;
			return;
		}
		let alive = true;
		artworkAccent(url).then((hex) => {
			if (alive) accent = hex;
		});
		return () => {
			alive = false;
		};
	});

	let wash = $state<string | null>(null);
	$effect(() => {
		const url = on() ? thumb(playback.now?.thumbnail, 400) : undefined;
		if (!url) {
			wash = null;
			return;
		}
		const hit = washes.get(url);
		if (hit !== undefined) {
			wash = hit;
			return;
		}
		let alive = true;
		bake(url).then((data) => {
			if (!alive || !data) return;
			if (washes.size >= MAX_WASHES && !washes.has(url)) {
				const oldest = washes.keys().next().value;
				if (oldest !== undefined) washes.delete(oldest);
			}
			washes.set(url, data);
			wash = data;
		});
		return () => {
			alive = false;
		};
	});

	// Falls back to the theme's own accent hue, so a greyscale cover (or a cover that hasn't been
	// read yet) still gets a lit room rather than a flat one.
	const hue = $derived(accent ? (hexToHsv(accent)?.h ?? null) : null);
	// Three blobs off one hue: the near-complement keeps it from reading as a single flat tint, and
	// staggered sizes/positions are what make it look lit rather than gradient-filled.
	const mesh = $derived.by(() => {
		const h = hue ?? 265;
		const a = (deg: number) => (h + deg + 360) % 360;
		return [
			`radial-gradient(70% 60% at 12% 18%, hsl(${a(0)} 72% 48% / 0.34), transparent 68%)`,
			`radial-gradient(60% 55% at 88% 82%, hsl(${a(38)} 70% 45% / 0.28), transparent 68%)`,
			`radial-gradient(55% 50% at 72% 8%, hsl(${a(-46)} 65% 52% / 0.2), transparent 70%)`
		].join(',');
	});
	// The light the cover sits in. A radial gradient, deliberately: this is the shape a big soft
	// box-shadow would draw, at no filter cost.
	const glow = $derived(
		`radial-gradient(closest-side, hsl(${hue ?? 265} 80% 55% / 0.5), transparent)`
	);

	return {
		/** The baked wash as a data URL, or null until it is ready (or if the cover can't be read). */
		get wash() {
			return wash;
		},
		get mesh() {
			return mesh;
		},
		get glow() {
			return glow;
		}
	};
}
