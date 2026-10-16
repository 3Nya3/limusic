// Self-check for the cover wash's pixel work in `wash.ts`:
//
//     node --experimental-strip-types ui/src/lib/wash.check.ts
//
// An off-by-one in the running sum shifts or darkens the wash, and a wrong peak lets a bright cover
// through at full strength behind the lyrics. Prints "ok" and exits 0, or throws on the first
// broken invariant.
import { blur, saturate } from './wash.ts';

function ok(cond: boolean, what: string): void {
	if (!cond) throw new Error(`FAIL: ${what}`);
}

const N = 32;
const img = (fill: (x: number, y: number) => number) => {
	const px = new Uint8ClampedArray(N * N * 4);
	for (let y = 0; y < N; y++)
		for (let x = 0; x < N; x++) {
			const v = fill(x, y);
			px.set([v, v, v, 255], (y * N + x) * 4);
		}
	return px;
};

// A flat image stays flat, edges included: held edges, not a dark frame.
const flat = img(() => 200);
blur(flat, N, 6);
ok(flat.every((v, i) => v === (i % 4 === 3 ? 255 : 200)), 'flat image unchanged by blur');

// A dot in the middle spreads evenly: mirror pixels match, the centre is no longer the dot.
const dot = img((x, y) => (x === 16 && y === 16 ? 255 : 0));
blur(dot, N, 3);
const at = (x: number, y: number) => dot[(y * N + x) * 4];
ok(at(16, 16) < 255 && at(16, 16) > 0, 'dot spread out');
ok(at(13, 16) === at(19, 16) && at(16, 13) === at(16, 19), 'spread is symmetric');
ok(at(0, 0) === 0, 'far corner untouched');

// Grey stays grey, and the peak is its brightness; a white pixel anywhere is the peak.
const grey = img(() => 128);
ok(Math.abs(saturate(grey, 1.5) - 128 / 255) < 1e-6 && grey[0] === 128, 'grey unchanged, peak 0.5');
const spot = img((x, y) => (x === 3 && y === 9 ? 255 : 20));
ok(saturate(spot, 1.5) > 0.999, 'white pixel is the peak');

// Colour moves away from grey.
const red = new Uint8ClampedArray([180, 90, 90, 255]);
saturate(red, 1.5);
ok(red[0] > 180 && red[1] < 90, 'saturate pushes colour out');

console.log('ok');
