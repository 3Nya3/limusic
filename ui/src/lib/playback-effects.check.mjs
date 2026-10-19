// Exercise the actual command queue and startup callbacks with controlled IPC completion order.
// This Tauri/Svelte module cannot be imported directly in Node, so evaluate those two source
// sections with state and IPC stubs; no copies of their update logic live in this check.
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import { runInNewContext } from 'node:vm';

function ok(condition, message) {
	if (!condition) throw new Error(message);
}
function deferred() {
	let resolve;
	let reject;
	const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
	return { promise, resolve, reject };
}
const source = readFileSync(new URL('./player.svelte.ts', import.meta.url), 'utf8');
const queueStart = source.indexOf('let effectsUpdate =');
const queueEnd = source.indexOf('// Mute *is* volume', queueStart);
const initStart = source.indexOf('export function initApp(');
ok(queueStart >= 0 && queueEnd > queueStart && initStart >= 0, 'Effect handlers must be found');
const code = stripTypeScriptTypes(
	source.slice(queueStart, queueEnd) + '\nlet started = false;\n' + source.slice(initStart),
	{ mode: 'strip' }
).replaceAll('export function ', 'function ');

function harness() {
	const snapshot = deferred();
	const requests = [];
	const callbacks = {};
	const errors = [];
	const playback = { speed: 1, semitones: 0, reverb: { wet: 25 }, effectsPending: false };
	const api = new Proxy({
		getPlayback: () => snapshot.promise,
		getQueue: () => Promise.resolve({ items: [] }),
		setPlaybackParams: (speed) => {
			const result = deferred();
			requests.push({ speed, result });
			return result.promise;
		}
	}, {
		get(target, key) {
			return target[key] ?? ((callback) => {
				callbacks[key] = callback;
				return Promise.resolve(() => {});
			});
		}
	});
	const context = { api, playback, toast: { error: (e) => errors.push(e) },
		forgetLocal() {}, applyLtState() {}, performance };
	const handlers = runInNewContext(code + '\n({ setTempoPitch, initApp })', context);
	handlers.initApp(true);
	return { ...handlers, playback, snapshot, requests, callbacks, errors };
}

const staleSnapshot = harness();
staleSnapshot.callbacks.onPlaybackParams({ speed: 0.8, semitones: 3, reverb: { wet: 40 } });
staleSnapshot.snapshot.resolve({ speed: 1, semitones: 0, reverb: { wet: 25 } });
await Promise.resolve();
ok(staleSnapshot.playback.speed === 0.8 && staleSnapshot.playback.reverb.wet === 40,
	'An older startup snapshot cannot replace another window\'s effects event');

const roomReset = harness();
const pending = roomReset.setTempoPitch(0.8, 3);
await Promise.resolve();
roomReset.callbacks.onPlaybackParams({ speed: 1, semitones: 3, reverb: { wet: 25 } });
roomReset.requests[0].result.resolve(undefined);
await pending;
ok(roomReset.playback.speed === 1 && !roomReset.playback.effectsPending,
	'A late command confirmation cannot overwrite the backend room reset');

const local = harness();
const first = local.setTempoPitch(0.8, -2);
await Promise.resolve();
local.requests[0].result.resolve(undefined);
await first;
ok(local.playback.speed === 0.8 && local.playback.semitones === -2,
	'Commands still confirm when no event has arrived');
local.snapshot.resolve({ speed: 1, semitones: 0, reverb: { wet: 25 } });
await Promise.resolve();
ok(local.playback.speed === 0.8, 'A completed local request invalidates an older snapshot too');

const serialized = harness();
const a = serialized.setTempoPitch(0.8, 0);
const b = serialized.setTempoPitch(1.2, 0);
await Promise.resolve();
ok(serialized.requests.length === 1, 'Only one effects command is in flight');
serialized.requests[0].result.resolve(undefined);
await a;
await Promise.resolve();
ok(serialized.requests.length === 2 && serialized.playback.effectsPending,
	'Next update starts after the first, keeping the pending state');
serialized.requests[1].result.resolve(undefined);
await b;
ok(serialized.playback.speed === 1.2 && !serialized.playback.effectsPending, 'Last request wins');

const rejected = harness();
const failed = rejected.setTempoPitch(0.8, 0);
await Promise.resolve();
rejected.snapshot.resolve({ speed: 1.2, semitones: 0, reverb: { wet: 25 } });
await Promise.resolve();
rejected.requests[0].result.reject('unavailable');
await failed;
await Promise.resolve();
ok(rejected.playback.speed === 1.2 && !rejected.playback.effectsPending && rejected.errors.length === 1,
	'Rejected effects retain the valid startup snapshot and release the pending flag');

const queuedFailure = harness();
const rejectedFirst = queuedFailure.setTempoPitch(0.8, 0);
await Promise.resolve();
queuedFailure.snapshot.resolve({ speed: 1.2, semitones: 0, reverb: { wet: 25 } });
await Promise.resolve();
const rejectedSecond = queuedFailure.setTempoPitch(0.9, 0);
queuedFailure.requests[0].result.reject('unavailable');
await rejectedFirst;
await Promise.resolve();
queuedFailure.requests[1].result.reject('unavailable');
await rejectedSecond;
await Promise.resolve();
await Promise.resolve();
ok(queuedFailure.playback.speed === 1.2 && !queuedFailure.playback.effectsPending,
	'A snapshot waits for requests queued after it arrived, retaining it when all requests fail');
console.log('ok — effect command and snapshot ordering');
