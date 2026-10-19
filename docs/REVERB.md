# Tempo, pitch and reverb

Open **Tempo, Pitch & Reverb** from the player bar's ⋮ menu. Effects apply to the
listening session and reset on app restart. Saved custom presets retain their
settings on this device; select one to reapply it. Changes synchronize between
the main window and Mini Player. Reset restores the default playback settings.

| Control | Range | Default / behavior |
| --- | --- | --- |
| Tempo | 0.5–2× | 1×; preserves pitch. Disabled during shared listening. |
| Pitch | −12 to +12 semitones | 0; independent of Tempo. |
| Reverb | On/off | Off; retains its controls when disabled. |
| Dry | 0–100% | 100%; original song level, independent of Wet. |
| Wet | 0–100% | 25%; added reflections. |
| Decay | 0.1–10 s | 1.1 s nominal RT60; also changes reflection spacing. |
| Pre-delay | 0–200 ms | 0 ms; additional wet-path delay. |
| Damping | 500–20,000 Hz | 8,000 Hz; wet-path low-pass cutoff. |
| Early reflections | 0–100% | 100%; level of the IR's first 80 ms. |

Sliders apply on release or a keyboard step. Dry and Wet are independent levels;
raising Wet does not automatically reduce Dry. A limiter prevents the combined
signal from clipping. In shared listening, presets apply Pitch and Reverb while
preserving their saved Tempo for solo playback.

The player and OS media controls display listening seconds: a four-minute track
lasts five minutes at 0.8×. Seeking converts that time back to source seconds;
lyrics retain the source timeline.

The 0.5× minimum is deliberate; this feature does not retain upstream's 0.25× option.

## Processing and limits

The order is gain → Rubber Band → Reverb → limiter. With audible Reverb,
Rubber Band runs inside the same FFmpeg graph, avoiding mpv's unsupported native
stretch-before-lavfi ordering. Source-clock timestamps and a sample-copying
scaletempo adapter retain mpv's playback timing without stretching twice.
Reverb off or Wet zero uses native Rubber Band. Normal Tempo with Pitch zero
bypasses stretching. Both paths use the R3 short window and shifted formants.
The room uses embedded, reproducible stereo impulse responses; no audio dependency
is added. Settings, per-track gain and rejected updates are handled on both decks.

With stock mpv, pitch/speed processing can produce an audible energy dip between
gapless tracks because Rubber Band is finalized and restarted for each file.
Reverb alone at normal speed and zero pitch bypasses Rubber Band. This feature
does not modify mpv or FFmpeg to address that dependency limitation.

Changing effects or seeking resets the room tail; full tails require audio or
silence after the last note. Unpadded EOF tests check final-note preservation,
not complete reverb-tail playback after the source ends.

## Validation

Player tests cover actual PCM output, every room control, both decks, gain,
rollback, escaped cache paths, live controls, seeking, playback rate and the
original metadata/channel-layout log regressions. UI checks cover saved presets,
including legacy Tempo migration and the shared-listening restriction.
The known stock-mpv boundary-energy regression is ignored in the default suite;
run `cargo test -p player gapless_join_energy_matches_continuous_audio -- --ignored --nocapture`
to reproduce it explicitly when evaluating playback dependencies.
