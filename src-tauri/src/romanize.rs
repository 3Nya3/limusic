//! Romanized lyrics (issue #202): a Latin-script reading under each line of a song in a script the
//! listener may not read.
//!
//! Apple Music's own pronunciation lines come first. Boidu serves Apple's TTML, which carries a
//! human-written, word-timed `<transliteration>`; `lyrics::parse_ttml_aaml` keeps it, and a song
//! that has it is left alone here. Everything else goes through a local engine, offline and
//! deterministic:
//!
//! - **Japanese:** lindera + IPADIC. A dictionary is the only way to read kanji, and the part of
//!   speech is what turns the particles は/へ/を into "wa"/"e"/"o" and decides where words break.
//!   Spelled the way Apple spells it: Hepburn without macrons (`shou`, `ijou`).
//! - **Korean:** Revised Romanization, with the sound changes at syllable boundaries applied, so
//!   `좋아해` reads "joahae" and `한국말` "hangungmal" rather than letter by letter.
//! - **Chinese:** Hanyu Pinyin with tone marks, one syllable per character.
//! - **Cyrillic, Greek, Georgian, Armenian:** `any_ascii`, whose tables are good for alphabets.
//!
//! Deliberately not covered: the Indic scripts (`any_ascii` drops vowels that are pronounced,
//! "zindagi" comes out "imdgi"), Thai (no spaces between words, so one unreadable run per line) and
//! the abjads (Arabic, Hebrew), where a transliteration without the unwritten vowels reads nothing
//! like the song. A wrong reading is worse than none.
//!
//! Runs on every `get_lyrics` answer and is never cached, so a better engine applies to songs that
//! were cached before it.
//!
//! **Word timing (#romaji sweep).** The local engine also propagates the original words' timing
//! into `romanized_words` whenever the source timed the line word by word (`LyricLine.words`).
//! `romanize_pieces` keeps, for every romaji span it emits, the char range of the original line it
//! was built from; `align_words` then hands each span the window of the timed word (or words) it
//! covers. Where one romaji span covers several timed words — or one timed word covers several
//! romaji spans, which is common because Apple's spans and our morpheme grouping disagree — the
//! window is distributed across the span's characters proportionally to the source chars each side
//! covers. That is an approximation and is labelled as one; the boundaries it produces on real
//! Apple data match Apple's own human transliteration spans. Lines with no word timing keep the
//! line-level fallback (`romanized_words` stays `None`).

use std::ops::Range;
use std::sync::LazyLock;

use lindera::{DictionaryKind, DictionaryLoader, Mode, Tokenizer};
use pinyin::ToPinyin;
use wana_kana::ConvertJapanese;

use crate::lyrics::{LyricLine, LyricWord, Lyrics};

/// One romanized span: the text it renders (with the trailing space that separates it from the
/// next span, Apple's convention) and the char range of the original line it was built from.
struct Piece {
    text: String,
    src: Range<usize>,
}

/// Fill `romanized` on every line that needs one. Lines already in Latin script get nothing, so an
/// English chorus in a K-pop song is not printed twice.
///
/// Lines whose source timed the original word by word also get `romanized_words`: the local
/// reading, sliced so each span carries the window of the timed word (or words) it was built
/// from. See `align_words` for where that mapping is exact and where it is approximate.
pub fn fill(lyrics: &mut Lyrics) {
    // Apple's reading or ours, never both in one song: they spell differently, and the mix reads
    // as a bug. Apple leaves its Latin lines out, which is also what this does.
    if lyrics.lines.iter().any(|l| l.romanized.is_some()) {
        return;
    }
    let all: String = lyrics.lines.iter().map(|l| l.text.as_str()).collect();
    let song_japanese = is_japanese(&all);
    for line in &mut lyrics.lines {
        // A Japanese line in a mostly Korean song (a J-version verse) is still Japanese.
        let japanese = song_japanese || line.text.chars().any(is_kana);
        let pieces = romanize_pieces(&line.text, japanese);
        let r: String = pieces.iter().map(|p| p.text.as_str()).collect();
        if r != line.text {
            if line.romanized_words.is_none() {
                line.romanized_words = align_words(line, &pieces);
            }
            line.romanized = Some(r);
        }
    }
}

fn is_kana(c: char) -> bool {
    matches!(c, '\u{3040}'..='\u{30FF}' | '\u{31F0}'..='\u{31FF}')
}

/// Kanji alone cannot tell Japanese from Chinese, kana can, so the whole song decides and a
/// kanji-only line in a Japanese song stays Japanese. Japanese prose runs well over half kana, so a
/// tenth is a safe floor, and it keeps a Chinese song with one stylised の Chinese.
fn is_japanese(text: &str) -> bool {
    let kana = text.chars().filter(|&c| is_kana(c)).count();
    let han = text.chars().filter(|&c| class(c) == Class::Cjk && !is_kana(c)).count();
    kana > 0 && kana * 10 >= han
}

#[derive(Clone, Copy, PartialEq)]
enum Class {
    /// Latin, digits, spaces, anything this does not touch.
    Keep,
    /// Kana and Han. Japanese or Chinese depending on the song.
    Cjk,
    Hangul,
    /// Scripts `any_ascii` transliterates well.
    Alphabet,
    /// CJK and full-width punctuation: `、` → `,`.
    Punct,
}

fn class(c: char) -> Class {
    match c as u32 {
        0x3040..=0x30FA | 0x30FC..=0x30FF | 0x31F0..=0x31FF | 0x3005..=0x3006 => Class::Cjk,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF => Class::Cjk,
        0xAC00..=0xD7A3 => Class::Hangul,
        0x0370..=0x03FF | 0x1F00..=0x1FFF => Class::Alphabet, // Greek
        0x0400..=0x052F => Class::Alphabet,                   // Cyrillic
        0x0530..=0x058F => Class::Alphabet,                   // Armenian
        0x10A0..=0x10FF => Class::Alphabet,                   // Georgian
        0x3000..=0x303F | 0x30FB | 0xFF00..=0xFFEF => Class::Punct,
        _ => Class::Keep,
    }
}

/// The whole line as one string — the pieces' texts concatenated. Kept for tests: it pins the
/// invariant that slicing the line into timed pieces never changes what the reading says.
#[cfg(test)]
fn romanize_line(line: &str, japanese: bool) -> String {
    romanize_pieces(line, japanese).iter().map(|p| p.text.as_str()).collect()
}

/// The romanized line as spans: `Piece::text` concatenates to exactly what `romanize_line`
/// returns (trailing spaces ride the word they follow — the sweep renders those as the word-end
/// margin), and `Piece::src` is the char range of `line` the span was built from. Runs of the
/// same script map to spans whose source ranges tile the line, which is what lets `align_words`
/// line local readings up with a source's timed words.
fn romanize_pieces(line: &str, japanese: bool) -> Vec<Piece> {
    let mut pieces: Vec<Piece> = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let cls = class(chars[i]);
        let start = i;
        while i < chars.len() && class(chars[i]) == cls {
            i += 1;
        }
        let run: String = chars[start..i].iter().collect();
        let run_pieces: Vec<Piece> = match cls {
            Class::Keep => keep_pieces(&run, start),
            Class::Punct | Class::Alphabet => {
                vec![Piece { text: any_ascii::any_ascii(&run), src: start..i }]
            }
            Class::Hangul => vec![Piece { text: korean(&run), src: start..i }],
            Class::Cjk if japanese => japanese_pieces(&run, start),
            Class::Cjk => chinese_pieces(&run, start),
        };
        // A script change with no space in the source ("Baby愛してる", "你好，世界") still needs one
        // in Latin, or the words run together. The seam space rides the piece before it.
        let joins = pieces.last().is_some_and(|p| {
            p.text.chars().last().is_some_and(|c| c.is_alphanumeric() || ",.!?;:".contains(c))
        }) && run_pieces
            .first()
            .and_then(|p| p.text.chars().next())
            .is_some_and(char::is_alphanumeric);
        if joins {
            if let Some(prev) = pieces.last_mut() {
                prev.text.push(' ');
            }
        }
        for p in run_pieces {
            // Whitespace with no word of its own (the space between two runs) rides the piece
            // before it, so the concatenation stays byte-identical to the old whole-line output.
            if !p.text.is_empty() && p.text.chars().all(char::is_whitespace) {
                if let Some(prev) = pieces.last_mut() {
                    prev.text.push_str(&p.text);
                } else {
                    pieces.push(p);
                }
            } else {
                pieces.push(p);
            }
        }
    }
    pieces
}

/// A Latin run split into word pieces. Each piece carries the whitespace that follows it (the
/// sweep's word-end convention); whitespace with no word after it rides the piece before, or
/// stands alone at the line's start. `src` covers exactly the chars the text came from.
fn keep_pieces(run: &str, start: usize) -> Vec<Piece> {
    let mut tokens: Vec<Piece> = Vec::new();
    let mut cur = String::new();
    let mut cur_start = 0usize;
    let mut cur_ws = false;
    for (k, c) in run.chars().enumerate() {
        let is_ws = c.is_whitespace();
        if !cur.is_empty() && is_ws != cur_ws {
            tokens.push(Piece {
                text: std::mem::take(&mut cur),
                src: (start + cur_start)..(start + k),
            });
            cur_start = k;
        }
        if cur.is_empty() {
            cur_start = k;
        }
        cur_ws = is_ws;
        cur.push(c);
    }
    if !cur.is_empty() {
        tokens.push(Piece { text: cur, src: (start + cur_start)..(start + run.chars().count()) });
    }
    let mut out: Vec<Piece> = Vec::new();
    for t in tokens {
        if t.text.chars().all(char::is_whitespace) {
            if let Some(prev) = out.last_mut() {
                prev.text.push_str(&t.text);
            } else {
                out.push(t);
            }
        } else {
            out.push(t);
        }
    }
    out
}

/// One piece per character (the trailing space rides the syllable before it), so a Chinese
/// reading gets the same per-character source tracking Japanese and Korean words have. ponytail:
/// one reading per character, the most common one. A polyphone inside a word takes the wrong
/// reading (了解 "le jiě", not "liǎo jiě"; 音乐 "yīn lè"). A phrase dictionary fixes that; the
/// crates that have one were either buggy on single characters or 20 MB.
fn chinese_pieces(run: &str, start: usize) -> Vec<Piece> {
    let chars: Vec<char> = run.chars().collect();
    let n = chars.len();
    chars
        .iter()
        .enumerate()
        .map(|(k, c)| {
            let mut text =
                c.to_pinyin().map_or_else(|| c.to_string(), |p| p.with_tone().to_owned());
            if k + 1 < n {
                text.push(' ');
            }
            Piece { text, src: (start + k)..(start + k + 1) }
        })
        .collect()
}

// --- Japanese -----------------------------------------------------------------------------------

/// IPADIC, embedded. The dictionary is ~45 MB of read-only data in the binary (~6 MB compressed in
/// the installers), paged in only when a Japanese song is shown.
static TOKENIZER: LazyLock<Option<Tokenizer>> = LazyLock::new(|| {
    DictionaryLoader::load_dictionary_from_kind(DictionaryKind::IPADIC)
        .map(|d| Tokenizer::new(d, None, Mode::Normal))
        .inspect_err(|e| tracing::warn!(error = %e, "romanize: IPADIC failed to load"))
        .ok()
});

struct Morph {
    surface: String,
    pos: String,
    sub: String,
    base: String,
    reading: String,
}

/// One Cjk run → romaji word pieces, each carrying the char range of the run it came from.
/// Word building is unchanged (particles, glue, the 一人 fusion); the only addition is that each
/// output word remembers which morpheme surfaces produced it, so `align_words` can line a local
/// reading up with a source's timed words character by character. The trailing space between
/// romaji words rides the word before it, the sweep's word-end convention.
fn japanese_pieces(run: &str, start: usize) -> Vec<Piece> {
    let run_chars = run.chars().count();
    // Tokenizer missing or its segmentation doesn't tile the run: one piece, no internal
    // boundaries to align on (still perfectly line-alignable from its edges).
    let as_is = |text: String| vec![Piece { text, src: start..(start + run_chars) }];
    let Some(tokenizer) = TOKENIZER.as_ref() else {
        return as_is(run.to_owned());
    };
    let Ok(mut tokens) = tokenizer.tokenize(run) else {
        return as_is(run.to_owned());
    };
    let mut morphs: Vec<Morph> = Vec::with_capacity(tokens.len());
    let mut spans: Vec<Range<usize>> = Vec::with_capacity(tokens.len());
    let mut cursor = 0usize;
    for t in tokens.iter_mut() {
        let surface = t.text.to_owned();
        let d = t.get_details().unwrap_or_default();
        let field = |i: usize| d.get(i).filter(|s| **s != "*").map(|s| s.to_string());
        let len = surface.chars().count();
        morphs.push(Morph {
            // An unknown word has no reading. Kana reads as itself; an unknown kanji stays.
            reading: field(7).unwrap_or_else(|| surface.clone()),
            pos: field(0).unwrap_or_default(),
            sub: field(1).unwrap_or_default(),
            base: field(6).unwrap_or_default(),
            surface,
        });
        spans.push(cursor..(cursor + len));
        cursor += len;
    }
    if cursor != run_chars {
        return as_is(run.to_owned());
    }

    // IPADIC reads a number and its counter apart: 一人 "ichi nin". These two are everywhere in
    // lyrics and are never read that way.
    for i in 1..morphs.len() {
        if morphs[i].surface == "人" && morphs[i - 1].sub == "数" {
            let fused = match morphs[i - 1].surface.as_str() {
                "一" => "ヒトリ",
                "二" => "フタリ",
                _ => continue,
            };
            morphs[i - 1].reading = fused.into();
            morphs[i].reading.clear();
        }
    }

    // Words as Apple writes them: an inflection stays on its stem ("yokatta", "aishiteru"), the
    // copula and particles stand alone ("yume nara ba", "koto o"). Each word also carries the
    // char range of the morphemes that produced it; a morpheme with no reading of its own (the
    // fused 人 of 一人) extends the word it disappeared into rather than dropping out of the map.
    let mut words: Vec<(String, Range<usize>)> = Vec::new();
    let mut pending: Option<Range<usize>> = None;
    let mut prev_copula = false;
    for (m, src) in morphs.iter().zip(&spans) {
        if m.reading.is_empty() {
            match words.last_mut() {
                Some((_, r)) => r.end = r.end.max(src.end),
                None => pending = Some(pending.map_or(src.clone(), |p| p.start..src.end)),
            }
            continue;
        }
        let reading = match (m.pos.as_str(), m.surface.as_str()) {
            ("助詞", "は") => "ワ",
            ("助詞", "へ") => "エ",
            ("助詞", "を") => "オ",
            _ => m.reading.as_str(),
        };
        let copula = m.pos == "助動詞" && matches!(m.base.as_str(), "だ" | "です");
        let glue = (m.pos == "助動詞" && (!copula || prev_copula))
            || (m.sub == "接続助詞" && matches!(m.surface.as_str(), "て" | "で"))
            || (m.sub == "非自立"
                && matches!(m.base.as_str(), "てる" | "でる" | "とく" | "ちゃう" | "じゃう"))
            || m.sub == "接尾";
        match words.last_mut() {
            Some((w, r)) if glue => {
                w.push_str(reading);
                r.end = src.end;
            }
            _ => {
                let range = pending.take().map_or(src.clone(), |p| p.start..src.end);
                words.push((reading.to_owned(), range));
            }
        }
        prev_copula = copula;
    }
    // Converted per word, not per morpheme, so a trailing っ doubles the next consonant.
    let n = words.len();
    words
        .into_iter()
        .enumerate()
        .map(|(i, (w, src))| {
            let mut text = w.to_romaji();
            if i + 1 < n {
                text.push(' ');
            }
            Piece { text, src: (src.start + start)..(src.end + start) }
        })
        .collect()
}

// --- Korean -------------------------------------------------------------------------------------

const INITIAL: [&str; 19] = [
    "g", "kk", "n", "d", "tt", "r", "m", "b", "pp", "s", "ss", "", "j", "jj", "ch", "k", "t", "p",
    "h",
];
const MEDIAL: [&str; 21] = [
    "a", "ae", "ya", "yae", "eo", "e", "yeo", "ye", "o", "wa", "wae", "oe", "yo", "u", "wo", "we",
    "wi", "yu", "eu", "ui", "i",
];

// Final consonant indices (0 = none), in Unicode's order.
const F_G: usize = 1;
const F_NH: usize = 6;
const F_D: usize = 7;
const F_LG: usize = 9;
const F_LH: usize = 15;
const F_B: usize = 17;
const F_J: usize = 22;
const F_T: usize = 25;
const F_H: usize = 27;
// Initial indices.
const I_G: usize = 0;
const I_N: usize = 2;
const I_D: usize = 3;
const I_R: usize = 5;
const I_M: usize = 6;
const I_SILENT: usize = 11;
const I_J: usize = 12;
const I_H: usize = 18;

/// What a final sounds like before a consonant or at the end of a word. `ㄺ` before `ㄱ` is the
/// one final whose sound depends on what follows; it is handled in `link`.
const CODA: [&str; 28] = [
    "", "k", "k", "k", "n", "n", "n", "t", "l", "k", "m", "l", "l", "l", "p", "l", "m", "p", "p",
    "t", "t", "ng", "t", "t", "k", "t", "p", "t",
];

/// A final carried over onto a following vowel: (what stays, what moves). `ㄶ`/`ㅀ`/`ㅎ` lose the
/// `ㅎ` there (않아 "ana", 좋아 "joa").
const LIAISON: [(&str, &str); 28] = [
    ("", ""),
    ("", "g"),
    ("", "kk"),
    ("k", "s"),
    ("", "n"),
    ("n", "j"),
    ("", "n"),
    ("", "d"),
    ("", "r"),
    ("l", "g"),
    ("l", "m"),
    ("l", "b"),
    ("l", "s"),
    ("l", "t"),
    ("l", "p"),
    ("", "r"),
    ("", "m"),
    ("", "b"),
    ("p", "s"),
    ("", "s"),
    ("", "ss"),
    ("ng", ""),
    ("", "j"),
    ("", "ch"),
    ("", "k"),
    ("", "t"),
    ("", "p"),
    ("", ""),
];

/// The sounds at one syllable boundary: the romanized final of the first syllable and the initial
/// of the second, after liaison, aspiration, nasalisation and the ㄹ rules. Tensing is not written
/// in Revised Romanization, so it is not modelled.
fn link(fin: usize, ini: usize, next_medial: usize) -> (&'static str, &'static str) {
    if ini == I_SILENT {
        // ㄷ/ㅌ before 이 palatalise: 같이 "gachi", 굳이 "guji".
        return match fin {
            F_D if next_medial == 20 => ("", "j"),
            F_T if next_medial == 20 => ("", "ch"),
            _ => LIAISON[fin],
        };
    }
    // ㅎ next to ㄱ/ㄷ/ㅈ/ㅂ aspirates it: 이렇게 "ireoke", 막혀 "makyeo".
    match (fin, ini) {
        (F_H | F_NH | F_LH, I_G | I_D | I_J) => {
            let rest = match fin {
                F_NH => "n",
                F_LH => "l",
                _ => "",
            };
            let aspirated = match ini {
                I_G => "k",
                I_D => "t",
                _ => "ch",
            };
            return (rest, aspirated);
        }
        (F_H, I_N) => return ("n", "n"),
        (F_G | F_LG, I_H) => return ("", "k"),
        (F_D, I_H) => return ("", "t"),
        (F_B, I_H) => return ("", "p"),
        (F_J, I_H) => return ("", "ch"),
        _ => {}
    }
    let coda = if fin == F_LG && ini == I_G { "l" } else { CODA[fin] };
    match (coda, ini) {
        ("k", I_N | I_M) => ("ng", INITIAL[ini]),
        ("t", I_N | I_M) => ("n", INITIAL[ini]),
        ("p", I_N | I_M) => ("m", INITIAL[ini]),
        ("n" | "l", I_R) | ("l", I_N) => ("l", "l"),
        ("m" | "ng", I_R) => (coda, "n"),
        ("k", I_R) => ("ng", "n"),
        ("p", I_R) => ("m", "n"),
        ("t", I_R) => ("n", "n"),
        _ => (coda, INITIAL[ini]),
    }
}

/// One run of Hangul syllables, which is one word: the source puts spaces between words, and
/// Revised Romanization applies the sound changes inside a word only.
fn korean(run: &str) -> String {
    let syl: Vec<(usize, usize, usize)> = run
        .chars()
        .map(|c| {
            let s = c as usize - 0xAC00;
            (s / 588, (s % 588) / 28, s % 28)
        })
        .collect();
    let mut out = String::new();
    let mut onset = INITIAL[syl[0].0];
    for (i, &(_, medial, fin)) in syl.iter().enumerate() {
        out.push_str(onset);
        out.push_str(MEDIAL[medial]);
        let (coda, next) = match syl.get(i + 1) {
            Some(&(ini, next_medial, _)) => link(fin, ini, next_medial),
            None => (CODA[fin], ""),
        };
        out.push_str(coda);
        onset = next;
    }
    out
}

/// Nudge a proportional split point to a syllable boundary: romaji syllables end in a vowel, so
/// a cut just after one ("yo|katta", never "y|okatta") reads as words the sweep lights one by
/// one. Only moves within ±3 chars of the proportional point — the timing stays proportional to
/// the source; this changes where the *text* is cut, never a window.
fn snap_to_syllable(text: &str, raw: usize, lo: usize, hi: usize) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let is_syllable_end = |i: usize| -> bool {
        i > 0 && i < chars.len() && matches!(chars[i - 1], 'a' | 'e' | 'i' | 'o' | 'u')
    };
    if is_syllable_end(raw) {
        return raw;
    }
    for d in 1..=3 {
        for cand in [raw + d, raw - d] {
            if cand >= lo && cand <= hi && is_syllable_end(cand) {
                return cand;
            }
        }
    }
    raw
}

/// Word-level timing for the local reading, taken from the line's own timed words.
///
/// The reading's spans and the source's words are two segmentations of the same line, so they are
/// aligned on char ranges: each source word is located in `line.text`, each romaji span knows the
/// char range it was built from, and a span crossing a source word boundary is split at that
/// boundary — each part taking its word's exact window. Where one source word covers several
/// romaji spans (its reading spans more than one of our tokens), its window is distributed across
/// them in proportion to the source chars each covers. Both directions are approximations of the
/// singer's true syllable timing and are labelled as such; neither invents a window the source
/// did not provide. A span over chars no timed word covers (punctuation the source left outside
/// its words, a trailing gap) gets a zero-width window at its neighbour's boundary, so it lights
/// exactly when the original moves on.
///
/// Returns `None` — the renderer keeps its line-level fallback — when the line carries no word
/// timing at all, or when a word's text cannot be located in the line (a source whose word texts
/// do not match its line text; guessing there would misattribute every following word).
fn align_words(line: &LyricLine, pieces: &[Piece]) -> Option<Vec<LyricWord>> {
    let words = line.words.as_deref().filter(|w| !w.is_empty())?;
    let chars: Vec<char> = line.text.chars().collect();

    // 1. Locate each source word in line.text. Word texts carry the whitespace between them, so
    //    an exact match at the cursor is the common case; a forward search covers sources whose
    //    chunking leaves a gap (a syllabus that skips a word). A word found nowhere → give up:
    //    every later word would be attributed to the wrong chars.
    let mut ranges: Vec<Range<usize>> = Vec::with_capacity(words.len());
    let mut cursor = 0usize;
    for w in words {
        let core: Vec<char> = w.text.trim().chars().collect();
        if core.is_empty() {
            ranges.push(cursor..cursor);
            continue;
        }
        while cursor < chars.len() && chars[cursor].is_whitespace() {
            cursor += 1;
        }
        let at = if cursor + core.len() <= chars.len()
            && chars[cursor..cursor + core.len()] == core[..]
        {
            Some(cursor)
        } else {
            (cursor..=chars.len().saturating_sub(core.len()))
                .find(|&p| chars[p..p + core.len()] == core[..])
        }?;
        ranges.push(at..(at + core.len()));
        cursor = at + core.len();
    }

    // 2. Which source word owns each char (chars no word covers stay None).
    let mut owner: Vec<Option<usize>> = vec![None; chars.len()];
    for (i, r) in ranges.iter().enumerate() {
        if r.end > r.start {
            for slot in owner.iter_mut().take(r.end.min(chars.len())).skip(r.start) {
                *slot = Some(i);
            }
        }
    }

    // 3. Split every romaji span at source-word boundaries. The trailing space (the sweep's
    //    word-end marker) stays on the last piece of each span; the body is split proportionally
    //    to the source chars each boundary covers.
    struct Seg {
        text: String,
        word: Option<usize>,
        src_len: usize,
        start_ms: u64,
        end_ms: u64,
    }
    let mut segs: Vec<Seg> = Vec::new();
    for piece in pieces {
        let body_len = piece.text.trim_end().chars().count();
        let trail: String = piece.text.chars().skip(body_len).collect();
        // (src_len, owner) runs across the piece's source range.
        let mut runs: Vec<(usize, Option<usize>)> = Vec::new();
        let end = piece.src.end.min(chars.len());
        for c in piece.src.start.min(chars.len())..end {
            let o = owner[c];
            match runs.last_mut() {
                Some((len, cur)) if *cur == o => *len += 1,
                _ => runs.push((1, o)),
            }
        }
        if runs.is_empty() {
            runs.push((0, None));
        }
        let total: usize = runs.iter().map(|(l, _)| *l).sum();
        let n = runs.len();
        let mut cut = 0usize;
        let mut prev_cut = 0usize;
        for (k, (src_len, word)) in runs.into_iter().enumerate() {
            cut = if k + 1 == n {
                body_len
            } else if total == 0 {
                body_len * (k + 1) / n
            } else {
                // Proportional to the source chars, clamped so every piece keeps ≥1 char when
                // there are enough to go round.
                let raw = body_len * (cut + src_len) / total;
                let lo = prev_cut + 1;
                let hi = body_len.saturating_sub(n - 1 - k).max(lo);
                snap_to_syllable(&piece.text, raw.clamp(lo, hi), lo, hi)
            };
            let text: String = {
                let mut t: String =
                    piece.text.chars().skip(prev_cut).take(cut - prev_cut).collect();
                if k + 1 == n {
                    t.push_str(&trail);
                }
                t
            };
            segs.push(Seg { text, word, src_len, start_ms: 0, end_ms: 0 });
            prev_cut = cut;
        }
    }
    if segs.is_empty() {
        return None;
    }

    // 4. Distribute each source word's window across the spans it covers, proportional to the
    //    source chars (u128 so a u64::MAX end time can't overflow the multiply).
    let mut any_timed = false;
    for (i, w) in words.iter().enumerate() {
        let members: Vec<usize> = (0..segs.len()).filter(|&s| segs[s].word == Some(i)).collect();
        if members.is_empty() {
            continue;
        }
        any_timed = true;
        let lo = w.start_ms.min(w.end_ms);
        // A source that ships u64::MAX as an end time (an unclosed last word) would freeze the
        // sweep — progress never reaches 1 — so clamp it to the line's own end cue when there is
        // one, else a short fixed hold. Never invents timing: it only bounds a broken one.
        let hi = if w.end_ms == u64::MAX {
            line.end_time_ms
                .filter(|e| *e > lo)
                .map_or(lo.saturating_add(3000), |e| e.min(lo.saturating_add(3000)).max(lo))
        } else {
            w.start_ms.max(w.end_ms)
        };
        let total: u128 = members.iter().map(|&s| segs[s].src_len.max(1) as u128).sum();
        let span = hi as u128 - lo as u128;
        let mut cum = 0u128;
        for &s in &members {
            let weight = segs[s].src_len.max(1) as u128;
            let a = lo as u128 + span * cum / total;
            cum += weight;
            let b = lo as u128 + span * cum / total;
            segs[s].start_ms = a as u64;
            segs[s].end_ms = b as u64;
        }
    }
    if !any_timed {
        return None;
    }

    // 5. Spans over chars no word covers: zero-width at the neighbouring boundary — the previous
    //    span's end, else the next span's start. They light exactly when the original moves on.
    let mut prev_end = 0u64;
    for s in segs.iter_mut() {
        if s.start_ms == 0 && s.end_ms == 0 && s.word.is_none() {
            s.start_ms = prev_end;
            s.end_ms = prev_end;
        } else if s.end_ms > 0 || s.word.is_some() {
            prev_end = s.end_ms;
        }
    }
    // A leading untimed span (before any word) borrows the first timed span's start.
    if let Some(first) = segs.iter().find(|s| s.word.is_some()).map(|s| s.start_ms) {
        for s in segs.iter_mut() {
            if s.word.is_none() && s.start_ms == 0 && s.end_ms == 0 {
                s.start_ms = first;
                s.end_ms = first;
            }
        }
    }

    Some(
        segs.into_iter()
            .map(|s| LyricWord { text: s.text, start_ms: s.start_ms, end_ms: s.end_ms })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn korean_applies_the_sound_changes() {
        for (hangul, want) in [
            ("좋아해", "joahae"),
            ("같이", "gachi"),
            ("있고", "itgo"),
            ("싶어", "sipeo"),
            ("않아", "ana"),
            ("읽다", "ikda"),
            ("숨이", "sumi"),
            ("막혀", "makyeo"),
            ("이렇게", "ireoke"),
            ("한국말", "hangungmal"),
            ("몰라", "molla"),
            ("신라", "silla"),
            ("입니다", "imnida"),
            ("있는", "inneun"),
            ("없어", "eopseo"),
            ("괜찮아", "gwaenchana"),
            ("꽃이", "kkochi"),
            ("밖에", "bakke"),
            ("앉아", "anja"),
            ("닭", "dak"),
            ("사랑해요", "saranghaeyo"),
            ("웃었지만", "useotjiman"),
            ("라면", "ramyeon"),
            ("심리", "simni"),
        ] {
            assert_eq!(korean(hangul), want, "{hangul}");
        }
    }

    #[test]
    fn korean_line_keeps_the_english() {
        assert_eq!(romanize_line("말해 줘 Say it back", false), "malhae jwo Say it back");
        assert_eq!(romanize_line("Walk in this 미로", false), "Walk in this miro");
    }

    #[test]
    fn japanese_reads_particles_and_breaks_words() {
        for (ja, want) in [
            ("夢ならばどれほどよかったでしょう", "yume nara ba dorehodo yokatta deshou"),
            ("君の名は", "kimi no na wa"),
            ("未だにあなたのことを夢にみる", "imadani anata no koto o yume ni miru"),
            ("一人で歩いた道", "hitori de aruita michi"),
            ("言えずに隠してた", "iezu ni kakushiteta"),
            ("明日へ", "ashita e"),
        ] {
            assert_eq!(romanize_line(ja, true), want, "{ja}");
        }
    }

    #[test]
    fn mixed_scripts_get_a_space_at_the_seam() {
        assert_eq!(romanize_line("Baby愛してる", true), "Baby aishiteru");
        assert_eq!(romanize_line("你好，世界", false), "nǐ hǎo, shì jiè");
        assert_eq!(romanize_line("僕は、君を", true), "boku wa, kimi o");
    }

    #[test]
    fn the_whole_song_decides_japanese_or_chinese() {
        assert!(is_japanese("東京の空\n東京"));
        assert!(!is_japanese("还记得你说家是唯一的城堡の"));
        assert!(!is_japanese("보고 싶다 Baby"));
    }

    #[test]
    fn fill_skips_latin_lines_and_defers_to_apple() {
        use crate::lyrics::LyricLine;
        let mut l = Lyrics {
            source: "t".into(),
            lines: vec![
                LyricLine::simple(None, "Я тебя люблю".into()),
                LyricLine::simple(None, "Hey".into()),
            ],
            ..Default::default()
        };
        fill(&mut l);
        assert_eq!(l.lines[0].romanized.as_deref(), Some("Ya tebya lyublyu"));
        assert_eq!(l.lines[1].romanized, None);

        l.lines[0].romanized = Some("from apple".into());
        l.lines.push(LyricLine::simple(None, "Привет".into()));
        fill(&mut l);
        assert_eq!(l.lines[2].romanized, None);
    }

    // --- word-timed local readings (#romaji sweep) ---------------------------------------------

    use crate::lyrics::{LyricLine, LyricWord};

    fn timed(text: &str, words: &[(&str, u64, u64)]) -> LyricLine {
        LyricLine {
            time_ms: Some(words.first().map_or(0, |w| w.1)),
            text: text.into(),
            words: Some(
                words
                    .iter()
                    .map(|(t, s, e)| LyricWord { text: (*t).into(), start_ms: *s, end_ms: *e })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// Invariant of every aligned line: the spans' texts concatenate to exactly the reading, in
    /// order, and no window runs backwards.
    fn assert_sane(line: &LyricLine) -> Vec<LyricWord> {
        let spans = line.romanized_words.clone().expect("expected word-timed romaji");
        let joined: String = spans.iter().map(|w| w.text.as_str()).collect();
        assert_eq!(joined, line.romanized.as_deref().unwrap(), "spans must reproduce the reading");
        for w in &spans {
            assert!(w.start_ms <= w.end_ms, "window runs backwards: {w:?}");
        }
        spans
    }

    #[test]
    fn japanese_line_with_timed_words_gets_word_timed_romaji() {
        // Real Apple spans, from the Lemon TTML: 夢|なら|ば|どれ|ほど|よ|かった|で|しょう.
        let line = timed(
            "夢ならばどれほどよかったでしょう",
            &[
                ("夢", 1241, 1635),
                ("なら", 1635, 2152),
                ("ば", 2152, 2651),
                ("どれ", 2651, 3352),
                ("ほど", 3352, 4003),
                ("よ", 4003, 4352),
                ("かった", 4352, 4920),
                ("で", 4920, 5368),
                ("しょう", 5368, 6433),
            ],
        );
        let mut l =
            Lyrics { source: "t".into(), synced: true, lines: vec![line], ..Default::default() };
        fill(&mut l);
        let spans = assert_sane(&l.lines[0]);
        // Where our token and Apple's span agree (夢|なら|ば|どれ|ほど), the window is copied
        // exactly. Where they disagree (よ|かった vs one "yokatta"), the reading is split at the
        // source boundary — snapped to the syllable — and each half takes its word's window.
        let find =
            |t: &str| spans.iter().find(|w| w.text.trim() == t).map(|w| (w.start_ms, w.end_ms));
        assert_eq!(find("yume"), Some((1241, 1635)), "1:1 word keeps its exact window");
        assert_eq!(find("nara"), Some((1635, 2152)));
        assert_eq!(find("ba"), Some((2152, 2651)));
        assert_eq!(find("dore"), Some((2651, 3352)));
        assert_eq!(find("hodo"), Some((3352, 4003)));
        assert_eq!(find("yo"), Some((4003, 4352)), "split at the よ|かった boundary");
        assert_eq!(find("katta"), Some((4352, 4920)));
        assert_eq!(find("de"), Some((4920, 5368)), "split at the で|しょう boundary");
        assert_eq!(find("shou"), Some((5368, 6433)));
        for w in spans.windows(2) {
            assert!(w[0].end_ms <= w[1].start_ms, "windows must not overlap: {w:?}");
        }
    }

    #[test]
    fn one_source_word_spanning_two_romaji_tokens_splits_proportionally() {
        // Apple word boundaries and lindera's disagree both ways; this is the other direction:
        // one timed word ("物を") covers two romaji tokens ("mono o"), so its window is split
        // between them in proportion to the source chars (1:1 here).
        let line = timed(
            "物を忘れられた",
            &[("物を", 1000, 2000), ("忘れ", 2000, 2600), ("られた", 2600, 3400)],
        );
        let mut l =
            Lyrics { source: "t".into(), synced: true, lines: vec![line], ..Default::default() };
        fill(&mut l);
        let spans = assert_sane(&l.lines[0]);
        assert_eq!(
            l.lines[0].romanized.as_deref(),
            Some("mono o wasurerareta"),
            "reading unchanged"
        );
        let mono = spans.iter().find(|w| w.text.trim() == "mono").unwrap();
        let o = spans.iter().find(|w| w.text.trim() == "o").unwrap();
        assert_eq!((mono.start_ms, mono.end_ms), (1000, 1500));
        assert_eq!((o.start_ms, o.end_ms), (1500, 2000));
        // 忘れ|られた splits at the source boundary, snapped to the syllable: wasu|rerareta.
        let wasu = spans.iter().find(|w| w.text.trim() == "wasu").unwrap();
        assert_eq!((wasu.start_ms, wasu.end_ms), (2000, 2600));
    }

    #[test]
    fn mixed_scripts_punctuation_and_whitespace_keep_their_timing() {
        // Latin + Japanese + full-width punctuation, one timed word per kana — the engine glues
        // 愛し て る into "aishiteru", so the reading is split back at the source boundaries.
        let line = timed(
            "Baby愛してる！",
            &[
                ("Baby", 500, 900),
                ("愛", 900, 1100),
                ("し", 1100, 1200),
                ("て", 1200, 1300),
                ("る", 1300, 1500),
                ("！", 1500, 1600),
            ],
        );
        let mut l =
            Lyrics { source: "t".into(), synced: true, lines: vec![line], ..Default::default() };
        fill(&mut l);
        let spans = assert_sane(&l.lines[0]);
        let find =
            |t: &str| spans.iter().find(|w| w.text.trim() == t).map(|w| (w.start_ms, w.end_ms));
        assert_eq!(find("Baby"), Some((500, 900)), "Latin word keeps its window");
        assert_eq!(find("ai"), Some((900, 1100)), "glued token split back at 愛");
        // The full-width ！ is timed too and lights with its own word.
        let last = spans.last().unwrap();
        assert_eq!(last.text.trim(), "!");
        assert_eq!((last.start_ms, last.end_ms), (1500, 1600));
        for w in spans.windows(2) {
            assert!(w[0].end_ms <= w[1].start_ms, "monotone: {w:?}");
        }
    }

    #[test]
    fn wordless_lines_keep_the_line_level_fallback() {
        let line = LyricLine { text: "君の名は".into(), ..Default::default() };
        let pieces = romanize_pieces(&line.text, true);
        assert!(align_words(&line, &pieces).is_none(), "no words → no romanized_words");
        let mut l = Lyrics { lines: vec![line], ..Default::default() };
        fill(&mut l);
        assert_eq!(l.lines[0].romanized.as_deref(), Some("kimi no na wa"));
        assert!(l.lines[0].romanized_words.is_none(), "fallback preserved");
    }

    #[test]
    fn unlocatable_word_text_falls_back_rather_than_guessing() {
        // A source whose word texts don't appear in its own line text: aligning by guesswork
        // would hand every later word the wrong window, so the whole line falls back.
        let mut line = timed("君の名は", &[("君の", 100, 200), ("名前", 200, 300)]);
        line.text = "君の名は".into(); // "名前" is not in the line
        let pieces = romanize_pieces(&line.text, true);
        assert!(align_words(&line, &pieces).is_none());
    }

    #[test]
    fn gaps_and_seek_jumps_stay_inside_source_windows() {
        // eLRC-style words with a gap between them and an unclosed last word (u64::MAX — the
        // parser caps it, but align must bound it too or the sweep freezes at the last word).
        let line =
            timed("東京の空", &[("東京", 1000, 1800), ("の", 2500, 2600), ("空", 2600, u64::MAX)]);
        let mut l =
            Lyrics { source: "t".into(), synced: true, lines: vec![line], ..Default::default() };
        fill(&mut l);
        let spans = assert_sane(&l.lines[0]);
        let find =
            |t: &str| spans.iter().find(|w| w.text.trim() == t).map(|w| (w.start_ms, w.end_ms));
        assert_eq!(find("toukyou"), Some((1000, 1800)), "gap after a word doesn't stretch it");
        // The unclosed last word is bounded (3s hold) instead of running to u64::MAX.
        let sora = find("sora").unwrap();
        assert_eq!(sora.0, 2600);
        assert!(sora.1 > sora.0 && sora.1 < 10_000, "bounded, not u64::MAX: {sora:?}");
        // Seek sweep: windows are static data, so any playhead maps to the same span — mid-gap
        // nothing is active in the romaji line either, matching the original's gap.
        let active = |t: u64| spans.iter().filter(|w| w.start_ms <= t && t < w.end_ms).count();
        for t in [0, 999, 1000, 1400, 1800, 2499, 2500, 2599, 2600, 3000, u64::MAX - 1] {
            let _ = active(t); // must not panic on any position, forward or backward
        }
        assert_eq!(active(2000), 0, "mid-gap: nothing sweeps, like the original");
        assert!(active(1200) >= 1, "inside 東京 something is sweeping");
    }

    #[test]
    fn fill_propagates_word_timing_end_to_end() {
        let line = timed(
            "未だにあなたのことを夢にみる",
            &[
                ("未だ", 6781, 7805),
                ("に", 7805, 8120),
                ("あなた", 8120, 9138),
                ("の", 9138, 9504),
                ("こと", 9504, 9837),
                ("を", 9837, 10354),
                ("夢に", 10354, 11117),
                ("み", 11117, 11295),
                ("る", 11295, 11974),
            ],
        );
        let mut l =
            Lyrics { source: "t".into(), synced: true, lines: vec![line], ..Default::default() };
        fill(&mut l);
        assert_eq!(l.lines[0].romanized.as_deref(), Some("imadani anata no koto o yume ni miru"));
        let spans = assert_sane(&l.lines[0]);
        assert!(!spans.is_empty());
        // Every span's window is contained in the union of the source words' windows.
        let words = l.lines[0].words.as_ref().unwrap();
        let lo = words.iter().map(|w| w.start_ms).min().unwrap();
        let hi = words.iter().map(|w| w.end_ms).max().unwrap();
        for s in &spans {
            assert!(s.start_ms >= lo && s.end_ms <= hi, "span outside source windows: {s:?}");
        }
        // And the first/last spans land exactly on the first/last source word.
        assert_eq!(spans.first().unwrap().start_ms, lo);
        assert_eq!(spans.last().unwrap().end_ms, hi.min(u64::MAX));
    }

    #[test]
    fn apple_readings_are_left_alone() {
        use crate::lyrics::LyricLine;
        let mut l = Lyrics {
            source: "t".into(),
            lines: vec![LyricLine {
                text: "夢なら".into(),
                romanized: Some("yume nara".into()),
                romanized_words: Some(vec![LyricWord {
                    text: "yume ".into(),
                    start_ms: 1,
                    end_ms: 2,
                }]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let before = serde_json::to_string(&l.lines[0].romanized_words).unwrap();
        fill(&mut l);
        let after = serde_json::to_string(&l.lines[0].romanized_words).unwrap();
        assert_eq!(after, before, "Apple's timing untouched");
    }
}
