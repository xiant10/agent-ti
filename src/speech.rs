//! agent-pi's `events/speech.ts`: streamed assistant text cut into speakable segments, so TTS
//! starts on the first sentence instead of the full answer.
//!
//! A segment ends at a sentence end: `.!?;` followed by whitespace, CJK `。！？；` anywhere, or a
//! line break. A sentence longer than `max_chars` is cut at its last clause break (`,`, `，`, `、`,
//! `:`, `：`, `;`), else its last space. Segments shorter than `min_chars` wait for more text, so
//! "1." or "OK." does not become a separate utterance. Markdown is stripped from each segment when
//! it is emitted, never from the raw stream, so a `**` split across two deltas is still removed.
use regex::Regex;
use std::sync::LazyLock;

pub struct SpeechSegmenter {
    min_chars: usize,
    max_chars: usize,
    buffer: String,
}
impl Default for SpeechSegmenter {
    fn default() -> Self {
        Self::new(12, 220)
    }
}
impl SpeechSegmenter {
    pub fn new(min_chars: usize, max_chars: usize) -> Self {
        Self {
            min_chars,
            max_chars,
            buffer: String::new(),
        }
    }
    /// Adds streamed text; returns the segments that are now complete.
    pub fn push(&mut self, text: &str) -> Vec<String> {
        self.buffer.push_str(text);
        let mut segments = Vec::new();
        while let Some(end) = self.boundary() {
            let segment = speakable(&self.buffer[..end]);
            self.buffer.drain(..end);
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
        segments
    }
    /// End of the message or a pause such as a tool call: whatever is buffered, if speakable.
    pub fn flush(&mut self) -> Option<String> {
        let segment = speakable(&std::mem::take(&mut self.buffer));
        (!segment.is_empty()).then_some(segment)
    }
    /// Drops buffered text, for an aborted message.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }
    /// Byte index just past the first segment end at or beyond `min_chars`.
    fn boundary(&self) -> Option<usize> {
        let text = &self.buffer;
        let chars: Vec<(usize, char)> = text.char_indices().collect();
        let mut i = 0;
        while i < chars.len() {
            let (at, c) = chars[i];
            let end = match c {
                '.' | '!' | '?' | ';' => chars
                    .get(i + 1)
                    .filter(|(_, next)| next.is_whitespace())
                    .map(|_| at + 1),
                '。' | '！' | '？' | '；' => Some(at + c.len_utf8()),
                '\n' => {
                    while chars.get(i + 1).is_some_and(|(_, next)| *next == '\n') {
                        i += 1;
                    }
                    Some(chars[i].0 + 1)
                }
                _ => None,
            };
            if let Some(end) = end
                && speakable(&text[..end]).chars().count() >= self.min_chars
            {
                return Some(end);
            }
            i += 1;
        }
        if chars.len() <= self.max_chars {
            return None;
        }
        let window = &chars[..self.max_chars];
        // A clause break reads more naturally than any space, so it wins even when a space comes later.
        let clause = window
            .iter()
            .rposition(|(_, c)| matches!(c, ',' | '，' | '、' | ':' | '：' | ';'));
        if let Some(n) = clause.filter(|n| *n > self.min_chars) {
            return Some(window[n].0 + window[n].1.len_utf8());
        }
        let space = window.iter().rposition(|(_, c)| *c == ' ');
        Some(match space.filter(|n| *n > self.min_chars) {
            Some(n) => window[n].0 + 1,
            None => chars[self.max_chars].0,
        })
    }
}

macro_rules! re {
    ($name:ident, $pattern:expr) => {
        static $name: LazyLock<Regex> = LazyLock::new(|| Regex::new($pattern).unwrap());
    };
}
re!(CODE_BLOCK, r"(?s)```.*?```");
re!(IMAGE, r"!\[([^\]]*)\]\([^)]*\)");
re!(LINK, r"\[([^\]]+)\]\([^)]*\)");
re!(HEADING, r"(?m)^\s{0,3}#{1,6}\s+");
re!(BULLET, r"(?m)^\s*(?:[-*+]|\d+[.)])\s+");
re!(QUOTE, r"(?m)^\s*>\s?");
re!(MARKS, r"(\*\*|__|\*|~~|`)");
// `_emphasis_`, but not the underscore of `customer_id`. Rust's regex has no lookahead, so the
// character after is captured and put back, and the replacement repeats for adjacent emphasis.
re!(UNDERSCORE, r"(^|[^\p{L}\p{N}])_([^_]+)_([^\p{L}\p{N}]|$)");
re!(SPACE, r"\s+");

/// Text as it should be spoken: no markdown emphasis, code ticks, headings, bullets, link targets,
/// or table pipes.
pub fn speakable(text: &str) -> String {
    let text = CODE_BLOCK.replace_all(text, " ");
    let text = IMAGE.replace_all(&text, "$1");
    let text = LINK.replace_all(&text, "$1");
    let text = HEADING.replace_all(&text, "");
    let text = BULLET.replace_all(&text, "");
    let text = QUOTE.replace_all(&text, "");
    let mut text = MARKS.replace_all(&text, "").into_owned();
    loop {
        let next = UNDERSCORE.replace_all(&text, "$1$2$3").into_owned();
        if next == text {
            break;
        }
        text = next;
    }
    let text = text.replace('|', " ");
    SPACE.replace_all(&text, " ").trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_whole_sentences_across_arbitrary_delta_boundaries() {
        let mut segmenter = SpeechSegmenter::default();
        let mut out = Vec::new();
        for chunk in [
            "Your next meet",
            "ing is at 3.30 today. It is with ",
            "**Acme**. Bye",
        ] {
            out.extend(segmenter.push(chunk));
        }
        // "3.30" is not a sentence end; "**" split across deltas is still stripped.
        assert_eq!(
            out,
            ["Your next meeting is at 3.30 today.", "It is with Acme."]
        );
        // No whitespace after the final "." yet, so it waits for more text or the flush at message end.
        assert!(segmenter.push(" now.").is_empty());
        assert_eq!(segmenter.flush().as_deref(), Some("Bye now."));
        assert_eq!(segmenter.flush(), None);
    }

    #[test]
    fn cuts_cjk_sentences_and_long_clauses() {
        let mut segmenter = SpeechSegmenter::new(4, 40);
        assert_eq!(
            segmenter.push("您的下一个会议在三点。和客户讨论续约。"),
            ["您的下一个会议在三点。", "和客户讨论续约。"]
        );
        let long = segmenter.push("This sentence has no end in sight, it rambles on and on");
        assert_eq!(long[0], "This sentence has no end in sight,");
    }

    #[test]
    fn short_sentences_wait_and_line_breaks_end_segments() {
        let mut segmenter = SpeechSegmenter::default();
        assert!(segmenter.push("OK. ").is_empty());
        assert_eq!(
            segmenter.push("Here is the plan\n\n- step"),
            ["OK. Here is the plan"]
        );
        segmenter.reset();
        assert_eq!(segmenter.flush(), None);
    }

    #[test]
    fn strips_markdown() {
        assert_eq!(
            speakable("## Plan\n- **Call** [Acme](https://acme.test) about `customer_id` _soon_"),
            "Plan Call Acme about customer_id soon"
        );
        assert_eq!(speakable("_a_ _b_ | c"), "a b c");
    }
}
