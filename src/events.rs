//! Live events. Committed events reach the hub after their transaction commits; text and thinking
//! deltas are published from memory and never written. One order covers both: a delta's ID is the
//! last committed event's sequence plus its index after it, `<session>:<seq>.<n>`. `speech`
//! events are deltas too: a session whose profile speaks cuts its text into sentences here, as
//! agent-pi's event mapper does, flushed before `tool.started`, `message.end` and `run.finished`.
//! The hub also keeps what an acknowledgement needs (`src/progress.rs`): the run's latest
//! `progress`, for snapshots, and when the user last saw something new.
use crate::speech::SpeechSegmenter;
use agent_ti_store::DurableEvent;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, broadcast};

/// Deltas a session keeps for resuming, by count and by bytes.
const RING_EVENTS: usize = 1024;
const RING_BYTES: usize = 1024 * 1024;
/// As agent-pi's hub: a session nobody follows is forgotten after this long without events.
const IDLE: Duration = Duration::from_secs(60);
/// A subscriber this far behind is disconnected and resumes from its cursor.
const CHANNEL: usize = 1024;
/// How long a prompt that will stream its own events counts as watched before its stream opens.
const EXPECTED: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventId {
    pub seq: i64,
    /// 0 for a committed event; 1, 2, … for the deltas after it.
    pub delta: u32,
}
impl EventId {
    pub fn render(&self, session: &str) -> String {
        if self.delta == 0 {
            format!("{session}:{}", self.seq)
        } else {
            format!("{session}:{}.{}", self.seq, self.delta)
        }
    }
    pub fn parse(session: &str, cursor: &str) -> Option<Self> {
        let rest = cursor.strip_prefix(session)?.strip_prefix(':')?;
        let (seq, delta) = match rest.split_once('.') {
            Some((seq, delta)) => (seq.parse().ok()?, delta.parse().ok().filter(|d| *d > 0)?),
            None => (rest.parse().ok()?, 0),
        };
        (seq >= 0).then_some(Self { seq, delta })
    }
}
#[derive(Clone, Debug)]
pub struct Published {
    pub id: EventId,
    pub payload: Arc<Value>,
}
/// The reply in progress: everything sent so far, so a client that missed deltas can pick up
/// from `offset` = the text's UTF-16 length.
#[derive(Clone, Debug)]
pub struct Partial {
    pub turn_id: String,
    pub message_id: String,
    pub text: String,
    /// Its first delta.
    start: EventId,
}
impl Partial {
    pub fn value(&self) -> Value {
        json!({"turnId":self.turn_id,"messageId":self.message_id,"text":self.text})
    }
}
struct Live {
    tx: broadcast::Sender<Published>,
    high_water: i64,
    next_delta: u32,
    ring: VecDeque<Published>,
    ring_bytes: usize,
    /// The newest delta dropped from the ring: a cursor before it can't resume exactly.
    dropped: Option<EventId>,
    partial: Option<Partial>,
    speech: Option<Speech>,
    active: Instant,
    /// The run whose `progress` this is, and the event, until that run finishes.
    progress: Option<(String, Value)>,
    /// When something new last reached the user: text, a tool, a retry. Progress itself doesn't count.
    seen: Option<Instant>,
    /// When answer text last arrived.
    text: Option<Instant>,
    /// Woken when `seen` moves.
    activity: Arc<Notify>,
    /// A prompt said it would stream this session's events, until then.
    expected: Option<Instant>,
}
/// A speaking session's segmenter, and the message whose text it has been fed.
#[derive(Default)]
struct Speech {
    segmenter: SpeechSegmenter,
    index: u64,
    message_id: Option<String>,
}
impl Live {
    fn new(high_water: i64) -> Self {
        Self {
            tx: broadcast::channel(CHANNEL).0,
            high_water,
            next_delta: 1,
            ring: VecDeque::new(),
            ring_bytes: 0,
            dropped: None,
            partial: None,
            speech: None,
            active: Instant::now(),
            progress: None,
            seen: None,
            text: None,
            activity: Arc::new(Notify::new()),
            expected: None,
        }
    }
    /// Something new reached the user; `text` when it was answer text.
    fn saw(&mut self, text: bool) {
        let now = Instant::now();
        self.seen = Some(now);
        if text {
            self.text = Some(now);
        }
        self.activity.notify_waiters();
    }
    /// Publishes one delta from memory, keeping it in the ring.
    fn publish(&mut self, payload: Value) -> EventId {
        let id = EventId {
            seq: self.high_water,
            delta: self.next_delta,
        };
        self.next_delta += 1;
        self.active = Instant::now();
        let size = payload["text"].as_str().map_or(64, str::len);
        let published = Published {
            id,
            payload: Arc::new(payload),
        };
        self.ring.push_back(published.clone());
        self.ring_bytes += size;
        while self.ring.len() > RING_EVENTS || self.ring_bytes > RING_BYTES {
            let old = self.ring.pop_front().unwrap();
            self.ring_bytes -= old.payload["text"].as_str().map_or(64, str::len);
            self.dropped = Some(old.id);
        }
        let _ = self.tx.send(published);
        id
    }
    /// Feeds a message's text to the segmenter and publishes the sentences it completes.
    fn speak(&mut self, message_id: &str, text: &str) {
        let Some(speech) = &mut self.speech else {
            return;
        };
        speech.message_id = Some(message_id.to_owned());
        let segments = speech.segmenter.push(text);
        self.say(segments);
    }
    /// Publishes whatever the segmenter holds, at a pause or the end of a message.
    fn flush_speech(&mut self) {
        let Some(speech) = &mut self.speech else {
            return;
        };
        let segment = speech.segmenter.flush();
        self.say(segment.into_iter().collect());
    }
    fn say(&mut self, segments: Vec<String>) {
        for text in segments {
            let speech = self.speech.as_mut().unwrap();
            let index = speech.index;
            speech.index += 1;
            self.publish(json!({"type":"speech","text":text,"index":index}));
        }
    }
    /// Speech for a committed event: what comes before it is flushed first, and an aborted
    /// message's buffered text is dropped, since interrupted speech isn't finished.
    fn speech_before(&mut self, payload: &Value) {
        if self.speech.is_none() {
            return;
        }
        match payload["type"].as_str() {
            Some("tool.started" | "run.finished") => self.flush_speech(),
            Some("message.end") => {
                let speech = self.speech.as_mut().unwrap();
                if matches!(payload["stopReason"].as_str(), Some("aborted" | "error")) {
                    speech.segmenter.reset();
                    speech.message_id = None;
                    return;
                }
                let message = payload["messageId"].as_str().unwrap_or_default();
                // A reply that wasn't streamed in this process, such as one recovered after a
                // restart, is spoken whole from its `message.end`.
                if speech.message_id.as_deref() != Some(message) {
                    let text = payload["text"].as_str().unwrap_or_default().to_owned();
                    self.speak(message, &text);
                }
                self.flush_speech();
                self.speech.as_mut().unwrap().message_id = None;
            }
            _ => {}
        }
    }
}
/// What a subscriber starts from, read under one lock so nothing falls between it and the live feed.
pub struct Subscription {
    pub rx: broadcast::Receiver<Published>,
    /// The newest event published: committed, or a delta after it.
    pub position: EventId,
    pub high_water: i64,
    pub partial: Option<Partial>,
    /// The running turn's latest `progress`, for a snapshot.
    pub progress: Option<Value>,
    /// Deltas after the cursor, when it can resume exactly; `None` means send a snapshot.
    pub deltas: Option<Vec<Published>>,
}

#[derive(Default)]
pub struct Hub {
    sessions: Mutex<HashMap<String, Live>>,
}
impl Hub {
    /// Committed events, in commit order.
    pub fn committed(&self, events: Vec<(String, DurableEvent)>) {
        let mut sessions = self.sessions.lock().unwrap();
        for (session, event) in events {
            let live = sessions
                .entry(session.clone())
                .or_insert_with(|| Live::new(event.sequence - 1));
            if event.sequence <= live.high_water {
                continue;
            }
            live.speech_before(&event.payload);
            match event.payload["type"].as_str() {
                Some("tool.started" | "tool.finished" | "message.end") => live.saw(false),
                Some("text.delta") => live.saw(true),
                Some("run.finished")
                    if live
                        .progress
                        .as_ref()
                        .is_some_and(|(run, _)| event.payload["runId"] == run.as_str()) =>
                {
                    live.progress = None
                }
                _ => {}
            }
            live.high_water = event.sequence;
            live.next_delta = 1;
            live.active = Instant::now();
            if matches!(
                event.payload["type"].as_str(),
                Some("message.end" | "run.finished")
            ) {
                live.partial = None;
            }
            let payload = Arc::new(event.payload);
            let _ = live.tx.send(Published {
                id: EventId {
                    seq: event.sequence,
                    delta: 0,
                },
                payload: payload.clone(),
            });
            // A plugin's reply is committed as one text delta.
            if payload["type"] == "text.delta" {
                let message = payload["messageId"].as_str().unwrap_or_default();
                live.speak(message, payload["text"].as_str().unwrap_or_default());
            }
        }
    }
    /// Marks a session whose profile speaks, before its text arrives. `stored` is its committed
    /// high-water mark, for a session this process hasn't seen yet.
    pub fn speak(&self, session: &str, stored: i64) {
        let mut sessions = self.sessions.lock().unwrap();
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(stored));
        live.speech.get_or_insert_with(Speech::default);
    }
    /// Publishes a text or thinking delta from memory. A text delta also extends the partial reply.
    pub fn delta(&self, session: &str, payload: Value) {
        let mut sessions = self.sessions.lock().unwrap();
        // A run's `run.started` was committed just before its first delta, so the session is known.
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(0));
        let id = EventId {
            seq: live.high_water,
            delta: live.next_delta,
        };
        if payload["type"] == "text.delta" {
            let text = payload["text"].as_str().unwrap_or_default().to_owned();
            let message = payload["messageId"].as_str().unwrap_or_default().to_owned();
            match &mut live.partial {
                Some(p) if p.message_id == message => p.text.push_str(&text),
                partial => {
                    let turn_id = payload["turnId"].as_str().unwrap_or_default().into();
                    *partial = Some(Partial {
                        turn_id,
                        message_id: message.clone(),
                        text: text.clone(),
                        start: id,
                    })
                }
            }
            live.publish(payload);
            live.speak(&message, &text);
            live.saw(true);
        } else {
            let seen = !matches!(payload["type"].as_str(), Some("progress" | "speech"));
            live.publish(payload);
            if seen {
                live.saw(false);
            }
        }
    }
    /// Publishes a run's `progress` and keeps it for snapshots; `finished` clears it.
    pub fn progress(&self, session: &str, run: &str, payload: Value) {
        let mut sessions = self.sessions.lock().unwrap();
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(0));
        live.progress = (payload["phase"] != "finished").then(|| (run.to_owned(), payload.clone()));
        live.publish(payload);
    }
    /// A prompt will stream this session's events: it counts as watched before its stream opens.
    pub fn expect_watcher(&self, session: &str, stored: i64) {
        let mut sessions = self.sessions.lock().unwrap();
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(stored));
        live.expected = Some(Instant::now() + EXPECTED);
    }
    /// Someone follows this session, or is about to.
    pub fn watched(&self, session: &str) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .get(session)
            .is_some_and(|live| {
                live.tx.receiver_count() > 0
                    || live.expected.is_some_and(|until| until > Instant::now())
            })
    }
    /// Woken whenever something new reaches the user of this session.
    pub fn activity(&self, session: &str) -> Arc<Notify> {
        let mut sessions = self.sessions.lock().unwrap();
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(0));
        live.activity.clone()
    }
    /// When something new last reached the user, and when answer text last arrived.
    pub fn last_seen(&self, session: &str) -> (Option<Instant>, Option<Instant>) {
        self.sessions
            .lock()
            .unwrap()
            .get(session)
            .map_or((None, None), |live| (live.seen, live.text))
    }
    /// Subscribes to a session. `stored` is its committed high-water mark in the store, for a session
    /// with no events since this process started. With a cursor, the deltas after it are returned if
    /// it can resume exactly: a delta still in the ring, or a committed event, unless deltas of the
    /// reply in progress after it were dropped. (Dropped deltas of finished messages don't matter:
    /// their committed `message.end` carries the whole text.)
    pub fn subscribe(&self, session: &str, stored: i64, cursor: Option<EventId>) -> Subscription {
        let mut sessions = self.sessions.lock().unwrap();
        let live = sessions
            .entry(session.to_owned())
            .or_insert_with(|| Live::new(stored));
        live.active = Instant::now();
        let position = live
            .ring
            .back()
            .map(|d| d.id)
            .filter(|id| id.seq == live.high_water)
            .unwrap_or(EventId {
                seq: live.high_water,
                delta: 0,
            });
        let lost = |c: &EventId| match (live.dropped, &live.partial) {
            (Some(dropped), Some(partial)) => dropped >= partial.start && *c < dropped,
            _ => false,
        };
        let exact = cursor.filter(|c| {
            *c <= position && !lost(c) && (c.delta == 0 || live.ring.iter().any(|d| d.id == *c))
        });
        Subscription {
            rx: live.tx.subscribe(),
            position,
            high_water: live.high_water,
            partial: live.partial.clone(),
            progress: live.progress.as_ref().map(|(_, p)| p.clone()),
            deltas: exact.map(|c| live.ring.iter().filter(|d| d.id > c).cloned().collect()),
        }
    }
    /// Forgets sessions nobody follows that have been idle for a minute, keeping a reply in progress.
    pub fn sweep(&self) {
        self.sessions.lock().unwrap().retain(|_, live| {
            live.tx.receiver_count() > 0
                || live.partial.is_some()
                || live.progress.is_some()
                || live.active.elapsed() < IDLE
        });
    }
}
