//! Live events. Committed events reach the hub after their transaction commits; text and thinking
//! deltas are published from memory and never written. One order covers both: a delta's ID is the
//! last committed event's sequence plus its index after it, `<session>:<seq>.<n>`.
use agent_ti_store::DurableEvent;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::broadcast;

/// Deltas a session keeps for resuming, by count and by bytes.
const RING_EVENTS: usize = 1024;
const RING_BYTES: usize = 1024 * 1024;
/// As agent-pi's hub: a session nobody follows is forgotten after this long without events.
const IDLE: Duration = Duration::from_secs(60);
/// A subscriber this far behind is disconnected and resumes from its cursor.
const CHANNEL: usize = 1024;

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
    active: Instant,
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
            active: Instant::now(),
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
            live.high_water = event.sequence;
            live.next_delta = 1;
            live.active = Instant::now();
            if matches!(
                event.payload["type"].as_str(),
                Some("message.end" | "run.finished")
            ) {
                live.partial = None;
            }
            let _ = live.tx.send(Published {
                id: EventId {
                    seq: event.sequence,
                    delta: 0,
                },
                payload: Arc::new(event.payload),
            });
        }
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
        live.next_delta += 1;
        live.active = Instant::now();
        if payload["type"] == "text.delta" {
            let text = payload["text"].as_str().unwrap_or_default();
            let message = payload["messageId"].as_str().unwrap_or_default();
            match &mut live.partial {
                Some(p) if p.message_id == message => p.text.push_str(text),
                partial => {
                    let turn_id = payload["turnId"].as_str().unwrap_or_default().into();
                    *partial = Some(Partial {
                        turn_id,
                        message_id: message.into(),
                        text: text.into(),
                        start: id,
                    })
                }
            }
        }
        let size = payload["text"].as_str().map_or(64, str::len);
        let published = Published {
            id,
            payload: Arc::new(payload),
        };
        live.ring.push_back(published.clone());
        live.ring_bytes += size;
        while live.ring.len() > RING_EVENTS || live.ring_bytes > RING_BYTES {
            let old = live.ring.pop_front().unwrap();
            live.ring_bytes -= old.payload["text"].as_str().map_or(64, str::len);
            live.dropped = Some(old.id);
        }
        let _ = live.tx.send(published);
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
            deltas: exact.map(|c| live.ring.iter().filter(|d| d.id > c).cloned().collect()),
        }
    }
    /// Forgets sessions nobody follows that have been idle for a minute, keeping a reply in progress.
    pub fn sweep(&self) {
        self.sessions.lock().unwrap().retain(|_, live| {
            live.tx.receiver_count() > 0 || live.partial.is_some() || live.active.elapsed() < IDLE
        });
    }
}
