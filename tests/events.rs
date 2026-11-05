use agent_ti::events::{EventId, Hub};
use agent_ti_store::DurableEvent;
use serde_json::json;

fn committed(hub: &Hub, seq: i64, kind: &str) {
    hub.committed(vec![(
        "s".into(),
        DurableEvent {
            sequence: seq,
            payload: json!({"type": kind}),
        },
    )]);
}
fn delta(hub: &Hub, text: &str) {
    hub.delta(
        "s",
        json!({"type":"text.delta","turnId":"t","messageId":"m","text":text}),
    );
}
#[test]
fn ids_order_deltas_after_the_last_committed_event() {
    assert_eq!(
        EventId::parse("s", "s:4"),
        Some(EventId { seq: 4, delta: 0 })
    );
    assert_eq!(
        EventId::parse("s", "s:4.2"),
        Some(EventId { seq: 4, delta: 2 })
    );
    for bad in ["s:4.0", "s:-1", "other:4", "s:x", "s:4.x"] {
        assert_eq!(EventId::parse("s", bad), None, "{bad}");
    }
    assert!(EventId { seq: 4, delta: 9 } < EventId { seq: 5, delta: 0 });
    assert_eq!(EventId { seq: 4, delta: 2 }.render("s"), "s:4.2");
}
#[tokio::test]
async fn a_stream_resumes_from_a_delta_or_gets_the_partial_reply() {
    let hub = Hub::default();
    committed(&hub, 1, "run.started");
    delta(&hub, "Hel");
    delta(&hub, "lo");
    // Fresh: no deltas to replay, the partial reply instead, positioned after the last delta.
    let fresh = hub.subscribe("s", 0, None);
    assert!(fresh.deltas.is_none());
    assert_eq!(fresh.partial.unwrap().text, "Hello");
    assert_eq!(fresh.position, EventId { seq: 1, delta: 2 });
    // From the first delta: exactly the rest.
    let resumed = hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 1 }));
    let rest: Vec<_> = resumed
        .deltas
        .unwrap()
        .iter()
        .map(|d| d.payload["text"].clone())
        .collect();
    assert_eq!(rest, [json!("lo")]);
    // From a committed cursor: the store supplies committed events, the hub the deltas after it.
    assert_eq!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 0 }))
            .deltas
            .unwrap()
            .len(),
        2
    );
    // A delta that never existed (as after a restart) or from the future gets a snapshot.
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 7 }))
            .deltas
            .is_none()
    );
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 9, delta: 0 }))
            .deltas
            .is_none()
    );
    // Live: a subscriber sees later deltas and commits in order.
    let mut rx = fresh.rx;
    delta(&hub, "!");
    committed(&hub, 2, "message.end");
    assert_eq!(rx.recv().await.unwrap().id, EventId { seq: 1, delta: 3 });
    assert_eq!(rx.recv().await.unwrap().id, EventId { seq: 2, delta: 0 });
    assert!(hub.subscribe("s", 0, None).partial.is_none());
}
#[test]
fn dropped_deltas_of_the_reply_in_progress_force_a_snapshot() {
    let hub = Hub::default();
    committed(&hub, 1, "run.started");
    let big = "x".repeat(4096);
    for _ in 0..300 {
        delta(&hub, &big);
    }
    // The ring holds 1 MiB: the earliest deltas of this reply are gone.
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 0 }))
            .deltas
            .is_none()
    );
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 1 }))
            .deltas
            .is_none()
    );
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 299 }))
            .deltas
            .is_some()
    );
    assert_eq!(
        hub.subscribe("s", 0, None).partial.unwrap().text.len(),
        300 * 4096
    );
    // Once that reply has ended, its committed message.end carries the text: replay is exact again.
    committed(&hub, 2, "message.end");
    assert!(
        hub.subscribe("s", 0, Some(EventId { seq: 1, delta: 0 }))
            .deltas
            .is_some()
    );
}
