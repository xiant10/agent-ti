use agent_ti_store::*;
use serde_json::json;
use std::path::Path;
fn scope(tenant: &str, user: &str) -> UserScope {
    UserScope {
        tenant_id: tenant.into(),
        agent_id: "assistant".into(),
        user_id: user.into(),
    }
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_sessions_per_tenant: 10_000,
        max_sessions_per_user: 1000,
        max_queued_per_tenant: 128,
        max_queued_per_session: 16,
        max_input_bytes: 32 * 1024,
        restart_limit: 2,
    }
}
async fn open(path: &Path) -> AgentStore {
    AgentStore::open(path, "assistant", limits()).await.unwrap()
}
fn create(s: UserScope, id: &str, key: &str) -> CreateSession {
    CreateSession {
        scope: s,
        session_id: id.into(),
        request_key: key.into(),
        profile: "chat".into(),
        config_version: "test".into(),
        origin: json!({"trigger":"user"}),
        metadata: json!({}),
    }
}
fn submit(s: UserScope, id: &str, key: &str) -> Submit {
    Submit {
        scope: s,
        session_id: id.into(),
        request_key: key.into(),
        fingerprint: digest(&json!("hello")),
        input: "hello".into(),
        turn_id: key.into(),
        traceparent: None,
        reply: None,
        when_busy: None,
    }
}
async fn claim_next(store: &AgentStore) -> Option<Work> {
    let next = store.runnable(10).await.unwrap().into_iter().next()?;
    store.claim(&next.command_id).await.unwrap()
}
fn tool(event: bool) -> Operation {
    Operation {
        id: "tool:1".into(),
        kind: "tool".into(),
        input: json!({"write":1}),
        unsafe_effect: true,
        event: event.then(|| json!({"type":"tool.started"})),
    }
}
fn done(messages: Vec<agent_ti_core::Message>) -> Completion {
    Completion {
        status: "completed".into(),
        messages,
        error: None,
        input_tokens: 0,
        output_tokens: 0,
        message_id: None,
        partial: None,
    }
}
#[tokio::test]
async fn concurrent_keys_quotas_and_foreign_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("a", "alice");
    let (one, two) = tokio::join!(
        store.create(create(a.clone(), "assistant.r0.one", "same")),
        store.create(create(a.clone(), "assistant.r0.two", "same"))
    );
    let id = one.unwrap().0.session_id;
    assert_eq!(two.unwrap().0.session_id, id);
    for foreign in [
        scope("a", "bob"),
        scope("b", "alice"),
        UserScope {
            agent_id: "other".into(),
            ..a.clone()
        },
    ] {
        assert_eq!(
            store.state(&foreign, &id).await.unwrap_err(),
            StoreError::NotFound
        );
        assert_eq!(
            store
                .admit(submit(foreign.clone(), &id, "x"))
                .await
                .unwrap_err(),
            StoreError::NotFound
        );
        assert_eq!(
            store.cancel(&foreign, &id, "x").await.unwrap_err(),
            StoreError::NotFound
        );
        assert_eq!(
            store.prior(&foreign, &id, "x").await.unwrap_err(),
            StoreError::NotFound
        );
        assert!(store.transcript(&foreign, &id, i64::MAX, 10).await.is_err());
        assert!(store.events(&foreign, &id, 0, 10).await.is_err());
    }
    let (one, two) = tokio::join!(
        store.admit(submit(a.clone(), &id, "same")),
        store.admit(submit(a.clone(), &id, "same"))
    );
    assert_eq!(one.unwrap().command_id, two.unwrap().command_id);
    let mut conflict = submit(a.clone(), &id, "same");
    conflict.fingerprint = digest(&json!("different"));
    assert_eq!(
        store.admit(conflict).await.unwrap_err(),
        StoreError::Conflict
    );
    for i in 1..16 {
        store
            .admit(submit(a.clone(), &id, &format!("key{i}")))
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .admit(submit(a.clone(), &id, "full"))
            .await
            .unwrap_err(),
        StoreError::QuotaExceeded
    );
    assert!(store.admit(submit(a, &id, "same")).await.unwrap().duplicate);
    store.close().await;
}
#[tokio::test]
async fn an_unsafe_step_cut_off_by_a_crash_is_interrupted_and_crash_loops_park() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("a", "alice");
    let id = "assistant.r0.one";
    store.create(create(a.clone(), id, "create")).await.unwrap();
    store.admit(submit(a.clone(), id, "run")).await.unwrap();
    let old = claim_next(&store).await.unwrap();
    assert_eq!(
        store.prepare(&old, tool(true)).await.unwrap(),
        Prepared::Run
    );
    assert_eq!(store.recover().await.unwrap(), 1);
    let recovered = claim_next(&store).await.unwrap();
    assert_eq!(
        store
            .record(&old, "tool:1", json!("stale"), None)
            .await
            .unwrap_err(),
        StoreError::Conflict
    );
    // Not repeated: the worker records `interrupted` for the model and carries on.
    assert_eq!(
        store.prepare(&recovered, tool(true)).await.unwrap(),
        Prepared::Interrupted
    );
    store
        .record(
            &recovered,
            "tool:1",
            json!({"content":"interrupted","is_error":true}),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        store.prepare(&recovered, tool(false)).await.unwrap(),
        Prepared::Recorded(_)
    ));
    // Two restarts in a row without a new step reach the restart limit of 2: parked, session busy.
    store.recover().await.unwrap();
    claim_next(&store).await.unwrap();
    store.recover().await.unwrap();
    assert!(claim_next(&store).await.is_none());
    let state = store.state(&a, id).await.unwrap();
    assert!(state.busy);
    assert_eq!(state.session.parked, 1);
    let events = store.events(&a, id, 0, 100).await.unwrap();
    assert!(events.iter().any(|e| e.payload["code"] == "parked"));
    // `session.cancel` gives up on it.
    assert!(store.cancel(&a, id, "give-up").await.unwrap().is_empty());
    let state = store.state(&a, id).await.unwrap();
    assert!(!state.busy);
    assert_eq!(state.session.parked, 0);
    store.close().await;
}
#[tokio::test]
async fn duplicate_cancel_does_not_cancel_the_next_command() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let s = scope("a", "user");
    let id = "assistant.r0.a";
    store.create(create(s.clone(), id, "create")).await.unwrap();
    for key in ["one", "two"] {
        store.admit(submit(s.clone(), id, key)).await.unwrap();
    }
    // One command at a time per session.
    assert_eq!(store.runnable(10).await.unwrap().len(), 1);
    let first = claim_next(&store).await.unwrap();
    assert!(store.runnable(10).await.unwrap().is_empty());
    assert_eq!(
        store.cancel(&s, id, "cancel-once").await.unwrap(),
        vec![first.command_id.clone()]
    );
    assert_eq!(
        store.prepare(&first, tool(false)).await.unwrap_err(),
        StoreError::Conflict
    );
    store.finish(&first, done(vec![])).await.unwrap();
    let next = claim_next(&store).await.unwrap();
    assert_eq!(first.session_id, next.session_id);
    assert!(
        store
            .cancel(&s, id, "cancel-once")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!next.cancel_requested);
    assert_eq!(
        store.prepare(&next, tool(false)).await.unwrap(),
        Prepared::Run
    );
    let events = store.events(&s, id, 0, 100).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.payload["status"] == "cancelled")
            .count(),
        1
    );
    store.close().await;
}
#[tokio::test]
async fn a_plugin_reply_is_placed_when_idle_and_queued_when_busy() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let s = scope("a", "alice");
    let id = "assistant.r0.reply";
    store.create(create(s.clone(), id, "create")).await.unwrap();
    let reply = Reply {
        text: "Hi.".into(),
        source: "shortcuts".into(),
    };
    let mut greeting = submit(s.clone(), id, "hi");
    greeting.reply = Some(reply.clone());
    let placed = store.admit(greeting.clone()).await.unwrap();
    assert!(placed.placed && !placed.duplicate);
    assert!(store.runnable(10).await.unwrap().is_empty());
    let page = store.transcript(&s, id, i64::MAX, 10).await.unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|e| e.message.source.as_deref())
            .collect::<Vec<_>>(),
        [Some("shortcuts"), Some("shortcuts")]
    );
    // A retried request ID returns the same reply without hooks running again.
    let prior = store.prior(&s, id, "hi").await.unwrap().unwrap();
    assert_eq!(prior.admission.reply, Some(reply.clone()));
    assert_eq!(prior.fingerprint, greeting.fingerprint);
    store.admit(submit(s.clone(), id, "model")).await.unwrap();
    let mut busy = submit(s.clone(), id, "hi-again");
    busy.reply = Some(reply);
    assert!(!store.admit(busy).await.unwrap().placed);
    let model = claim_next(&store).await.unwrap();
    store
        .finish(
            &model,
            done(vec![agent_ti_core::Message::text("assistant", "answer")]),
        )
        .await
        .unwrap();
    let queued = claim_next(&store).await.unwrap();
    assert!(queued.reply.is_some());
    store
        .finish(
            &queued,
            done(vec![agent_ti_core::Message {
                source: Some("shortcuts".into()),
                ..agent_ti_core::Message::text("assistant", "Hi.")
            }]),
        )
        .await
        .unwrap();
    let events = store.events(&s, id, 0, 100).await.unwrap();
    let ends: Vec<_> = events
        .iter()
        .filter(|e| e.payload["type"] == "message.end")
        .map(|e| e.payload["source"].as_str())
        .collect();
    assert_eq!(ends, [Some("shortcuts"), None, Some("shortcuts")]);
    store.close().await;
}
#[tokio::test]
async fn a_store_belongs_to_one_agent_and_one_process() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assistant.sqlite");
    let store = open(&path).await;
    assert!(matches!(
        AgentStore::open(&path, "assistant", limits()).await,
        Err(StoreError::Conflict)
    ));
    let id = store.instance_id(None).await.unwrap();
    assert_eq!(store.instance_id(None).await.unwrap(), id);
    store.close().await;
    assert!(
        matches!(AgentStore::open(&path, "sales", limits()).await, Err(StoreError::WrongAgent(owner)) if owner == "assistant")
    );
}
#[tokio::test]
async fn workbench_metadata_activity_and_transcript_pagination_are_durable() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let s = scope("a", "alice");
    let id = "assistant.r0.presentation";
    let mut request = create(s.clone(), id, "create");
    request.metadata = json!({"title":"My chat","pinned":true});
    let (created, _) = store.create(request).await.unwrap();
    assert_eq!(created.metadata["title"], "My chat");
    assert!(created.created_at > 0);
    assert!(!created.busy);
    for i in 0..3 {
        let key = format!("request{i}");
        store.admit(submit(s.clone(), id, &key)).await.unwrap();
        assert!(store.state(&s, id).await.unwrap().session.busy);
        let work = claim_next(&store).await.unwrap();
        store
            .finish(
                &work,
                done(vec![agent_ti_core::Message::text(
                    "assistant",
                    format!("answer{i}"),
                )]),
            )
            .await
            .unwrap();
    }
    let page = store.transcript(&s, id, i64::MAX, 2).await.unwrap();
    assert!(page.has_more);
    assert_eq!(page.entries[0].request_id, "request2");
    assert_eq!(page.entries[1].message.content, "answer2");
    let older = store
        .transcript(&s, id, page.entries[0].sequence, 4)
        .await
        .unwrap();
    assert!(!older.has_more);
    assert_eq!(older.entries.len(), 4);
    assert_eq!(older.entries[0].sequence, 1);
    assert_eq!(
        store
            .transcript(&scope("a", "bob"), id, i64::MAX, 10)
            .await
            .unwrap_err(),
        StoreError::NotFound
    );
    let updated = store
        .update(
            &s,
            id,
            None,
            Some(json!({"title":"Renamed","pinned":false})),
            false,
        )
        .await
        .unwrap();
    assert_eq!(updated.metadata["title"], "Renamed");
    assert!(!updated.busy);
    assert!(updated.last_active_at >= created.created_at);
    store.close().await;
}
