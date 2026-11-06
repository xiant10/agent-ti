//! Step 12 at the store: retention, online backup, the read pool, and what sessions spent.
use agent_ti::plugins::within;
use agent_ti_core::Message;
use agent_ti_store::*;
use serde_json::json;
use std::path::Path;
use std::time::Duration;

fn scope(user: &str) -> UserScope {
    UserScope {
        tenant_id: "a".into(),
        agent_id: "assistant".into(),
        user_id: user.into(),
    }
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_sessions_per_tenant: 100,
        max_sessions_per_user: 100,
        max_queued_per_tenant: 100,
        max_queued_per_session: 10,
        max_input_bytes: 1024,
        restart_limit: 5,
    }
}
async fn open(path: &Path) -> AgentStore {
    AgentStore::open(path, "assistant", limits()).await.unwrap()
}
fn create(s: &UserScope, id: &str) -> CreateSession {
    CreateSession {
        scope: s.clone(),
        session_id: id.into(),
        request_key: id.into(),
        profile: "chat".into(),
        config_version: "test".into(),
        origin: json!({"trigger":"user"}),
        metadata: json!({}),
        child: None,
    }
}
fn submit(s: &UserScope, id: &str, key: &str) -> Submit {
    Submit {
        scope: s.clone(),
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
fn done(tokens: (u64, u64)) -> Completion {
    Completion {
        status: "completed".into(),
        messages: vec![Message::text("assistant", "hi")],
        error: None,
        input_tokens: tokens.0,
        output_tokens: tokens.1,
        message_id: None,
        partial: None,
    }
}
/// A session with one finished exchange.
async fn exchange(store: &AgentStore, s: &UserScope, id: &str, tokens: (u64, u64)) {
    store.create(create(s, id)).await.unwrap();
    store
        .admit(submit(s, id, &format!("{id}-1")))
        .await
        .unwrap();
    finish_next(store, tokens).await;
}
async fn finish_next(store: &AgentStore, tokens: (u64, u64)) {
    let next = store
        .runnable(10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let work = store.claim(&next.command_id).await.unwrap().unwrap();
    spend(store, &work, tokens).await;
    store.finish(&work, done(tokens)).await.unwrap();
}
/// One journaled model call.
async fn spend(store: &AgentStore, work: &Work, tokens: (u64, u64)) {
    let op = Operation {
        id: "model:0".into(),
        kind: "model".into(),
        input: json!({}),
        unsafe_effect: false,
        event: None,
    };
    store.prepare(work, op).await.unwrap();
    store
        .record_spent(work, "model:0", json!({}), None, tokens)
        .await
        .unwrap();
}
const FUTURE: i64 = i64::MAX / 2;

#[tokio::test]
async fn closed_sessions_go_whole_and_only_after_their_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    exchange(&store, &a, "assistant.r0.closed", (1, 1)).await;
    exchange(&store, &a, "assistant.r0.open", (1, 1)).await;
    store
        .update(&a, "assistant.r0.closed", None, None, true)
        .await
        .unwrap();

    // Not old enough yet: nothing goes.
    assert_eq!(store.prune(None, Some(0)).await.unwrap(), Pruned::default());
    assert!(store.state(&a, "assistant.r0.closed").await.is_ok());

    let pruned = store.prune(None, Some(FUTURE)).await.unwrap();
    assert_eq!(pruned.closed_sessions, 1);
    assert!(matches!(
        store.state(&a, "assistant.r0.closed").await,
        Err(StoreError::NotFound)
    ));
    // An open session isn't touched by this cutoff, however old.
    let page = store
        .transcript(&a, "assistant.r0.open", i64::MAX, 100)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
}

#[tokio::test]
async fn idle_sessions_lose_events_keep_transcripts_and_streams_can_tell() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    let id = "assistant.r0.idle";
    exchange(&store, &a, id, (1, 1)).await;
    let high = store.state(&a, id).await.unwrap().high_water;
    assert!(high > 2);
    assert!(!store.events_gone(&a, id, 0).await.unwrap());

    assert_eq!(
        store.prune(Some(FUTURE), None).await.unwrap().idle_sessions,
        1
    );
    assert!(store.events(&a, id, 0, 100).await.unwrap().is_empty());
    assert_eq!(
        store
            .transcript(&a, id, i64::MAX, 100)
            .await
            .unwrap()
            .entries
            .len(),
        2
    );
    // A cursor before what's gone can't resume; one at the end has missed nothing.
    assert!(store.events_gone(&a, id, 0).await.unwrap());
    assert!(!store.events_gone(&a, id, high).await.unwrap());
    // Nothing more to do on a second pass.
    assert_eq!(
        store.prune(Some(FUTURE), None).await.unwrap().idle_sessions,
        0
    );

    // The next event carries on from the same sequence.
    store.admit(submit(&a, id, "again")).await.unwrap();
    assert!(store.state(&a, id).await.unwrap().high_water > high);
    assert!(store.events_gone(&a, id, 0).await.unwrap());
}

#[tokio::test]
async fn work_in_progress_keeps_everything() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    let id = "assistant.r0.busy";
    store.create(create(&a, id)).await.unwrap();
    store.admit(submit(&a, id, "queued")).await.unwrap();
    assert_eq!(
        store.prune(Some(FUTURE), Some(FUTURE)).await.unwrap(),
        Pruned::default()
    );
    assert!(!store.events(&a, id, 0, 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_backup_taken_with_pending_work_restores_and_runs_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    exchange(&store, &a, "assistant.r0.done", (1, 1)).await;
    let id = "assistant.r0.pending";
    store.create(create(&a, id)).await.unwrap();
    store.admit(submit(&a, id, "pending")).await.unwrap();
    // Claimed and cut off, as a crash would leave it.
    let next = store
        .runnable(10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    store.claim(&next.command_id).await.unwrap().unwrap();

    let copy = dir.path().join("restore");
    std::fs::create_dir(&copy).unwrap();
    let target = copy.join("assistant.sqlite");
    store.backup(&target).await.unwrap();
    // The live store is still writable after, and an existing target is refused.
    assert!(store.backup(&target).await.is_err());
    store
        .create(create(&a, "assistant.r0.after"))
        .await
        .unwrap();

    let restored = open(&target).await;
    assert_eq!(restored.recover().await.unwrap(), 1);
    assert_eq!(
        restored
            .transcript(&a, "assistant.r0.done", i64::MAX, 100)
            .await
            .unwrap()
            .entries
            .len(),
        2
    );
    // What came after the backup isn't in it.
    assert!(matches!(
        restored.state(&a, "assistant.r0.after").await,
        Err(StoreError::NotFound)
    ));
    let next = restored
        .runnable(10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let work = restored.claim(&next.command_id).await.unwrap().unwrap();
    assert_eq!(work.session_id, id);
    restored.finish(&work, done((1, 1))).await.unwrap();
    assert!(!restored.state(&a, id).await.unwrap().busy);
}

#[tokio::test]
async fn reads_see_every_commit_while_writes_run() {
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(open(&dir.path().join("assistant.sqlite")).await);
    let a = scope("alice");
    let mut writers = tokio::task::JoinSet::new();
    for n in 0..20 {
        let (store, a) = (store.clone(), a.clone());
        writers.spawn(async move {
            let id = format!("assistant.r0.s{n}");
            store.create(create(&a, &id)).await.unwrap();
            // Read straight after the commit, on another connection.
            assert_eq!(store.state(&a, &id).await.unwrap().session.session_id, id);
        });
    }
    while let Some(done) = writers.join_next().await {
        done.unwrap();
    }
    assert_eq!(store.list(&a, "", 100).await.unwrap().len(), 20);
}

#[tokio::test]
async fn spend_adds_up_and_the_token_budget_stops_wakes() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    let id = "assistant.r0.spender";
    exchange(&store, &a, id, (100, 50)).await;
    store.admit(submit(&a, id, "second")).await.unwrap();
    finish_next(&store, (10, 5)).await;
    assert_eq!(store.usage(&a, id).await.unwrap().own, (110, 55));

    // A foreground child counts as the parent's, a task's apart.
    let mut helper = create(&a, "assistant.r0.helper");
    helper.child = Some(child_of(id, "helper"));
    store.create(helper).await.unwrap();
    store
        .admit(submit(&a, "assistant.r0.helper", "h"))
        .await
        .unwrap();
    finish_next(&store, (7, 3)).await;
    let mut scout = create(&a, "assistant.r0.scout");
    scout.child = Some(child_of(id, "scout"));
    store.create(scout).await.unwrap();
    store
        .admit(submit(&a, "assistant.r0.scout", "s"))
        .await
        .unwrap();
    finish_next(&store, (2_000, 0)).await;
    store
        .task_start(&a, id, "call", "scout", "work", 5)
        .await
        .unwrap()
        .unwrap();
    store
        .task_set_child(&a, id, "t1", "assistant.r0.scout")
        .await
        .unwrap();
    let usage = store.usage(&a, id).await.unwrap();
    assert_eq!(usage.foreground, (7, 3));
    assert_eq!(usage.tasks, vec![("t1".to_string(), (2_000, 0))]);

    // Spent since the user last spoke: over a budget of 1,000 tokens, a report is a note.
    let settle = |key: &str, tokens: u64| Settled {
        status: "idle".into(),
        content: "report".into(),
        last_report: "report".into(),
        report_key: key.into(),
        wanted: "wake".into(),
        budget: UnattendedBudget { runs: 100, tokens },
    };
    let input = |key: &str| TaskMessage {
        scope: a.clone(),
        session_id: id.into(),
        task_id: "t1".into(),
        request_key: key.into(),
        kind: "input".into(),
        content: "work".into(),
        urgent: false,
        level: "wake".into(),
        status: "submitted".into(),
    };
    let level = |key: &str, messages: Vec<TaskMessage>| {
        messages
            .into_iter()
            .find(|m| m.request_key == key)
            .unwrap()
            .level
    };
    store
        .task_settle(&input("task-input:call"), Some(settle("report:1", 1_000)))
        .await
        .unwrap();
    assert_eq!(
        level("report:1", store.open_task_messages(10).await.unwrap()),
        "note"
    );
    // Once a user has spoken, the same spend is behind them.
    store.tasks_heard(&a, id).await.unwrap();
    store
        .task_send(&a, id, "t1", "send:2", "more", false)
        .await
        .unwrap()
        .unwrap();
    store
        .task_settle(&input("send:2"), Some(settle("report:2", 1_000)))
        .await
        .unwrap();
    assert_eq!(
        level("report:2", store.open_task_messages(10).await.unwrap()),
        "wake"
    );
}

fn child_of(parent: &str, name: &str) -> Child {
    Child {
        parent: parent.into(),
        spec: json!({"name": name}),
    }
}

#[tokio::test]
async fn a_slow_hook_is_an_error() {
    let limit = Duration::from_millis(10);
    let slow = within(limit, "before_tool_call", async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok::<_, String>(1)
    });
    assert!(
        slow.await
            .unwrap_err()
            .contains("before_tool_call took more than 10 ms")
    );
    assert_eq!(
        within(limit, "h", async { Ok::<_, String>(1) }).await,
        Ok(1)
    );
}

#[tokio::test]
async fn a_run_that_ends_badly_keeps_what_it_spent_and_a_replay_adds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("assistant.sqlite")).await;
    let a = scope("alice");
    let id = "assistant.r0.failing";
    store.create(create(&a, id)).await.unwrap();
    store.admit(submit(&a, id, "fails")).await.unwrap();
    let next = store
        .runnable(10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let work = store.claim(&next.command_id).await.unwrap().unwrap();
    spend(&store, &work, (100, 50)).await;
    // A step whose result was recorded is reused, so nothing records or counts it again.
    let reused = Operation {
        id: "model:0".into(),
        kind: "model".into(),
        input: json!({}),
        unsafe_effect: false,
        event: None,
    };
    assert!(matches!(
        store.prepare(&work, reused).await.unwrap(),
        Prepared::Recorded(_)
    ));
    let failed = Completion {
        status: "failed".into(),
        messages: vec![],
        error: Some("the provider went away".into()),
        ..done((0, 0))
    };
    store.finish(&work, failed).await.unwrap();
    assert_eq!(store.usage(&a, id).await.unwrap().own, (100, 50));
}
