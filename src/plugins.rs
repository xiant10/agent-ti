//! agent-pi's plugin library (`agent-pi/plugins`), by the names configs `use`.
use agent_ti_plugin_sdk::*;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, LazyLock};

/// The plugins compiled into this binary. A product with plugins of its own builds its own binary
/// with a catalog that extends this one.
pub fn catalog(name: &str) -> Option<Factory> {
    Some(match name {
        "audit" => |_, _| Ok(Arc::new(Audit)),
        "clock" => Clock::build,
        "guardrails" => |_, _| Ok(Arc::new(Guardrails)),
        "shortcuts" => Shortcuts::build,
        _ => return None,
    })
}
fn no_options(options: &Value, allowed: &[&str]) -> Result<(), String> {
    match options
        .as_object()
        .and_then(|o| o.keys().find(|k| !allowed.contains(&k.as_str())))
    {
        Some(key) => Err(format!("unknown option {key}")),
        None => Ok(()),
    }
}

/// 13 to 19 digits, optionally grouped by spaces or dashes: a payment card number.
static CARD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(?:\d[ -]?){12,18}\d\b").unwrap());
fn redact(text: &str) -> String {
    CARD.replace_all(text, "[card number removed]").into_owned()
}
/// Card numbers never reach the model or the transcript: removed from user input before the model
/// sees it, and from tool results before they are stored.
struct Guardrails;
impl Plugin for Guardrails {
    fn name(&self) -> &str {
        "guardrails"
    }
    fn before_prompt<'a>(
        &'a self,
        turn: &'a Turn<'a>,
    ) -> BoxFuture<'a, Result<Option<PromptDecision>, String>> {
        Box::pin(async move {
            if !CARD.is_match(turn.text) {
                return Ok(None);
            }
            tracing::info!(session_id = turn.session_id, "guardrails.redacted");
            Ok(Some(PromptDecision::Input(redact(turn.text))))
        })
    }
    fn after_tool_call(&self, _call: &ToolCall, result: ToolOutcome) -> ToolOutcome {
        ToolOutcome {
            content: redact(&result.content),
            ..result
        }
    }
}

#[derive(Deserialize)]
struct Faq {
    patterns: Vec<String>,
    answer: String,
}
static GREETING: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*(hi|hello|hey|good (morning|afternoon|evening)|你好|您好)[\s!.,，。！]*$")
        .unwrap()
});
/// Answers that need no model: a bare greeting, and questions the FAQ answers verbatim.
struct Shortcuts {
    faqs: Vec<Faq>,
    assistant_name: String,
}
impl Shortcuts {
    fn build(options: &Value, snapshot: &Snapshot) -> Result<Arc<dyn Plugin>, String> {
        no_options(options, &["faqFile", "assistantName"])?;
        let file = text_option(options, "faqFile")?;
        let faqs: Vec<Faq> =
            serde_json::from_slice(&snapshot.read(&file).map_err(|e| e.to_string())?)
                .map_err(|e| format!("{file}: {e}"))?;
        Ok(Arc::new(Self {
            faqs,
            assistant_name: text_option(options, "assistantName")?,
        }))
    }
}
impl Plugin for Shortcuts {
    fn name(&self) -> &str {
        "shortcuts"
    }
    fn before_prompt<'a>(
        &'a self,
        turn: &'a Turn<'a>,
    ) -> BoxFuture<'a, Result<Option<PromptDecision>, String>> {
        Box::pin(async move {
            if GREETING.is_match(turn.text) {
                return Ok(Some(PromptDecision::Reply(format!(
                    "Hi, I'm {}. What can I do for you?",
                    self.assistant_name
                ))));
            }
            let lowered = turn.text.to_lowercase();
            Ok(self
                .faqs
                .iter()
                .find(|faq| {
                    faq.patterns
                        .iter()
                        .any(|p| lowered.contains(&p.to_lowercase()))
                })
                .map(|faq| PromptDecision::Reply(faq.answer.clone())))
        })
    }
}

/// Tells the model the current date and time in the business's time zone, in each model request
/// only, just before the latest user message: the stored transcript and the cached prompt prefix
/// stay the same from request to request.
struct Clock {
    zone: jiff::tz::TimeZone,
    name: String,
}
impl Clock {
    fn build(options: &Value, _: &Snapshot) -> Result<Arc<dyn Plugin>, String> {
        no_options(options, &["timeZone"])?;
        let name = text_option(options, "timeZone")?;
        let zone = jiff::tz::TimeZone::get(&name).map_err(|e| format!("timeZone {name:?}: {e}"))?;
        Ok(Arc::new(Self { zone, name }))
    }
}
impl Plugin for Clock {
    fn name(&self) -> &str {
        "clock"
    }
    fn before_model_request<'a>(
        &'a self,
        mut messages: Vec<Message>,
        ctx: &'a dyn RequestContext,
    ) -> BoxFuture<'a, Result<Vec<Message>, String>> {
        Box::pin(async move {
            // Fixed per run, so a request retried after a crash sees the same time.
            let now = ctx
                .memo(
                    "now",
                    Box::new(|| {
                        json!(
                            jiff::Timestamp::now()
                                .to_zoned(self.zone.clone())
                                .strftime("%A, %B %-d, %Y at %-I:%M %p")
                                .to_string()
                        )
                    }),
                )
                .await?;
            let note = Message::text(
                "user",
                format!(
                    "<now>{} ({})</now>",
                    now.as_str().unwrap_or_default(),
                    self.name
                ),
            );
            let at = messages
                .iter()
                .rposition(|m| m.role == "user")
                .unwrap_or(messages.iter().take_while(|m| m.role == "system").count());
            messages.insert(at, note);
            Ok(messages)
        })
    }
}

/// One log line per settled run: who asked, what answered it (the model or a plugin), and how it ended.
struct Audit;
impl Plugin for Audit {
    fn name(&self) -> &str {
        "audit"
    }
    fn session_created<'a>(
        &'a self,
        s: &'a SessionContext<'a>,
    ) -> BoxFuture<'a, Result<(), String>> {
        tracing::info!(
            agent = s.agent,
            session_id = s.session_id,
            user_id = s.user_id,
            tenant_id = s.tenant_id,
            profile = s.profile,
            "audit.session_created"
        );
        Box::pin(async { Ok(()) })
    }
    fn after_run(&self, r: &RunInfo<'_>) {
        tracing::info!(
            agent = r.agent,
            session_id = r.session_id,
            turn_id = r.turn_id,
            user_id = r.user_id,
            tenant_id = r.tenant_id,
            profile = r.profile,
            answered_by = r.answered_by,
            status = r.status,
            reason = r.reason,
            chars = r.text.chars().count(),
            "audit.run"
        );
    }
}
