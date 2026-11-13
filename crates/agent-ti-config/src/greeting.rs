//! The call greeting and the context variables its text can name (`docs/greeting.md`): their
//! configuration, the `{{name ?? "default"}}` placeholder syntax, and a tenant's override.
use crate::{ConfigError, Raw, Reader, Result};
use regex::Regex;
use serde_json::Value;
use std::{collections::BTreeMap, sync::LazyLock};

const GREETING: &[&str] = &[
    "instructions",
    "instructionsFile",
    "text",
    "model",
    "locale",
    "timeZone",
    "timeoutMs",
    "variablesTimeoutMs",
    "cooldownMs",
];
const VARIABLE: &[&str] = &["tool", "args", "pick", "description", "ttlMs", "trust"];

/// At most this many tool variables an agent, or a tenant, configures.
pub const MAX_AGENT_VARIABLES: usize = 16;
pub const MAX_TENANT_VARIABLES: usize = 10;
pub const MAX_INSTRUCTIONS_BYTES: usize = 4 * 1024;
pub const MAX_TEXT_CHARS: usize = 300;
pub const MAX_ALTERNATIVES: usize = 10;

/// Namespaces the harness answers itself. A configured variable can't use one.
pub const BUILTIN_NAMESPACES: &[&str] = &[
    "user",
    "team",
    "time",
    "conversation",
    "agent",
    "call",
    "client",
];

/// Whether a variable's value is the platform's (inline anywhere) or text someone else wrote
/// (kept out of a model's instructions, in a data block).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trust {
    System,
    Content,
}
impl Trust {
    pub fn as_str(self) -> &'static str {
        match self {
            Trust::System => "system",
            Trust::Content => "content",
        }
    }
}

/// A built-in variable: its name, what it holds, its trust, and whether it's personal (a caller
/// who chose a simple greeting hears none of the personal ones).
pub struct Builtin {
    pub name: &'static str,
    pub description: &'static str,
    pub trust: Trust,
    pub personal: bool,
}
pub const BUILTINS: &[Builtin] = &[
    Builtin {
        name: "user.name",
        description: "The caller's display name",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "user.role",
        description: "The caller's role in the organization",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "team.name",
        description: "The caller's team (organization unit)",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "team.manager",
        description: "The caller's manager's name",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "time.part",
        description: "Part of the day, in the call's language: morning, afternoon, evening or night",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "time.weekday",
        description: "Day of the week, in the call's language",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "time.local",
        description: "Local time, such as 14:05",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "time.date",
        description: "Local date, such as 2026-11-12",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "conversation.returning",
        description: "Whether this conversation already has messages",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "conversation.minutesSince",
        description: "Minutes since the conversation's last message",
        trust: Trust::System,
        personal: true,
    },
    Builtin {
        name: "conversation.recent",
        description: "The conversation's last few lines",
        trust: Trust::Content,
        personal: true,
    },
    Builtin {
        name: "agent.name",
        description: "The agent's name",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "call.locale",
        description: "The call's language",
        trust: Trust::System,
        personal: false,
    },
    Builtin {
        name: "call.channel",
        description: "How the greeting is delivered: call",
        trust: Trust::System,
        personal: false,
    },
];
/// `client.*` takes any field the client sent.
pub const CLIENT_DESCRIPTION: &str =
    "What the caller's app sent with the call, such as client.screen";

/// A greeting's fixed text: alternatives, per language. The empty language is text for any.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GreetingText(pub Vec<(String, Vec<String>)>);
impl GreetingText {
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|(_, list)| list.is_empty())
    }
    /// The alternatives for a language: exact, then by primary tag, then `fallback`'s, then the
    /// first given.
    pub fn for_locale(&self, locale: &str, fallback: &str) -> &[String] {
        let primary = |l: &str| {
            l.split(['-', '_'])
                .next()
                .unwrap_or("")
                .to_ascii_lowercase()
        };
        let find = |want: &str| {
            self.0
                .iter()
                .find(|(l, _)| l.eq_ignore_ascii_case(want))
                .or_else(|| {
                    self.0
                        .iter()
                        .find(|(l, _)| !l.is_empty() && primary(l) == primary(want))
                })
        };
        find(locale)
            .or_else(|| find(fallback))
            .or_else(|| self.0.iter().find(|(l, _)| l.is_empty()))
            .or_else(|| self.0.first())
            .map_or(&[], |(_, list)| list.as_slice())
    }
    pub fn texts(&self) -> impl Iterator<Item = &String> {
        self.0.iter().flat_map(|(_, list)| list)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GreetingConfig {
    pub instructions: Option<String>,
    pub text: GreetingText,
    /// A name in `models`.
    pub model: String,
    pub locale: String,
    pub time_zone: String,
    pub timeout_ms: u64,
    pub variables_timeout_ms: u64,
    pub cooldown_ms: u64,
}

/// A variable whose value comes from one of the agent's tools, called as the caller.
#[derive(Clone, Debug, PartialEq)]
pub struct VariableConfig {
    pub name: String,
    /// `owner:tool`, as `alwaysOnTools` names it.
    pub tool: String,
    pub args: Value,
    /// A path into the tool's summarized result: `count`, `items`, `items[].title`.
    pub pick: String,
    pub description: String,
    pub ttl_ms: u64,
    pub trust: Trust,
}

/// One `{{name}}` or `{{name ?? "default"}}` in a text.
#[derive(Clone, Debug, PartialEq)]
pub struct Placeholder {
    pub name: String,
    pub default: Option<String>,
    pub start: usize,
    pub end: usize,
}

static ANY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{(.*?)\}\}").unwrap());
static ONE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"^\s*([a-z][a-zA-Z0-9_]*(?:\.[a-zA-Z0-9_]+)+)\s*(?:\?\?\s*"((?:[^"\\]|\\.)*)"\s*)?$"#,
    )
    .unwrap()
});

/// The placeholders in a text, or what's wrong with one.
pub fn placeholders(text: &str) -> std::result::Result<Vec<Placeholder>, String> {
    let mut found = Vec::new();
    for m in ANY.captures_iter(text) {
        let whole = m.get(0).unwrap();
        let Some(parts) = ONE.captures(&m[1]) else {
            return Err(format!("{} is not a valid placeholder", whole.as_str()));
        };
        found.push(Placeholder {
            name: parts[1].to_owned(),
            default: parts
                .get(2)
                .map(|d| d.as_str().replace("\\\"", "\"").replace("\\\\", "\\")),
            start: whole.start(),
            end: whole.end(),
        });
    }
    Ok(found)
}

/// The namespace of a variable name: everything before its first dot.
pub fn namespace(name: &str) -> &str {
    name.split('.').next().unwrap_or(name)
}

/// A configured variable's name: `namespace.field`, not in a built-in namespace.
pub fn variable_name_ok(name: &str) -> bool {
    ONE.captures(name).is_some_and(|c| c[1] == *name)
        && !BUILTIN_NAMESPACES.contains(&namespace(name))
}

/// Whether a name can be looked up: a built-in, any `client.*`, a configured variable, or a name
/// in a plugin's namespace.
pub fn known_variable(
    name: &str,
    configured: &dyn Fn(&str) -> bool,
    plugin_namespaces: &[String],
) -> bool {
    BUILTINS.iter().any(|b| b.name == name)
        || (namespace(name) == "client" && name.matches('.').count() == 1)
        || configured(name)
        || plugin_namespaces.iter().any(|n| n == namespace(name))
}

fn text_list(value: &Value, at: &str) -> std::result::Result<Vec<String>, String> {
    let list = match value {
        Value::String(s) => vec![s.clone()],
        Value::Array(items) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or(format!("{at} must hold strings"))
            })
            .collect::<std::result::Result<_, _>>()?,
        _ => return Err(format!("{at} must be a string or a list of strings")),
    };
    if list.len() > MAX_ALTERNATIVES {
        return Err(format!(
            "{at} has more than {MAX_ALTERNATIVES} alternatives"
        ));
    }
    for text in &list {
        if text.trim().is_empty() {
            return Err(format!("{at} has an empty text"));
        }
        if text.chars().count() > MAX_TEXT_CHARS {
            return Err(format!(
                "{at} has a text longer than {MAX_TEXT_CHARS} characters"
            ));
        }
        placeholders(text).map_err(|e| format!("{at}: {e}"))?;
    }
    Ok(list)
}

/// `text`: a string, a list of alternatives, or either per language.
pub fn greeting_text(value: Option<&Value>, at: &str) -> std::result::Result<GreetingText, String> {
    match value {
        None | Some(Value::Null) => Ok(GreetingText::default()),
        Some(Value::Object(by_language)) => {
            let mut out = Vec::new();
            for (language, texts) in by_language {
                let ok = !language.is_empty()
                    && language.len() <= 16
                    && language
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
                if !ok {
                    return Err(format!("{at}.{language} is not a language tag"));
                }
                out.push((
                    language.clone(),
                    text_list(texts, &format!("{at}.{language}"))?,
                ));
            }
            Ok(GreetingText(out))
        }
        Some(v) => Ok(GreetingText(vec![(String::new(), text_list(v, at)?)])),
    }
}

/// Every placeholder of the instructions and texts names something that can be looked up.
pub fn check_names(
    instructions: Option<&str>,
    text: &GreetingText,
    configured: &dyn Fn(&str) -> bool,
    at: &str,
) -> std::result::Result<(), String> {
    let texts = instructions
        .into_iter()
        .chain(text.texts().map(String::as_str));
    for t in texts {
        for p in placeholders(t)? {
            // A plugin's namespace isn't known while configuration is read, so any name outside a
            // built-in namespace is left to lookup, where an unknown one has no value.
            let builtin_namespace = BUILTIN_NAMESPACES.contains(&namespace(&p.name));
            if builtin_namespace && !known_variable(&p.name, configured, &[]) {
                return Err(format!("{at} names unknown variable {}", p.name));
            }
        }
    }
    Ok(())
}

pub(crate) fn variables(
    r: &Reader,
    value: Option<&Value>,
    at: &str,
) -> Result<BTreeMap<String, VariableConfig>> {
    let mut out = BTreeMap::new();
    for (name, v) in r.map(value, at)? {
        let path = format!("{at}.{name}");
        out.insert(
            name.clone(),
            variable(name, v).map_err(|e| ConfigError(format!("{}: {path}: {e}", r.source)))?,
        );
    }
    if out.len() > MAX_AGENT_VARIABLES {
        return r.fail(
            at,
            &format!("has more than {MAX_AGENT_VARIABLES} variables"),
        );
    }
    Ok(out)
}

/// One tool variable, as an agent or a tenant configures it.
pub fn variable(name: &str, value: &Value) -> std::result::Result<VariableConfig, String> {
    if !variable_name_ok(name) {
        return Err("a variable is named namespace.field, outside the built-in namespaces".into());
    }
    let Some(o) = value.as_object() else {
        return Err("must be an object".into());
    };
    if let Some(key) = o.keys().find(|k| !VARIABLE.contains(&k.as_str())) {
        return Err(format!("{key} is not a known field"));
    }
    let tool = o.get("tool").and_then(Value::as_str).unwrap_or("");
    let owned = tool
        .split_once(':')
        .is_some_and(|(owner, t)| !owner.is_empty() && !t.is_empty() && !t.contains('*'));
    if !owned {
        return Err("tool must name one tool as owner:tool".into());
    }
    let args = o.get("args").cloned().unwrap_or(Value::Object(Raw::new()));
    if !args.is_object() {
        return Err("args must be an object".into());
    }
    if args.to_string().len() > 4096 {
        return Err("args are longer than 4096 bytes".into());
    }
    let pick = o.get("pick").and_then(Value::as_str).unwrap_or("count");
    let pick_ok = !pick.is_empty()
        && pick.len() <= 128
        && pick.split('.').all(|part| {
            let part = part.strip_suffix("[]").unwrap_or(part);
            !part.is_empty() && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
    if !pick_ok {
        return Err("pick must be a path such as count or items[].title".into());
    }
    let ttl_ms = match o.get("ttlMs") {
        None => 60_000,
        Some(v) => v
            .as_u64()
            .filter(|n| *n <= 600_000)
            .ok_or("ttlMs must be 0 to 600000")?,
    };
    let trust = match o.get("trust").and_then(Value::as_str) {
        None | Some("content") => Trust::Content,
        Some("system") => Trust::System,
        Some(_) => return Err("trust must be system or content".into()),
    };
    let description = o
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    if description.len() > 500 {
        return Err("description is longer than 500 bytes".into());
    }
    Ok(VariableConfig {
        name: name.to_owned(),
        tool: tool.to_owned(),
        args,
        pick: pick.to_owned(),
        description,
        ttl_ms,
        trust,
    })
}

pub(crate) fn greeting(
    r: &Reader,
    value: Option<&Value>,
    model_name: &dyn Fn(Option<&Value>, &str) -> Result<String>,
    default_model: &str,
    read_text: &dyn Fn(&str, &str) -> Result<String>,
    configured: &dyn Fn(&str) -> bool,
) -> Result<Option<GreetingConfig>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let g = r.object(value, "greeting", GREETING)?;
    let file = r.optional_string(g.get("instructionsFile"), "greeting.instructionsFile")?;
    let inline = r.optional_string(g.get("instructions"), "greeting.instructions")?;
    let instructions = match (inline, file) {
        (Some(_), Some(_)) => {
            return r.fail("greeting", "has both instructions and instructionsFile");
        }
        (Some(text), None) => Some(text),
        (None, Some(file)) => Some(read_text(&file, "greeting.instructionsFile")?),
        (None, None) => None,
    };
    if instructions
        .as_ref()
        .is_some_and(|i| i.len() > MAX_INSTRUCTIONS_BYTES * 4)
    {
        return r.fail("greeting.instructions", "is longer than 16 KiB");
    }
    let text = greeting_text(g.get("text"), "greeting.text")
        .map_err(|e| ConfigError(format!("{}: {e}", r.source)))?;
    if instructions.is_none() && text.is_empty() {
        return r.fail("greeting", "needs instructions or text");
    }
    check_names(instructions.as_deref(), &text, configured, "greeting")
        .map_err(|e| ConfigError(format!("{}: {e}", r.source)))?;
    let model = match g.get("model") {
        None => default_model.to_owned(),
        m => model_name(m, "greeting.model")?,
    };
    let time_zone = r
        .optional_string(g.get("timeZone"), "greeting.timeZone")?
        .unwrap_or_else(|| "UTC".into());
    let ranged = |key: &str, fallback: u64, range: std::ops::RangeInclusive<u64>| -> Result<u64> {
        let n = r.count(g.get(key), &format!("greeting.{key}"), fallback)?;
        if !range.contains(&n) {
            return r.fail(
                &format!("greeting.{key}"),
                &format!("must be {} to {}", range.start(), range.end()),
            );
        }
        Ok(n)
    };
    Ok(Some(GreetingConfig {
        instructions,
        text,
        model,
        locale: r
            .optional_string(g.get("locale"), "greeting.locale")?
            .unwrap_or_else(|| "en".into()),
        time_zone,
        timeout_ms: ranged("timeoutMs", 3_000, 1_000..=10_000)?,
        variables_timeout_ms: ranged("variablesTimeoutMs", 800, 100..=3_000)?,
        cooldown_ms: ranged("cooldownMs", 120_000, 0..=86_400_000)?,
    }))
}

/// A tenant's greeting and variables, from its agent config. What it doesn't set stays the agent's.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TenantGreeting {
    pub off: bool,
    pub instructions: Option<String>,
    pub text: Option<GreetingText>,
    pub variables: BTreeMap<String, VariableConfig>,
}

/// Reads a tenant's `greeting` and `variables`. Control checks them on publish, so an error here
/// means a version this harness can't use, and the caller keeps the agent's greeting.
pub fn tenant_greeting(content: &Value) -> std::result::Result<TenantGreeting, String> {
    let mut out = TenantGreeting::default();
    if let Some(vars) = content.get("variables").filter(|v| !v.is_null()) {
        let Some(vars) = vars.as_object() else {
            return Err("variables must be an object".into());
        };
        if vars.len() > MAX_TENANT_VARIABLES {
            return Err(format!("more than {MAX_TENANT_VARIABLES} variables"));
        }
        for (name, v) in vars {
            out.variables.insert(
                name.clone(),
                variable(name, v).map_err(|e| format!("variables.{name}: {e}"))?,
            );
        }
    }
    let Some(g) = content.get("greeting").filter(|g| !g.is_null()) else {
        return Ok(out);
    };
    let Some(g) = g.as_object() else {
        return Err("greeting must be an object".into());
    };
    out.off = g.get("off").and_then(Value::as_bool).unwrap_or(false);
    out.instructions = g
        .get("instructions")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned);
    if out
        .instructions
        .as_ref()
        .is_some_and(|i| i.len() > MAX_INSTRUCTIONS_BYTES)
    {
        return Err("greeting.instructions is longer than 4 KiB".into());
    }
    if g.get("text").is_some_and(|t| !t.is_null()) {
        out.text = Some(greeting_text(g.get("text"), "greeting.text")?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_placeholders_with_defaults() {
        let found =
            placeholders(r#"Hi {{user.name ?? "there"}}, {{ time.part }}! {{tasks.overdue??"0"}}"#)
                .unwrap();
        let names: Vec<_> = found
            .iter()
            .map(|p| (p.name.as_str(), p.default.as_deref()))
            .collect();
        assert_eq!(
            names,
            [
                ("user.name", Some("there")),
                ("time.part", None),
                ("tasks.overdue", Some("0"))
            ]
        );
        assert!(placeholders("{{user}}").is_err());
        assert!(placeholders("{{User.name}}").is_err());
        assert!(placeholders("{{user.name ?? there}}").is_err());
        assert_eq!(
            placeholders(r#"{{a.b ?? "say \"hi\""}}"#).unwrap()[0]
                .default
                .as_deref(),
            Some(r#"say "hi""#)
        );
    }

    #[test]
    fn picks_text_by_language() {
        let text = greeting_text(
            Some(&json!({"zh": ["你好 {{user.name}}", "你好"], "en": "Hi"})),
            "text",
        )
        .unwrap();
        assert_eq!(
            text.for_locale("zh-Hans", "en"),
            ["你好 {{user.name}}", "你好"]
        );
        assert_eq!(text.for_locale("en-US", "zh"), ["Hi"]);
        assert_eq!(text.for_locale("fr", "en"), ["Hi"]);
        let plain = greeting_text(Some(&json!("Hello")), "text").unwrap();
        assert_eq!(plain.for_locale("zh", "en"), ["Hello"]);
    }

    #[test]
    fn checks_variables() {
        assert!(
            variable(
                "tasks.overdue",
                &json!({"tool": "collab:work_list", "pick": "count"})
            )
            .is_ok()
        );
        assert!(
            variable("user.tasks", &json!({"tool": "collab:work_list"})).is_err(),
            "built-in namespace"
        );
        assert!(variable("tasks.x", &json!({"tool": "collab:*"})).is_err());
        assert!(
            variable(
                "tasks.x",
                &json!({"tool": "collab:work_list", "pick": "items[].title"})
            )
            .is_ok()
        );
        assert!(
            variable(
                "tasks.x",
                &json!({"tool": "collab:work_list", "pick": "items[0]"})
            )
            .is_err()
        );
        assert!(
            variable(
                "tasks.x",
                &json!({"tool": "collab:work_list", "write": true})
            )
            .is_err()
        );
        let names = |n: &str| n == "tasks.overdue";
        let text = GreetingText::default();
        assert!(
            check_names(
                Some("{{user.name}} {{tasks.overdue}} {{client.screen}}"),
                &text,
                &names,
                "g"
            )
            .is_ok()
        );
        assert!(check_names(Some("{{user.nickname}}"), &text, &names, "g").is_err());
    }

    #[test]
    fn reads_a_tenant_greeting() {
        let t = tenant_greeting(&json!({
            "greeting": {"text": {"zh": "您好"}, "off": false},
            "variables": {"deals.open": {"tool": "crm:deal_list"}}
        }))
        .unwrap();
        assert_eq!(t.text.unwrap().for_locale("zh", "zh"), ["您好"]);
        assert!(t.variables.contains_key("deals.open"));
        assert!(tenant_greeting(&json!({"greeting": {"instructions": "x".repeat(5000)}})).is_err());
        assert_eq!(
            tenant_greeting(&json!({})).unwrap(),
            TenantGreeting::default()
        );
    }
}
