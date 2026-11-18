//! agent-pi in Rust: transport, auth, plugins and the session worker. Configuration is read only
//! through `agent-ti-config`'s config sources.
pub mod adapters;
pub mod agents;
pub mod auth;
pub mod catalog;
pub mod children;
pub mod compaction;
pub mod control_copy;
pub mod docs;
pub mod events;
pub mod flights;
pub mod flow;
pub mod grants;
pub mod greeting;
pub mod maintenance;
pub mod metrics;
pub mod plugins;
pub mod progress;
pub mod server;
pub mod speech;
pub mod tasks;
pub mod tenant_config;
pub mod tenant_connectors;
pub mod tenant_skills;
pub mod variables;
pub mod worker;
