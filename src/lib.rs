//! agent-pi in Rust: transport, auth, plugins and the session worker. Configuration is read only
//! through `agent-ti-config`'s config sources.
pub mod adapters;
pub mod agents;
pub mod auth;
pub mod catalog;
pub mod children;
pub mod compaction;
pub mod events;
pub mod flow;
pub mod grants;
pub mod maintenance;
pub mod metrics;
pub mod plugins;
pub mod server;
pub mod speech;
pub mod tasks;
pub mod tenant_config;
pub mod tenant_connectors;
pub mod tenant_skills;
pub mod worker;
