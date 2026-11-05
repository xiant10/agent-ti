use std::{error::Error, path::Path};

#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let filter = |level: &str| {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level))
    };
    let host = match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["serve", file] | ["check", file] => {
            match agent_ti_config::load_host(Path::new(file), &agent_ti_config::process_env) {
                Ok(host) => Some(host),
                Err(e) => {
                    eprintln!("agent-ti: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => None,
    };
    let level = host
        .as_ref()
        .map_or("info", |h| h.log_level.as_str())
        .to_owned();
    tracing_subscriber::fmt()
        .with_env_filter(filter(&level))
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    if let Err(e) = run(&args, host).await {
        eprintln!("agent-ti: {e}");
        std::process::exit(1);
    }
}
async fn run(
    args: &[String],
    host: Option<agent_ti_config::HostConfig>,
) -> Result<(), Box<dyn Error>> {
    match (args.first().map(String::as_str), host) {
        (Some("serve"), Some(host)) => agent_ti::server::start(host).await?,
        (Some("check"), Some(host)) => {
            let client = reqwest::Client::new();
            for agent in agent_ti::agents::load_all(&host, &client).await? {
                println!(
                    "{} version={} profiles={:?} plugins={:?}",
                    agent.name,
                    agent.version,
                    agent.config.profiles.keys().collect::<Vec<_>>(),
                    agent.plugins.iter().map(|p| p.name()).collect::<Vec<_>>()
                );
            }
        }
        _ => return Err("usage: agent-ti serve <host.json> | check <host.json>".into()),
    }
    Ok(())
}
