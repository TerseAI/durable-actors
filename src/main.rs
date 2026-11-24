use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_actors::{
    control_plane::{
        ControlPlaneProcessConfig, DevOptions, serve_control_plane, serve_local, serve_pool_manager,
    },
    entrypoint::{init_logging, shutdown_signal},
};
use tracing::error;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let development_logs = cli.command.is_some()
        || std::env::var("DURABLE_ACTORS_LOG_MODE").as_deref() == Ok("development");
    init_logging(development_logs);
    if let Err(error) = run(cli).await {
        if development_logs {
            error!(error = %format!("{error:#}"), "local actor runtime failed");
        } else {
            error!(error = %format!("{error:#}"), "durable-actors process failed");
        }
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    if let Some(Commands::Dev(options)) = cli.command {
        return serve_local(options, shutdown_signal()).await;
    }
    let shutdown = shutdown_signal();
    match std::env::var("DURABLE_ACTORS_PROCESS_ROLE")
        .as_deref()
        .unwrap_or("host")
    {
        "control_plane" => {
            serve_control_plane(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        "pool_manager" => {
            serve_pool_manager(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        "node_cache" => durable_actors::node_cache::serve_node_cache(shutdown).await,
        role => durable_actors::host::serve_role(role, shutdown).await,
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "Run durable TypeScript and Python actors locally or in the cloud"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start local actors with persistent file storage")]
    Dev(DevOptions),
}
