//! The binary sandbox images run. It serves only the spare and host roles, so it links none of
//! the control plane: under gVisor every sandbox keeps its own resident copy of this file.
use clap::Parser;
use durable_actors::entrypoint::{init_logging, shutdown_signal};
use tracing::error;

#[derive(Parser)]
#[command(name = "durable-actors", version)]
struct Cli {}

#[tokio::main]
async fn main() {
    Cli::parse();
    init_logging(std::env::var("DURABLE_ACTORS_LOG_MODE").as_deref() == Ok("development"));
    let role = std::env::var("DURABLE_ACTORS_PROCESS_ROLE").unwrap_or_else(|_| "host".into());
    if let Err(error) = durable_actors::host::serve_role(&role, shutdown_signal()).await {
        error!(error = %format!("{error:#}"), "durable-actors process failed");
        std::process::exit(1);
    }
}
