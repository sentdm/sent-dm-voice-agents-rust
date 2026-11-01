//! The conformance agent of sent-dm-voice-protocol, as a program: the protocol's harness runs it with
//! SENT_CONFORMANCE_URL set, in CI before every release, and by hand against a harness on localhost.

use sentdm_voice_agents::conformance::ConformanceHandler;
use sentdm_voice_agents::{Agent, Options};

#[tokio::main]
async fn main() {
    let url = std::env::var("SENT_CONFORMANCE_URL")
        .unwrap_or_else(|_| "ws://localhost:8765/v1/agents/connect".into());
    let agent = Agent::new(
        Options::new("conformance", "agent_conformance").with_url(url),
        ConformanceHandler,
    );
    if let Err(error) = agent.serve().await {
        eprintln!("the agent stopped: {error}");
        std::process::exit(1);
    }
}
