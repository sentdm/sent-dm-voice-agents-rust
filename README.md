# Sent Voice Agents for Rust

An AI agent that takes and places phone calls through [Sent](https://sent.dm), on the
[Sent Voice Agents protocol](https://github.com/sentdm/sent-dm-voice-protocol) 0.2.0, on Tokio.

Status: Preview, with the protocol. Generated and rendered by `sent-dm-sdk-pipeline` from the protocol at
`v0.2.0`; every release passes the protocol's conformance scenarios in CI.

Read the [Voice Agents docs](https://docs.sent.dm) to get started.

## Install

```sh
cargo add sentdm-voice-agents tokio --features tokio/full
```

Rust 1.85 or later. The runtime is Tokio; the connection is `tokio-tungstenite` with rustls, the frames are
`serde_json`.

## Quickstart

```rust
use sentdm_voice_agents::{Agent, Handler, Options, Session, UserSaid};
use std::sync::Arc;

struct Support;

impl Handler for Support {
    async fn on_session_start(&self, session: Arc<Session>) {
        session.say("Hi, this is Acme support. How can I help?").ok();
    }

    async fn on_user_said(&self, session: Arc<Session>, said: UserSaid) {
        if said.text.contains("person") {
            session.say("Transferring you now.").ok();
            session.transfer_number("+14155550100").await.ok();
        } else {
            session.say(&format!("You said {}", said.text)).ok();
        }
    }
}

#[tokio::main]
async fn main() {
    let options = Options::new(std::env::var("SENT_API_KEY").unwrap(), "agent_...");
    Agent::new(options, Support).serve().await.unwrap();
}
```

`serve()` connects to Sent with the account API key and serves calls until SIGINT or SIGTERM; `run()` does the
same without the signal handlers, and `stop()` ends it. Each session's handlers run in order on their own task,
so sessions never wait for one another. The runtime answers heartbeats, opens a new connection when one drains
or is lost, and resumes the sessions that can move; the handlers only see calls.

| Handler                              | When                                                                                           |
| ------------------------------------ | ---------------------------------------------------------------------------------------------- |
| `on_session_start(session)`          | A call reached the agent. In a text session, `say` to speak                                    |
| `on_user_said(session, said)`        | Text sessions: the caller said something; `said.correction` means it replaces the previous one |
| `on_audio(session, samples)`         | Audio sessions: the caller's audio, 16-bit mono PCM at `session.sample_rate`                   |
| `on_tool_result(session, result)`    | Sent answered a tool call; the waiting `tool` call gets the same result                        |
| `on_session_end(session, end)`       | The call left the agent; `end.reason` says why                                                 |
| `on_session_lost(session)`           | The connection was lost and the session could not be resumed                                   |

On a `Session`: `say(text)`, `say_with(text, last, interruptible)`, `clear()`, `send_audio(samples)`, and the
tools `transfer(party)`, `transfer_number(number)`, `hangup()`, `send_dtmf(digits)`, `hold(held)`, each
resolving with Sent's `ToolResult`. `session.from`, `to`, `direction`, `session_type`, `language`, `context`,
`sample_rate` carry what `session.start` said.

`sentdm_voice_agents::frames` holds every frame as a type, with `decode` and `encode` that check the
protocol's rules; it is generated from the protocol and not edited by hand.

## Develop

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings   # formatting, lints
cargo test                                                       # the tests against the protocol's fixtures
cargo build --release --bin conformance                          # the conformance agent the protocol's action runs
```

Releases are cut by release-please from `main` and published to crates.io from GitHub Actions by trusted
publishing.
