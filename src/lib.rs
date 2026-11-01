//! Sent Voice Agents for Rust: an AI agent that takes and places phone calls through Sent.
//!
//! ```no_run
//! use sentdm_voice_agents::{Agent, Handler, Options, Session, UserSaid};
//! use std::sync::Arc;
//!
//! struct Support;
//!
//! impl Handler for Support {
//!     async fn on_session_start(&self, session: Arc<Session>) {
//!         session.say("Hi, this is Acme support. How can I help?").ok();
//!     }
//!
//!     async fn on_user_said(&self, session: Arc<Session>, said: UserSaid) {
//!         if said.text.contains("person") {
//!             session.say("Transferring you now.").ok();
//!             session.transfer_number("+14155550100").await.ok();
//!         } else {
//!             session.say(&format!("You said {}", said.text)).ok();
//!         }
//!     }
//! }
//!
//! # async fn run() -> Result<(), sentdm_voice_agents::Refused> {
//! let agent = Agent::new(Options::new(std::env::var("SENT_API_KEY").unwrap(), "agent_..."), Support);
//! agent.serve().await
//! # }
//! ```
//!
//! The agent connects to Sent with the account API key and serves calls until stopped. [`frames`] holds the
//! protocol's frames as types, generated from sent-dm-voice-protocol.

pub mod agent;
pub mod audio;
pub mod conformance;
pub mod frames;
pub mod session;

pub use agent::{default_url, Agent, Handler, Options, Refused};
pub use audio::{audio_frame, bytes_per_frame, samples_of, stream_of};
pub use frames::{Frame, FrameError, Party, SessionEnd, ToolResult, UserSaid, PROTOCOL_VERSION};
pub use session::{Error, Session};

/// This crate's own version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
