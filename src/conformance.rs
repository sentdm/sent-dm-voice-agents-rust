//! The conformance agent of sent-dm-voice-protocol's conformance/README.md, on this SDK's API. The protocol's
//! harness runs it in CI before every release, through the `conformance` binary: it connects to
//! SENT_CONFORMANCE_URL with any API key and does exactly what the README's table says.

use crate::agent::Handler;
use crate::frames::{ToolResult, UserSaid};
use crate::session::{Error, Session};
use serde_json::{json, Map, Value};
use std::sync::Arc;

/// What the conformance agent says first in a text session.
pub const GREETING: &str = "Hello from the conformance agent.";

/// One action the conformance agent performs for a user.said.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Say {
        text: String,
        last: Option<bool>,
    },
    Tool {
        name: String,
        arguments: Option<Map<String, Value>>,
    },
}

fn object(value: Value) -> Option<Map<String, Value>> {
    match value {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// What a user.said asks for: the actions the agent performs in order. A command is the text up to the first
/// space; `stream` takes the next word as the first piece.
pub fn decide(text: &str) -> Vec<Step> {
    let (command, rest) = text.split_once(' ').unwrap_or((text, ""));
    match command {
        "echo" => vec![Step::Say {
            text: rest.to_string(),
            last: None,
        }],
        "stream" => {
            let (word, remainder) = rest.split_once(' ').unwrap_or((rest, ""));
            vec![
                Step::Say {
                    text: word.to_string(),
                    last: Some(false),
                },
                Step::Say {
                    text: remainder.to_string(),
                    last: None,
                },
            ]
        }
        "transfer" => vec![Step::Tool {
            name: "transfer".into(),
            arguments: object(json!({"to": {"kind": "user", "value": "conformance-desk"}})),
        }],
        "hangup" => vec![Step::Tool {
            name: "hangup".into(),
            arguments: None,
        }],
        "dtmf" => vec![Step::Tool {
            name: "send_dtmf".into(),
            arguments: object(json!({"digits": rest})),
        }],
        "hold" => vec![Step::Tool {
            name: "hold".into(),
            arguments: object(json!({"held": true})),
        }],
        _ => Vec::new(),
    }
}

/// The conformance agent's behaviour.
pub struct ConformanceHandler;

impl Handler for ConformanceHandler {
    async fn on_session_start(&self, session: Arc<Session>) {
        if session.session_type == "text" {
            let _ = session.say(GREETING);
        }
    }

    async fn on_user_said(&self, session: Arc<Session>, said: UserSaid) {
        if said.correction && session.clear() == Err(Error::SessionEnded) {
            return;
        }
        for step in decide(&said.text) {
            let result = match step {
                Step::Say { text, last } => session.say_with(&text, last, None),
                // The harness answers every tool call; the result also reaches on_tool_result.
                Step::Tool { name, arguments } => session.tool(&name, arguments).await.map(|_| ()),
            };
            if result == Err(Error::SessionEnded) {
                return;
            }
        }
    }

    async fn on_tool_result(&self, session: Arc<Session>, result: ToolResult) {
        if !result.ok {
            if let Some(error) = result.error {
                let _ = session.say(&format!("Tool failed: {}", error.code));
            }
        }
    }

    async fn on_audio(&self, session: Arc<Session>, samples: Vec<u8>) {
        let _ = session.send_audio(&samples);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands() {
        assert_eq!(
            decide("echo hello there"),
            [Step::Say {
                text: "hello there".into(),
                last: None
            }]
        );
        assert_eq!(
            decide("stream Let me think"),
            [
                Step::Say {
                    text: "Let".into(),
                    last: Some(false)
                },
                Step::Say {
                    text: "me think".into(),
                    last: None
                }
            ]
        );
        assert_eq!(
            decide("hangup"),
            [Step::Tool {
                name: "hangup".into(),
                arguments: None
            }]
        );
        assert_eq!(
            decide("dtmf 12#"),
            [Step::Tool {
                name: "send_dtmf".into(),
                arguments: object(json!({"digits": "12#"}))
            }]
        );
        assert_eq!(
            decide("hold"),
            [Step::Tool {
                name: "hold".into(),
                arguments: object(json!({"held": true}))
            }]
        );
        assert!(matches!(&decide("transfer")[..], [Step::Tool { name, .. }] if name == "transfer"));
        assert!(decide("something else").is_empty());
    }
}
