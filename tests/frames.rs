//! The generated frames against the protocol's own fixtures, copied here by the lane at render time.

use sentdm_voice_agents::frames::{self, Frame, FrameError};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn fixtures(kind: &str) -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(kind);
    let mut files: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("no {kind} fixtures at {}: {e}", root.display()))
        .flatten()
        .filter(|dir| dir.path().is_dir())
        .flat_map(|dir| fs::read_dir(dir.path()).unwrap().flatten().map(|f| f.path()))
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no {kind} fixtures");
    files
}

#[test]
fn valid_frames_round_trip() {
    for file in fixtures("valid") {
        let data = fs::read_to_string(&file).unwrap();
        let frame = frames::decode(&data).unwrap_or_else(|e| panic!("{}: refused: {e}", file.display()));
        assert!(
            !matches!(frame, Frame::Unknown(_)),
            "{}: decoded as unknown",
            file.display()
        );
        let original: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(frame.frame_type(), original["type"], "{}", file.display());
        let again = frames::encode(&frame).unwrap_or_else(|e| panic!("{}: encode: {e}", file.display()));
        for (key, value) in original.as_object().unwrap() {
            assert_eq!(&again[key], value, "{}: {key} changed on the way back", file.display());
        }
        let decoded = frames::decode_value(again.clone()).unwrap();
        assert_eq!(
            frames::encode(&decoded).unwrap(),
            again,
            "{}: a second round changed the frame",
            file.display()
        );
    }
}

#[test]
fn invalid_frames_are_refused() {
    for file in fixtures("invalid") {
        let data = fs::read_to_string(&file).unwrap();
        let refused: Result<Frame, FrameError> = frames::decode(&data);
        assert!(
            refused.is_err(),
            "{}: accepted although the schema refuses it",
            file.display()
        );
    }
}

#[test]
fn unknown_frames_and_fields_pass() {
    let frame = frames::decode(r#"{"type":"session.note","session_id":"vs_1","x":1}"#).unwrap();
    assert_eq!(frame.frame_type(), "session.note");
    assert_eq!(frame.session_id(), Some("vs_1"));
    assert_eq!(
        frames::encode_to_string(&frame).unwrap(),
        r#"{"type":"session.note","session_id":"vs_1","x":1}"#
    );
    match frames::decode(r#"{"type":"user.said","session_id":"vs_1","text":"hi","mood":"good"}"#).unwrap() {
        Frame::UserSaid(said) => assert!(!said.correction),
        other => panic!("{other:?}"),
    }
    match frames::decode(r#"{"type":"session.end","session_id":"vs_1","reason":"added_later","duration_ms":1}"#)
        .unwrap()
    {
        Frame::SessionEnd(end) => assert_eq!(end.reason, "added_later"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn defaults_and_facts() {
    match frames::decode(r#"{"type":"agent.say","session_id":"vs_1","text":"hi"}"#).unwrap() {
        Frame::AgentSay(say) => assert!(say.last == Some(true) && say.interruptible == Some(true)),
        other => panic!("{other:?}"),
    }
    let say = frames::AgentSay {
        session_id: "vs_1".into(),
        text: "hi, +1 <you>".into(),
        interruptible: None,
        last: None,
    };
    assert_eq!(
        frames::encode_to_string(&Frame::AgentSay(say)).unwrap(),
        r#"{"type":"agent.say","session_id":"vs_1","text":"hi, +1 <you>"}"#
    );
    assert_eq!(frames::SUBPROTOCOL, "sent.voice.v1");
    assert_eq!(frames::RETAINED_FRAMES, 32);
    assert!(frames::UNCOUNTED.contains(&"session.start") && frames::UNCOUNTED.contains(&"error"));
    assert!(!frames::UNCOUNTED.contains(&"agent.say"));
    assert!(frames::FROM_AGENT.contains(&"pong"));
    assert_eq!(frames::session_types("agent.say"), Some(&["text"][..]));
}

#[test]
fn encode_refuses_a_bad_frame() {
    let say = frames::AgentSay {
        session_id: "nope".into(),
        text: "hi".into(),
        interruptible: None,
        last: None,
    };
    assert!(frames::encode(&Frame::AgentSay(say)).is_err());
    let call = frames::ToolCall {
        session_id: "vs_1".into(),
        call_ref: "t1".into(),
        name: "transfer".into(),
        arguments: None,
    };
    assert!(frames::encode(&Frame::ToolCall(call)).is_err());
    assert!(frames::decode("not json").is_err());
    assert!(frames::decode(r#"{"type":"ping","ts":1.0}"#).is_err());
}
