//! One session: a call while this agent is on it.
//!
//! A [`Session`] is what the handlers of an agent receive. It carries what session.start said about the call,
//! sends the agent's frames on whichever connection the session is on, keeps the latest counted frames so a
//! resume can send again the ones Sent did not read, and turns a tool call into the tool.result that answers it.

use crate::audio::audio_frame;
use crate::frames::{
    self, AgentClear, AgentSay, Frame, FrameError, Party, SessionEnd, SessionResume,
    SessionResumed, SessionStart, ToolCall, ToolResult, RETAINED_FRAMES, UNCOUNTED,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{mpsc, oneshot};

/// What a session's methods can fail with.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The frame breaks the protocol; it was refused before it counted.
    Frame(FrameError),
    /// The call has left the agent: session.end arrived, or the session was lost.
    SessionEnded,
    /// Audio was sent on a text session.
    TextSession,
    /// The samples are not whole 16-bit samples.
    Audio(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Frame(e) => write!(f, "{e}"),
            Error::SessionEnded => f.write_str("the session has ended"),
            Error::TextSession => f.write_str("the session is a text session"),
            Error::Audio(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for Error {}

impl From<FrameError> for Error {
    fn from(e: FrameError) -> Self {
        Error::Frame(e)
    }
}

/// The connection a session sends on: the agent gives each session one, and moves it on a resume.
pub(crate) trait Sender: Send + Sync {
    fn send_text(&self, kind: &str, data: String);
    fn send_bytes(&self, frame: Vec<u8>);
}

/// What a session's handlers see, in the order it arrived.
pub(crate) enum Item {
    Frame(Box<Frame>),
    Audio(Vec<u8>),
    Lost,
}

struct State {
    stream: Option<u16>,
    token: Option<String>,
    window_ms: Option<i64>,
    received: i64,
    end: Option<SessionEnd>,
    lost: bool,
    conn: Arc<dyn Sender>,
    resuming: bool,
    kept: Vec<Frame>,
    before: i64,
    calls: u64,
    pending: HashMap<String, oneshot::Sender<ToolResult>>,
}

/// The call the agent is on, and the ways to act on it. The agent creates it; its handlers run in order.
pub struct Session {
    pub id: String,
    pub call_id: String,
    pub agent_id: String,
    /// inbound or outbound.
    pub direction: String,
    pub from: Party,
    pub to: Party,
    /// text or audio.
    pub session_type: String,
    pub language: Option<String>,
    pub context: Map<String, Value>,
    pub started_at: String,
    /// Hz of the session's 16-bit mono PCM, both ways, in an audio session.
    pub sample_rate: u32,
    /// What session.start said about the call.
    pub start: SessionStart,
    state: Mutex<State>,
    pub(crate) inbox: mpsc::UnboundedSender<Item>,
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Session({}, {}, {})",
            self.id, self.session_type, self.direction
        )
    }
}

impl Session {
    pub(crate) fn new(
        start: SessionStart,
        conn: Arc<dyn Sender>,
    ) -> (Arc<Session>, mpsc::UnboundedReceiver<Item>) {
        let (inbox, items) = mpsc::unbounded_channel();
        let session = Session {
            id: start.session_id.clone(),
            call_id: start.call_id.clone(),
            agent_id: start.agent_id.clone(),
            direction: start.direction.clone(),
            from: start.from.clone(),
            to: start.to.clone(),
            session_type: start.session_type.clone(),
            language: start.language.clone(),
            context: start.context.clone().unwrap_or_default(),
            started_at: start.started_at.clone(),
            sample_rate: start
                .audio
                .as_ref()
                .and_then(|a| a.sample_rate)
                .unwrap_or(16000) as u32,
            state: Mutex::new(State {
                stream: start.audio.as_ref().map(|a| a.stream as u16),
                token: start.resume.as_ref().map(|r| r.token.clone()),
                window_ms: start.resume.as_ref().map(|r| r.window_ms),
                received: 0,
                end: None,
                lost: false,
                conn,
                resuming: false,
                kept: Vec::new(),
                before: 0,
                calls: 0,
                pending: HashMap::new(),
            }),
            start,
            inbox,
        };
        (Arc::new(session), items)
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // ----- what the agent reads about the call -----

    /// The number in the header of the session's binary audio frames on its current connection.
    pub fn stream(&self) -> Option<u16> {
        self.state().stream
    }

    /// The latest resume token, when the session can be resumed. Keep it out of logs.
    pub fn token(&self) -> Option<String> {
        self.state().token.clone()
    }

    /// Whether the session survives a lost connection: it has a resume token.
    pub fn resumable(&self) -> bool {
        self.state().token.is_some()
    }

    /// How long Sent keeps the session for a resume after its connection is lost.
    pub fn window_ms(&self) -> Option<i64> {
        self.state().window_ms
    }

    /// How many of Sent's counted frames for the session the agent has read: the `received` of a resume.
    pub fn received(&self) -> i64 {
        self.state().received
    }

    /// How many counted frames the agent has sent for the session.
    pub fn sent(&self) -> i64 {
        let state = self.state();
        state.before + state.kept.len() as i64
    }

    /// session.end, once it arrived.
    pub fn end(&self) -> Option<SessionEnd> {
        self.state().end.clone()
    }

    /// Whether the connection was lost and the session could not be resumed: Sent ends the call without a
    /// session.end.
    pub fn lost(&self) -> bool {
        self.state().lost
    }

    /// Whether the call has left the agent: session.end arrived, or the session was lost.
    pub fn ended(&self) -> bool {
        let state = self.state();
        state.end.is_some() || state.lost
    }

    // ----- what the agent does on the call -----

    /// Text sessions: speak `text` to the caller.
    pub fn say(&self, text: &str) -> Result<(), Error> {
        self.say_with(text, None, None)
    }

    /// Text sessions: speak `text`, in pieces when `last` is `Some(false)`, finished by a piece with
    /// `Some(true)`; the pieces are spoken as one utterance. `interruptible` is whether the caller speaking stops
    /// this speech.
    pub fn say_with(
        &self,
        text: &str,
        last: Option<bool>,
        interruptible: Option<bool>,
    ) -> Result<(), Error> {
        self.send(Frame::AgentSay(AgentSay {
            session_id: self.id.clone(),
            text: text.to_string(),
            interruptible,
            last,
        }))
    }

    /// Drop the speech or audio the agent sent that Sent has not played yet.
    pub fn clear(&self) -> Result<(), Error> {
        self.send(Frame::AgentClear(AgentClear {
            session_id: self.id.clone(),
        }))
    }

    /// Audio sessions: play `samples`, 16-bit little-endian mono PCM at the session's sample rate. Send 20 ms per
    /// call, and no more than 200 ms. Fails with [`Error::SessionEnded`] once the call has left the agent.
    pub fn send_audio(&self, samples: &[u8]) -> Result<(), Error> {
        let state = self.state();
        if state.end.is_some() || state.lost {
            return Err(Error::SessionEnded);
        }
        let Some(stream) = state.stream else {
            return Err(Error::TextSession);
        };
        // The stream number belongs to the old connection; on the new one it may be another caller's until
        // session.resumed assigns this session's. Audio is not counted, so there is nothing to send again.
        if state.resuming {
            return Ok(());
        }
        let frame = audio_frame(stream, samples).map_err(Error::Audio)?;
        state.conn.send_bytes(frame);
        Ok(())
    }

    /// Move the call to another party. On success the session then ends with `transferred`.
    pub async fn transfer(&self, to: &Party) -> Result<ToolResult, Error> {
        let mut party = Map::new();
        party.insert("kind".into(), Value::String(to.kind.clone()));
        if let Some(value) = &to.value {
            party.insert("value".into(), Value::String(value.clone()));
        }
        let mut arguments = Map::new();
        arguments.insert("to".into(), Value::Object(party));
        self.tool("transfer", Some(arguments)).await
    }

    /// Move the call to an E.164 number.
    pub async fn transfer_number(&self, number: &str) -> Result<ToolResult, Error> {
        self.transfer(&Party {
            kind: "number".into(),
            value: Some(number.to_string()),
        })
        .await
    }

    /// End the call, after the speech the agent queued before it.
    pub async fn hangup(&self) -> Result<ToolResult, Error> {
        self.tool("hangup", None).await
    }

    /// Play `digits` on the call: 0 to 9, *, #, A to D, and w to wait half a second.
    pub async fn send_dtmf(&self, digits: &str) -> Result<ToolResult, Error> {
        let mut arguments = Map::new();
        arguments.insert("digits".into(), Value::String(digits.to_string()));
        self.tool("send_dtmf", Some(arguments)).await
    }

    /// Put the caller on hold, or take them off it.
    pub async fn hold(&self, held: bool) -> Result<ToolResult, Error> {
        let mut arguments = Map::new();
        arguments.insert("held".into(), Value::Bool(held));
        self.tool("hold", Some(arguments)).await
    }

    /// Call a tool by name and wait for its tool.result. Fails with [`Error::SessionEnded`] if the session ends
    /// first.
    pub async fn tool(
        &self,
        name: &str,
        arguments: Option<Map<String, Value>>,
    ) -> Result<ToolResult, Error> {
        let (answer, waiting) = oneshot::channel();
        let call_ref = {
            let mut state = self.state();
            if state.end.is_some() || state.lost {
                return Err(Error::SessionEnded);
            }
            state.calls += 1;
            let call_ref = format!("t{}", state.calls);
            state.pending.insert(call_ref.clone(), answer);
            call_ref
        };
        let frame = Frame::ToolCall(ToolCall {
            session_id: self.id.clone(),
            call_ref: call_ref.clone(),
            name: name.to_string(),
            arguments,
        });
        if let Err(error) = self.send(frame) {
            self.state().pending.remove(&call_ref);
            return Err(error);
        }
        waiting.await.map_err(|_| Error::SessionEnded)
    }

    // ----- what the agent's connection does with the session -----

    /// Sends a frame for the session on its connection, keeping it if it counts. A frame that breaks the
    /// protocol is refused before it counts. While a resume is unanswered the frame is kept and not sent: the
    /// replay after session.resumed sends it, once, on the connection the session is on. Keeping and sending
    /// happen under one lock, so the wire order is the kept order.
    pub(crate) fn send(&self, frame: Frame) -> Result<(), Error> {
        let data = frames::encode_to_string(&frame)?;
        let mut state = self.state();
        if state.end.is_some() || state.lost {
            return Err(Error::SessionEnded);
        }
        keep(&mut state, &frame);
        if state.resuming {
            return Ok(());
        }
        state.conn.send_text(frame.frame_type(), data);
        Ok(())
    }

    /// Counts one of Sent's frames for the session, as Counting frames in docs/protocol.md says.
    pub(crate) fn read(&self, frame: &Frame) {
        if !UNCOUNTED.contains(&frame.frame_type()) {
            self.state().received += 1;
        }
    }

    pub(crate) fn resume_frame(&self) -> Option<SessionResume> {
        let state = self.state();
        state.token.as_ref().map(|token| SessionResume {
            session_id: self.id.clone(),
            token: token.clone(),
            received: state.received,
        })
    }

    pub(crate) fn resuming(&self) -> bool {
        self.state().resuming
    }

    pub(crate) fn move_to(&self, conn: Arc<dyn Sender>) {
        let mut state = self.state();
        state.conn = conn;
        state.resuming = true;
    }

    pub(crate) fn resume_refused(&self) {
        self.state().resuming = false;
    }

    /// Takes the answer to a session.resume and sends again, on the session's connection, the frames Sent did
    /// not read, or a hangup when Sent needs one the session no longer keeps. Returns how many it sent. The
    /// replay and the end of the resume happen under the session's lock, so nothing a handler sends can slip
    /// between.
    pub(crate) fn resumed(&self, frame: &SessionResumed) -> usize {
        let mut state = self.state();
        state.token = frame.resume.as_ref().map(|r| r.token.clone());
        if let Some(audio) = &frame.audio {
            state.stream = Some(audio.stream as u16);
        }
        // Without a next token the session ended while it was detached; its replay ends with session.end.
        if frame.resume.is_none() {
            state.resuming = false;
            return 0;
        }
        let again: Vec<Frame> = if frame.received < state.before {
            state.calls += 1;
            vec![Frame::ToolCall(ToolCall {
                session_id: self.id.clone(),
                call_ref: format!("t{}", state.calls),
                name: "hangup".into(),
                arguments: None,
            })]
        } else {
            // A received past what was sent is Sent's mistake; there is nothing to send again for it.
            let read = ((frame.received - state.before) as usize).min(state.kept.len());
            state.kept[read..].to_vec()
        };
        state.before = frame.received;
        state.kept.clear();
        for frame_again in &again {
            keep(&mut state, frame_again);
            if let Ok(data) = frames::encode_to_string(frame_again) {
                state.conn.send_text(frame_again.frame_type(), data);
            }
        }
        state.resuming = false;
        again.len()
    }

    pub(crate) fn answer_tool(&self, result: &ToolResult) {
        let answer = self.state().pending.remove(&result.call_ref);
        if let Some(answer) = answer {
            let _ = answer.send(result.clone());
        }
    }

    pub(crate) fn finish(&self, end: SessionEnd) {
        let mut state = self.state();
        state.end = Some(end);
        state.pending.clear();
    }

    /// The session is lost: no tool.result or session.end will come, so every waiting tool call fails, and
    /// anything sent from now on is refused.
    pub(crate) fn abandon(&self) {
        let mut state = self.state();
        state.lost = true;
        state.resuming = false;
        state.pending.clear();
    }
}

/// Runs under the session's lock.
fn keep(state: &mut State, frame: &Frame) {
    if UNCOUNTED.contains(&frame.frame_type()) {
        return;
    }
    state.kept.push(frame.clone());
    if state.kept.len() > RETAINED_FRAMES {
        state.kept.remove(0);
        state.before += 1;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// A connection that records what it sends.
    #[derive(Default)]
    pub(crate) struct Recorder {
        pub frames: StdMutex<Vec<Frame>>,
        pub audio: StdMutex<Vec<Vec<u8>>>,
    }

    impl Recorder {
        pub fn texts(&self) -> Vec<String> {
            self.frames
                .lock()
                .unwrap()
                .iter()
                .filter_map(|f| match f {
                    Frame::AgentSay(s) => Some(s.text.clone()),
                    Frame::ToolCall(c) => Some(c.name.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    impl Sender for Recorder {
        fn send_text(&self, _kind: &str, data: String) {
            self.frames
                .lock()
                .unwrap()
                .push(frames::decode(&data).expect("a valid frame"));
        }

        fn send_bytes(&self, frame: Vec<u8>) {
            self.audio.lock().unwrap().push(frame);
        }
    }

    pub(crate) fn start(extra: &str) -> SessionStart {
        let data = format!(
            r#"{{"type":"session.start","session_id":"vs_1","call_id":"call_1","agent_id":"agent_1","direction":"inbound","from":{{"kind":"number","value":"+14155550100"}},"to":{{"kind":"agent","value":"agent_1"}},"session_type":"text","started_at":"2026-10-31T00:00:00Z","resume":{{"token":"vsr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","window_ms":15000}}{extra}}}"#
        );
        match frames::decode(&data).unwrap() {
            Frame::SessionStart(s) => s,
            other => panic!("{other:?}"),
        }
    }

    fn resumed(received: i64, token: char) -> SessionResumed {
        let data = format!(
            r#"{{"type":"session.resumed","session_id":"vs_1","received":{received},"resume":{{"token":"vsr_{}","window_ms":15000}}}}"#,
            token.to_string().repeat(30)
        );
        match frames::decode(&data).unwrap() {
            Frame::SessionResumed(s) => s,
            other => panic!("{other:?}"),
        }
    }

    pub(crate) fn decode(data: &str) -> Frame {
        frames::decode(data).unwrap()
    }

    fn session(extra: &str) -> (Arc<Session>, Arc<Recorder>) {
        let recorder = Arc::new(Recorder::default());
        let (session, _items) = Session::new(start(extra), recorder.clone());
        (session, recorder)
    }

    #[test]
    fn counts_both_ways_and_keeps_the_latest_frames() {
        let (s, r) = session("");
        assert!(s.resumable() && s.received() == 0 && s.sent() == 0);
        s.read(&decode(
            r#"{"type":"user.said","session_id":"vs_1","text":"hi"}"#,
        ));
        s.read(&decode(
            r#"{"type":"session.resumed","session_id":"vs_1","received":0}"#,
        ));
        assert_eq!(s.received(), 1);
        for i in 0..RETAINED_FRAMES + 5 {
            s.say(&format!("line {i}")).unwrap();
        }
        assert_eq!(s.sent() as usize, RETAINED_FRAMES + 5);
        assert_eq!(r.frames.lock().unwrap().len(), RETAINED_FRAMES + 5);
        let state = s.state();
        assert!(state.kept.len() == RETAINED_FRAMES && state.before == 5);
    }

    #[test]
    fn a_resume_sends_again_what_sent_did_not_read() {
        let (s, r) = session("");
        for text in ["one", "two", "three"] {
            s.say(text).unwrap();
        }
        let resume = s.resume_frame().unwrap();
        assert!(resume.token.starts_with("vsr_") && resume.received == 0);
        s.state().resuming = true;
        assert_eq!(s.resumed(&resumed(1, 'b')), 2);
        assert_eq!(r.texts(), ["one", "two", "three", "two", "three"]);
        assert!(s.token().unwrap().starts_with("vsr_b"));
        assert_eq!(s.sent(), 3);
    }

    #[test]
    fn a_resume_past_the_retained_frames_hangs_up() {
        let (s, r) = session("");
        for i in 0..RETAINED_FRAMES + 3 {
            s.say(&i.to_string()).unwrap();
        }
        s.state().resuming = true;
        assert_eq!(s.resumed(&resumed(1, 'c')), 1);
        assert_eq!(r.texts().last().unwrap(), "hangup");
    }

    #[test]
    fn a_resume_past_what_was_sent_sends_nothing_again() {
        let (s, _r) = session("");
        s.say("one").unwrap();
        s.state().resuming = true;
        assert_eq!(s.resumed(&resumed(5, 'e')), 0);
        assert_eq!(s.sent(), 5);
    }

    #[tokio::test]
    async fn tool_calls_are_answered_by_their_result() {
        let (s, r) = session("");
        let waiting = {
            let s = s.clone();
            tokio::spawn(async move { s.transfer_number("+14155550199").await })
        };
        tokio::task::yield_now().await;
        let call = loop {
            if let Some(Frame::ToolCall(c)) = r.frames.lock().unwrap().last().cloned() {
                break c;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(call.name, "transfer");
        assert_eq!(call.arguments.unwrap()["to"]["value"], "+14155550199");
        let result = decode(&format!(
            r#"{{"type":"tool.result","session_id":"vs_1","call_ref":"{}","ok":true}}"#,
            call.call_ref
        ));
        if let Frame::ToolResult(result) = result {
            s.answer_tool(&result);
        }
        assert!(waiting.await.unwrap().unwrap().ok);
    }

    #[tokio::test]
    async fn a_session_that_ended_refuses_more() {
        let (s, _r) = session("");
        let waiting = {
            let s = s.clone();
            tokio::spawn(async move { s.hangup().await })
        };
        while s.state().pending.is_empty() {
            tokio::task::yield_now().await;
        }
        if let Frame::SessionEnd(end) = decode(
            r#"{"type":"session.end","session_id":"vs_1","reason":"caller_hung_up","duration_ms":10}"#,
        ) {
            s.finish(end);
        }
        assert_eq!(waiting.await.unwrap(), Err(Error::SessionEnded));
        assert_eq!(s.say("too late"), Err(Error::SessionEnded));
    }

    #[test]
    fn audio_goes_out_on_the_session_stream() {
        let (s, r) = session(r#","session_type":"audio","audio":{"stream":7}"#);
        s.send_audio(&[1u8; 640]).unwrap();
        let audio = r.audio.lock().unwrap();
        assert_eq!(&audio[0][..2], &[0, 7]);
        assert_eq!(audio[0].len(), 642);
    }

    #[test]
    fn a_frame_that_breaks_the_protocol_is_refused_before_it_counts() {
        let (s, r) = session("");
        assert!(matches!(s.say(""), Err(Error::Frame(_))));
        assert_eq!(s.sent(), 0);
        assert!(r.frames.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn frames_sent_during_a_resume_wait_for_the_replay() {
        let (s, r) = session("");
        s.say("before").unwrap();
        s.state().resuming = true;
        s.say("during").unwrap();
        let waiting = {
            let s = s.clone();
            tokio::spawn(async move { s.hangup().await })
        };
        while s.sent() < 3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(r.texts(), ["before"]);
        assert_eq!(s.resumed(&resumed(1, 'd')), 2);
        assert_eq!(r.texts(), ["before", "during", "hangup"]);
        let call_ref = match r.frames.lock().unwrap().last().cloned() {
            Some(Frame::ToolCall(c)) => c.call_ref,
            other => panic!("{other:?}"),
        };
        if let Frame::ToolResult(result) = decode(&format!(
            r#"{{"type":"tool.result","session_id":"vs_1","call_ref":"{call_ref}","ok":true}}"#
        )) {
            s.answer_tool(&result);
        }
        assert!(waiting.await.unwrap().unwrap().ok);
    }

    #[test]
    fn audio_during_a_resume_is_dropped_and_after_the_end_refused() {
        let (s, r) = session(r#","session_type":"audio","audio":{"stream":7}"#);
        s.state().resuming = true;
        s.send_audio(&[0u8; 640]).unwrap();
        assert!(r.audio.lock().unwrap().is_empty());
        s.state().resuming = false;
        if let Frame::SessionEnd(end) = decode(
            r#"{"type":"session.end","session_id":"vs_1","reason":"caller_hung_up","duration_ms":10}"#,
        ) {
            s.finish(end);
        }
        assert_eq!(s.send_audio(&[0u8; 640]), Err(Error::SessionEnded));
    }

    #[tokio::test]
    async fn a_lost_session_fails_its_tool_calls_and_refuses_more() {
        let (s, _r) = session("");
        let waiting = {
            let s = s.clone();
            tokio::spawn(async move { s.hangup().await })
        };
        while s.state().pending.is_empty() {
            tokio::task::yield_now().await;
        }
        s.abandon();
        assert_eq!(waiting.await.unwrap(), Err(Error::SessionEnded));
        assert!(s.ended() && s.lost());
        assert_eq!(s.say("too late"), Err(Error::SessionEnded));
    }

    #[test]
    fn unknown_session_frames_count_and_errors_do_not() {
        let (s, _r) = session("");
        s.read(&decode(
            r#"{"type":"session.note","session_id":"vs_1","x":1}"#,
        ));
        s.read(&decode(
            r#"{"type":"error","session_id":"vs_1","error":{"code":"frame_not_allowed","message":"no"}}"#,
        ));
        assert_eq!(s.received(), 1);
    }
}
