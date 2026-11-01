//! The agent: one process that serves calls on the Sent Voice Agents protocol.
//!
//! Give [`Agent::new`] a [`Handler`] and call [`Agent::run`] or [`Agent::serve`]: the agent connects to Sent with
//! the account API key and serves sessions until stopped. The runtime answers heartbeats, opens a new connection
//! when one drains or is lost, and resumes the sessions that can move, so the handlers only see calls. One task
//! runs each session's handlers in the order Sent's frames arrived; different sessions run at the same time.

use crate::audio::{samples_of, stream_of, HEADER_BYTES};
use crate::frames::{
    self, ConnectionReady, ErrorFrame, Frame, Pong, SessionEnd, SessionResumed, SessionStart, ToolResult, UnknownFrame,
    UserSaid, CONNECT_PATH, HOST, SUBPROTOCOL,
};
use crate::session::{Item, Sender, Session};
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{error, info, warn};

/// Where the agent connects when [`Options::url`] is empty.
pub fn default_url() -> String {
    format!("wss://{HOST}{CONNECT_PATH}")
}

/// The longest wait before another attempt to connect.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How long a handshake may take before the attempt counts as failed, and how long an open socket may wait
/// for connection.ready.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How many heartbeat intervals without a frame make a connection dead, as Heartbeats in docs/protocol.md says.
const SILENT_INTERVALS: u32 = 3;

/// Why Sent refuses a handshake with each status, from Connecting in docs/protocol.md. Each needs a person.
fn refusal(status: u16) -> Option<&'static str> {
    match status {
        400 => Some("agent_id or the subprotocol is missing"),
        401 => Some("the API key is not valid"),
        403 => Some("voice is not enabled for the account, or the key does not own the agent"),
        404 => Some("the agent does not exist"),
        _ => None,
    }
}

/// Sent refused the connection for a reason that needs a person: the status and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub status: u16,
    pub reason: String,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.reason)
    }
}

impl std::error::Error for Refused {}

/// What an agent does on its calls. Every method does nothing by default; implement the ones you need with
/// `async fn`.
pub trait Handler: Send + Sync + 'static {
    /// A connection to Sent is ready for sessions.
    fn on_connected(&self, ready: ConnectionReady) -> impl Future<Output = ()> + Send {
        let _ = ready;
        async {}
    }

    /// A call reached the agent. In a text session, `say` to speak; an audio session waits for audio.
    fn on_session_start(&self, session: Arc<Session>) -> impl Future<Output = ()> + Send {
        let _ = session;
        async {}
    }

    /// Text sessions: the caller said something. With `correction` true it replaces the previous one.
    fn on_user_said(&self, session: Arc<Session>, said: UserSaid) -> impl Future<Output = ()> + Send {
        let _ = (session, said);
        async {}
    }

    /// Audio sessions: the caller's audio, 16-bit little-endian mono PCM at `session.sample_rate`.
    fn on_audio(&self, session: Arc<Session>, samples: Vec<u8>) -> impl Future<Output = ()> + Send {
        let _ = (session, samples);
        async {}
    }

    /// Sent answered a tool call. The call's waiting `tool` gets the same result.
    fn on_tool_result(&self, session: Arc<Session>, result: ToolResult) -> impl Future<Output = ()> + Send {
        let _ = (session, result);
        async {}
    }

    /// A session frame this SDK version does not know. It was counted; nothing else is needed.
    fn on_frame(&self, session: Arc<Session>, frame: UnknownFrame) -> impl Future<Output = ()> + Send {
        let _ = (session, frame);
        async {}
    }

    /// The call left the agent. `end.reason` says why.
    fn on_session_end(&self, session: Arc<Session>, end: SessionEnd) -> impl Future<Output = ()> + Send {
        let _ = (session, end);
        async {}
    }

    /// The session's connection was lost and it could not be resumed: Sent ends the call with
    /// agent_unreachable. No session.end follows.
    fn on_session_lost(&self, session: Arc<Session>) -> impl Future<Output = ()> + Send {
        let _ = session;
        async {}
    }

    /// Sent refused a frame; the connection stays open.
    fn on_error(&self, error: ErrorFrame) -> impl Future<Output = ()> + Send {
        let _ = error;
        async {}
    }
}

/// What an agent needs.
#[derive(Debug, Clone)]
pub struct Options {
    /// The account API key. Keep it on the server the agent runs on.
    pub api_key: String,
    /// The agent, as POST /v3/voice/agents created it.
    pub agent_id: String,
    /// Where to connect. Empty means Sent.
    pub url: String,
}

impl Options {
    /// Options for an agent on Sent.
    pub fn new(api_key: impl Into<String>, agent_id: impl Into<String>) -> Options {
        Options {
            api_key: api_key.into(),
            agent_id: agent_id.into(),
            url: String::new(),
        }
    }

    /// The same options, connecting elsewhere.
    pub fn with_url(mut self, url: impl Into<String>) -> Options {
        self.url = url.into();
        self
    }
}

struct ConnState {
    ready: bool,
    draining: bool,
    sessions: HashMap<String, Arc<Session>>,
    pending: Vec<Arc<Session>>,
    heartbeat: Duration,
    open: bool,
}

/// One WebSocket to Sent, and the sessions it reads and writes.
struct Connection {
    state: Mutex<ConnState>,
    outbox: mpsc::UnboundedSender<Message>,
    closer: Notify,
}

impl Connection {
    fn state(&self) -> MutexGuard<'_, ConnState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Sender for Connection {
    fn send_text(&self, kind: &str, data: String) {
        if !self.state().open || self.outbox.send(Message::Text(data.into())).is_err() {
            // A session frame is still counted as sent. Sent's received on the next resume says to send it again.
            info!(r#type = kind, "not sent: the connection is not open");
        }
    }

    fn send_bytes(&self, frame: Vec<u8>) {
        if self.state().open {
            let _ = self.outbox.send(Message::Binary(frame.into()));
        }
    }
}

struct State {
    connections: Vec<Arc<Connection>>,
    live: HashMap<String, Arc<Session>>,
    attempt: u32,
    stopped: bool,
    error: Option<Refused>,
}

struct Inner<H> {
    options: Options,
    handler: H,
    state: Mutex<State>,
    done: Notify,
}

/// Serves calls. Its [`Handler`]'s methods run on the session's own task, in order.
pub struct Agent<H: Handler> {
    inner: Arc<Inner<H>>,
}

impl<H: Handler> Clone for Agent<H> {
    fn clone(&self) -> Self {
        Agent {
            inner: self.inner.clone(),
        }
    }
}

impl<H: Handler> Agent<H> {
    /// An agent for the options and the handler. It connects when `run` or `serve` is called.
    pub fn new(mut options: Options, handler: H) -> Agent<H> {
        if options.url.is_empty() {
            options.url = default_url();
        }
        Agent {
            inner: Arc::new(Inner {
                options,
                handler,
                state: Mutex::new(State {
                    connections: Vec::new(),
                    live: HashMap::new(),
                    attempt: 0,
                    stopped: false,
                    error: None,
                }),
                done: Notify::new(),
            }),
        }
    }

    /// Serves calls until [`Agent::stop`] is called. Fails with [`Refused`] when Sent refuses the connection for
    /// a reason that needs a person.
    pub async fn run(&self) -> Result<(), Refused> {
        Inner::open(&self.inner, Vec::new(), Duration::ZERO);
        self.inner.done.notified().await;
        let error = self.inner.state().error.clone();
        match error {
            Some(refused) => Err(refused),
            None => Ok(()),
        }
    }

    /// Runs the agent until SIGINT or SIGTERM. For a program whose main is the agent.
    pub async fn serve(&self) -> Result<(), Refused> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let interrupt = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(signal) => signal,
                    Err(_) => {
                        let _ = interrupt.await;
                        Inner::stop(&inner);
                        return;
                    }
                };
                tokio::select! {
                    _ = interrupt => {}
                    _ = terminate.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = interrupt.await;
            }
            Inner::stop(&inner);
        });
        self.run().await
    }

    /// Stops serving: every connection closes with 1000, so the calls on them follow the agent's fallback at
    /// once.
    pub fn stop(&self) {
        Inner::stop(&self.inner);
    }

    /// Every live session.
    pub fn sessions(&self) -> Vec<Arc<Session>> {
        self.inner.state().live.values().cloned().collect()
    }
}

impl<H: Handler> Inner<H> {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn stop(inner: &Arc<Inner<H>>) {
        let connections = {
            let mut state = inner.state();
            if state.stopped {
                return;
            }
            state.stopped = true;
            state.connections.clone()
        };
        for conn in connections {
            conn.closer.notify_one();
        }
        inner.done.notify_one();
    }

    fn finish(inner: &Arc<Inner<H>>, error: Refused) {
        {
            let mut state = inner.state();
            if state.stopped {
                return;
            }
            state.error = Some(error);
        }
        Inner::stop(inner);
    }

    // ----- connections -----

    fn open(inner: &Arc<Inner<H>>, pending: Vec<Arc<Session>>, delay: Duration) {
        let (outbox, outgoing) = mpsc::unbounded_channel();
        let conn = Arc::new(Connection {
            state: Mutex::new(ConnState {
                ready: false,
                draining: false,
                sessions: HashMap::new(),
                pending,
                heartbeat: Duration::ZERO,
                open: false,
            }),
            outbox,
            closer: Notify::new(),
        });
        inner.state().connections.push(conn.clone());
        let inner = inner.clone();
        tokio::spawn(async move { Inner::serve_connection(inner, conn, outgoing, delay).await });
    }

    async fn serve_connection(
        inner: Arc<Inner<H>>,
        conn: Arc<Connection>,
        mut outgoing: mpsc::UnboundedReceiver<Message>,
        delay: Duration,
    ) {
        if delay > Duration::ZERO {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = conn.closer.notified() => {
                    Inner::closed(&inner, &conn);
                    return;
                }
            }
        }
        let separator = if inner.options.url.contains('?') { '&' } else { '?' };
        let url = format!("{}{separator}agent_id={}", inner.options.url, inner.options.agent_id);
        let mut request = match url.as_str().into_client_request() {
            Ok(request) => request,
            Err(e) => {
                error!(error = %e, "the URL does not parse");
                Inner::finish(
                    &inner,
                    Refused {
                        status: 0,
                        reason: format!("the URL does not parse: {e}"),
                    },
                );
                Inner::closed(&inner, &conn);
                return;
            }
        };
        let authorization = match HeaderValue::from_str(&format!("Bearer {}", inner.options.api_key)) {
            Ok(value) => value,
            Err(_) => {
                Inner::finish(
                    &inner,
                    Refused {
                        status: 0,
                        reason: "the API key is not a header value".into(),
                    },
                );
                Inner::closed(&inner, &conn);
                return;
            }
        };
        request.headers_mut().insert("Authorization", authorization);
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static(SUBPROTOCOL));
        let host = request.uri().host().unwrap_or("").to_string();
        let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, tokio_tungstenite::connect_async(request)).await {
            Err(_) => {
                warn!("the handshake timed out");
                Inner::closed(&inner, &conn);
                return;
            }
            Ok(Err(tungstenite::Error::Http(response))) => {
                let status = response.status().as_u16();
                match refusal(status) {
                    Some(reason) => {
                        error!(status, reason, "refused");
                        Inner::finish(
                            &inner,
                            Refused {
                                status,
                                reason: reason.to_string(),
                            },
                        );
                    }
                    None => warn!(status, "refused, trying again"),
                }
                Inner::closed(&inner, &conn);
                return;
            }
            Ok(Err(e)) => {
                warn!(error = %e, "connect failed");
                Inner::closed(&inner, &conn);
                return;
            }
            Ok(Ok((stream, _))) => stream,
        };
        let (mut sink, mut source) = stream.split();
        conn.state().open = true;
        info!(host, "connected");
        let mut last_heard = Instant::now();
        loop {
            // Until connection.ready arrives the socket has the handshake's deadline; after it, the heartbeat's.
            let (silence, expected) = {
                let state = conn.state();
                if state.ready {
                    (state.heartbeat * SILENT_INTERVALS, "frame")
                } else {
                    (HANDSHAKE_TIMEOUT, "connection.ready")
                }
            };
            let remaining = (last_heard + silence).saturating_duration_since(Instant::now());
            tokio::select! {
                biased;
                _ = conn.closer.notified() => {
                    let close = CloseFrame { code: CloseCode::Normal, reason: "".into() };
                    let _ = sink.send(Message::Close(Some(close))).await;
                    info!("closed by the agent");
                    break;
                }
                message = outgoing.recv() => {
                    match message {
                        Some(message) => {
                            if sink.send(message).await.is_err() {
                                info!("lost while sending");
                                break;
                            }
                        }
                        None => break,
                    }
                }
                next = tokio::time::timeout(remaining, source.next()) => {
                    match next {
                        Err(_) => {
                            warn!(expected, after = ?silence, "nothing arrived, closing");
                            break;
                        }
                        Ok(None) => {
                            info!("lost");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            info!(error = %e, "lost");
                            break;
                        }
                        Ok(Some(Ok(message))) => {
                            last_heard = Instant::now();
                            match message {
                                Message::Text(text) => match frames::decode(&text) {
                                    Ok(frame) => Inner::handle(&inner, &conn, frame),
                                    Err(e) => warn!(error = %e, "ignored a frame"),
                                },
                                Message::Binary(data) => Inner::audio(&inner, &conn, &data),
                                Message::Close(frame) => {
                                    let code = frame.as_ref().map_or(1005, |f| u16::from(f.code));
                                    info!(code, "closed");
                                    break;
                                }
                                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                            }
                        }
                    }
                }
            }
        }
        conn.state().open = false;
        let _ = sink.close().await;
        Inner::closed(&inner, &conn);
    }

    fn handle(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: Frame) {
        match frame {
            Frame::ConnectionReady(ready) => Inner::ready(inner, conn, ready),
            Frame::Ping(ping) => {
                if let Ok(data) = frames::encode_to_string(&Frame::Pong(Pong { ts: ping.ts })) {
                    conn.send_text("pong", data);
                }
                // A connection that lived through a heartbeat interval is a good one: the next loss starts the
                // backoff over. A connection that dies before its first ping keeps it growing.
                if conn.state().ready {
                    inner.state().attempt = 0;
                }
            }
            Frame::ConnectionDraining(_) => Inner::drain(inner, conn),
            Frame::SessionStart(start) => Inner::start(inner, conn, start),
            Frame::SessionResumed(resumed) => Inner::resumed(inner, conn, &resumed),
            Frame::ErrorFrame(error) => {
                Inner::failed(inner, conn, &error);
                let inner = inner.clone();
                tokio::spawn(async move { safely(inner.handler.on_error(error)).await });
            }
            frame => match frame.session_id() {
                Some(_) => Inner::session_frame(inner, conn, frame),
                None => info!(r#type = frame.frame_type(), "ignored a frame"),
            },
        }
    }

    fn ready(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: ConnectionReady) {
        let pending: Vec<Arc<Session>> = {
            let mut state = conn.state();
            state.ready = true;
            state.heartbeat = Duration::from_millis(frame.heartbeat_interval_ms.max(0) as u64);
            let live = inner.state();
            let pending = state
                .pending
                .drain(..)
                .filter(|session| live.live.get(&session.id).is_some_and(|s| Arc::ptr_eq(s, session)))
                .collect();
            pending
        };
        info!(
            protocol = frame.protocol_version,
            max_sessions = frame.max_sessions,
            resuming = pending.len(),
            "ready"
        );
        for session in pending {
            Inner::resume(inner, conn, &session);
        }
        let inner = inner.clone();
        tokio::spawn(async move { safely(inner.handler.on_connected(frame)).await });
    }

    fn resume(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, session: &Arc<Session>) {
        {
            let state = inner.state();
            for other in &state.connections {
                other.state().sessions.remove(&session.id);
            }
        }
        conn.state().sessions.insert(session.id.clone(), session.clone());
        session.move_to(conn.clone());
        if let Some(resume) = session.resume_frame() {
            if let Ok(data) = frames::encode_to_string(&Frame::SessionResume(resume)) {
                conn.send_text("session.resume", data);
            }
        }
    }

    fn drain(inner: &Arc<Inner<H>>, conn: &Arc<Connection>) {
        let moving: Vec<Arc<Session>> = {
            let mut state = conn.state();
            state.draining = true;
            state
                .sessions
                .values()
                .filter(|s| s.resumable() && !s.resuming())
                .cloned()
                .collect()
        };
        info!(moving = moving.len(), "draining");
        Inner::open(inner, moving, Duration::ZERO);
    }

    fn start(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: SessionStart) {
        let (session, items) = Session::new(frame, conn.clone());
        inner.state().live.insert(session.id.clone(), session.clone());
        conn.state().sessions.insert(session.id.clone(), session.clone());
        info!(
            session = session.id,
            session_type = session.session_type,
            direction = session.direction,
            "session started"
        );
        let inner = inner.clone();
        tokio::spawn(async move { Inner::run_session(inner, session, items).await });
    }

    fn session_frame(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: Frame) {
        let session = conn.state().sessions.get(frame.session_id().unwrap_or("")).cloned();
        // A session that moved to another connection is read there: Sent replays what this one did not deliver.
        let Some(session) = session else {
            return;
        };
        if session.resuming() {
            return;
        }
        session.read(&frame);
        match &frame {
            Frame::ToolResult(result) => session.answer_tool(result),
            Frame::SessionEnd(end) => {
                session.finish(end.clone());
                Inner::forget(inner, &session);
            }
            _ => {}
        }
        let _ = session.inbox.send(Item::Frame(Box::new(frame)));
    }

    fn audio(_inner: &Arc<Inner<H>>, conn: &Arc<Connection>, message: &[u8]) {
        if message.len() < HEADER_BYTES {
            return;
        }
        let stream = stream_of(message);
        let state = conn.state();
        for session in state.sessions.values() {
            if session.stream() == Some(stream) && !session.resuming() {
                let _ = session.inbox.send(Item::Audio(message.to_vec()));
                return;
            }
        }
    }

    fn resumed(_inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: &SessionResumed) {
        let session = conn.state().sessions.get(&frame.session_id).cloned();
        let Some(session) = session else {
            return;
        };
        if !session.resuming() {
            return;
        }
        let again = session.resumed(frame);
        info!(session = session.id, sent_again = again, "session resumed");
    }

    /// An error is uncounted, as Counting frames in docs/protocol.md says. Only one that refuses a
    /// session.resume changes the session's fate; any other is the handler's to read.
    fn failed(inner: &Arc<Inner<H>>, conn: &Arc<Connection>, frame: &ErrorFrame) {
        warn!(code = frame.error.code, message = frame.error.message, "error");
        let about_resume = frame.frame_type.as_deref() == Some("session.resume")
            || matches!(frame.error.code.as_str(), "resume_failed" | "connection_full");
        let Some(session_id) = frame.session_id.as_ref().filter(|_| about_resume) else {
            return;
        };
        let session = {
            let mut state = conn.state();
            match state.sessions.get(session_id).cloned() {
                Some(session) if session.resuming() => {
                    session.resume_refused();
                    state.sessions.remove(session_id);
                    session
                }
                _ => return,
            }
        };
        if frame.error.code == "connection_full" {
            // This connection already carries max_sessions. The token is still good on another one.
            Inner::open(inner, vec![session], Duration::ZERO);
            return;
        }
        Inner::lost(inner, &session);
    }

    fn forget(inner: &Arc<Inner<H>>, session: &Arc<Session>) {
        let state = inner.state();
        let mut live = state;
        live.live.remove(&session.id);
        for conn in &live.connections {
            conn.state().sessions.remove(&session.id);
        }
    }

    fn lost(inner: &Arc<Inner<H>>, session: &Arc<Session>) {
        Inner::forget(inner, session);
        session.abandon();
        let _ = session.inbox.send(Item::Lost);
    }

    fn closed(inner: &Arc<Inner<H>>, conn: &Arc<Connection>) {
        let mut lost_sessions = Vec::new();
        let (pending, drained, delay) = {
            let mut state = inner.state();
            let Some(index) = state.connections.iter().position(|c| Arc::ptr_eq(c, conn)) else {
                return;
            };
            state.connections.remove(index);
            if state.stopped {
                return;
            }
            let queued = |session: &Arc<Session>| {
                state
                    .connections
                    .iter()
                    .any(|other| other.state().pending.iter().any(|s| Arc::ptr_eq(s, session)))
            };
            let mut movable = Vec::new();
            let conn_state = conn.state();
            for session in conn_state.sessions.values() {
                if queued(session) {
                    continue;
                }
                if session.resumable() && !session.resuming() {
                    movable.push(session.clone());
                    continue;
                }
                let reason = if session.resumable() {
                    "its resume was not answered"
                } else {
                    "it has no resume token"
                };
                warn!(session = session.id, reason, "session lost");
                lost_sessions.push(session.clone());
            }
            let mut pending: Vec<Arc<Session>> = conn_state
                .pending
                .iter()
                .filter(|session| state.live.get(&session.id).is_some_and(|s| Arc::ptr_eq(s, session)))
                .cloned()
                .collect();
            drop(conn_state);
            pending.extend(movable);
            let drained = conn.state().draining && pending.is_empty();
            let delay = if drained { Duration::ZERO } else { backoff(&mut state) };
            (pending, drained, delay)
        };
        // Reported whether or not a reconnect follows: a drained connection's leftovers are lost too.
        for session in &lost_sessions {
            Inner::lost(inner, session);
        }
        if drained {
            return;
        }
        info!(
            in_ms = delay.as_millis() as u64,
            resuming = pending.len(),
            "reconnecting"
        );
        Inner::open(inner, pending, delay);
    }

    // ----- the session's task -----

    async fn run_session(inner: Arc<Inner<H>>, session: Arc<Session>, mut items: mpsc::UnboundedReceiver<Item>) {
        safely(inner.handler.on_session_start(session.clone())).await;
        while let Some(item) = items.recv().await {
            match item {
                Item::Lost => {
                    safely(inner.handler.on_session_lost(session.clone())).await;
                    return;
                }
                Item::Audio(data) => safely(inner.handler.on_audio(session.clone(), samples_of(&data).to_vec())).await,
                Item::Frame(frame) => match *frame {
                    Frame::UserSaid(said) => safely(inner.handler.on_user_said(session.clone(), said)).await,
                    Frame::ToolResult(result) => safely(inner.handler.on_tool_result(session.clone(), result)).await,
                    Frame::SessionEnd(end) => {
                        safely(inner.handler.on_session_end(session.clone(), end)).await;
                        return;
                    }
                    Frame::Unknown(unknown) => safely(inner.handler.on_frame(session.clone(), unknown)).await,
                    _ => {}
                },
            }
        }
    }
}

/// Runs under the agent's lock.
fn backoff(state: &mut State) -> Duration {
    let delay = if state.attempt == 0 {
        Duration::ZERO
    } else {
        MAX_BACKOFF.min(Duration::from_secs(1u64 << (state.attempt - 1).min(20)))
    };
    state.attempt += 1;
    delay
}

/// Runs a handler; a panic in it is logged and does not take the session or the agent down.
async fn safely(handler: impl Future<Output = ()>) {
    if AssertUnwindSafe(handler).catch_unwind().await.is_err() {
        error!("a handler failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::tests::{decode, start};
    use tokio::sync::Mutex as AsyncMutex;

    fn error_frame(frame_type: Option<&str>, code: &str) -> ErrorFrame {
        let extra = frame_type
            .map(|t| format!(r#","frame_type":"{t}""#))
            .unwrap_or_default();
        match decode(&format!(
            r#"{{"type":"error","session_id":"vs_1","error":{{"code":"{code}","message":"no"}}{extra}}}"#
        )) {
            Frame::ErrorFrame(e) => e,
            other => panic!("{other:?}"),
        }
    }

    struct Quiet;
    impl Handler for Quiet {}

    fn agent_with<T: Handler>(handler: T) -> (Agent<T>, Arc<Connection>) {
        let agent = Agent::new(Options::new("k", "agent_1"), handler);
        let (outbox, _outgoing) = mpsc::unbounded_channel();
        let conn = Arc::new(Connection {
            state: Mutex::new(ConnState {
                ready: true,
                draining: false,
                sessions: HashMap::new(),
                pending: Vec::new(),
                heartbeat: Duration::from_secs(15),
                open: false,
            }),
            outbox,
            closer: Notify::new(),
        });
        agent.inner.state().connections.push(conn.clone());
        (agent, conn)
    }

    fn resuming_session<T: Handler>(agent: &Agent<T>, conn: &Arc<Connection>) -> Arc<Session> {
        let (session, items) = Session::new(start(""), conn.clone());
        std::mem::forget(items);
        session.move_to(conn.clone());
        agent.inner.state().live.insert(session.id.clone(), session.clone());
        conn.state().sessions.insert(session.id.clone(), session.clone());
        session
    }

    #[tokio::test]
    async fn only_an_error_about_the_resume_refuses_it() {
        let (agent, conn) = agent_with(Quiet);
        let session = resuming_session(&agent, &conn);
        Inner::failed(
            &agent.inner,
            &conn,
            &error_frame(Some("agent.say"), "frame_not_allowed"),
        );
        assert!(session.resuming() && conn.state().sessions.contains_key("vs_1") && !session.lost());
        Inner::failed(
            &agent.inner,
            &conn,
            &error_frame(Some("session.resume"), "resume_failed"),
        );
        assert!(session.lost() && !conn.state().sessions.contains_key("vs_1"));
        assert!(!agent.inner.state().live.contains_key("vs_1"));
        agent.stop();
    }

    #[tokio::test]
    async fn a_full_connection_moves_the_resume_elsewhere() {
        let (agent, conn) = agent_with(Quiet);
        let session = resuming_session(&agent, &conn);
        Inner::failed(&agent.inner, &conn, &error_frame(None, "connection_full"));
        assert!(!session.lost() && !conn.state().sessions.contains_key("vs_1"));
        let opened: Vec<Arc<Connection>> = agent
            .inner
            .state()
            .connections
            .iter()
            .filter(|c| !Arc::ptr_eq(c, &conn))
            .cloned()
            .collect();
        assert_eq!(opened.len(), 1);
        assert!(opened[0].state().pending.iter().any(|s| Arc::ptr_eq(s, &session)));
        agent.stop();
    }

    #[tokio::test]
    async fn the_backoff_starts_over_after_a_heartbeat_not_on_ready() {
        let (agent, conn) = agent_with(Quiet);
        conn.state().ready = false;
        agent.inner.state().attempt = 3;
        if let Frame::ConnectionReady(ready) = decode(
            r#"{"type":"connection.ready","protocol_version":"0.2.0","heartbeat_interval_ms":15000,"max_sessions":10}"#,
        ) {
            Inner::ready(&agent.inner, &conn, ready);
        }
        assert_eq!(agent.inner.state().attempt, 3);
        Inner::handle(&agent.inner, &conn, decode(r#"{"type":"ping","ts":1}"#));
        assert_eq!(agent.inner.state().attempt, 0);
        agent.stop();
    }

    struct Noticing {
        seen: AsyncMutex<Vec<&'static str>>,
        done: Notify,
    }

    impl Handler for Noticing {
        async fn on_session_start(&self, session: Arc<Session>) {
            if session.hangup().await == Err(crate::session::Error::SessionEnded) {
                self.seen.lock().await.push("tool failed");
            }
        }

        async fn on_session_lost(&self, _session: Arc<Session>) {
            self.seen.lock().await.push("lost");
            self.done.notify_one();
            panic!("thrown by the handler");
        }
    }

    #[tokio::test]
    async fn a_lost_session_reaches_its_handler_after_its_tool_call_fails() {
        let (agent, conn) = agent_with(Noticing {
            seen: AsyncMutex::new(Vec::new()),
            done: Notify::new(),
        });
        Inner::start(&agent.inner, &conn, start(""));
        let session = agent.sessions().pop().unwrap();
        while session.sent() < 1 {
            tokio::task::yield_now().await;
        }
        Inner::lost(&agent.inner, &session);
        agent.inner.handler.done.notified().await;
        tokio::task::yield_now().await;
        assert_eq!(*agent.inner.handler.seen.lock().await, ["tool failed", "lost"]);
        assert!(agent.sessions().is_empty());
        agent.stop();
    }

    #[tokio::test]
    async fn a_draining_connection_reports_its_leftover_sessions_lost_when_it_closes() {
        let (agent, conn) = agent_with(Noticing {
            seen: AsyncMutex::new(Vec::new()),
            done: Notify::new(),
        });
        conn.state().draining = true;
        let no_token = r#"{"type":"session.start","session_id":"vs_1","call_id":"call_1","agent_id":"agent_1","direction":"inbound","from":{"kind":"number","value":"+14155550100"},"to":{"kind":"agent","value":"agent_1"},"session_type":"text","started_at":"2026-10-31T00:00:00Z"}"#;
        if let Frame::SessionStart(s) = decode(no_token) {
            Inner::start(&agent.inner, &conn, s);
        }
        let session = agent.sessions().pop().unwrap();
        while session.sent() < 1 {
            tokio::task::yield_now().await;
        }
        Inner::closed(&agent.inner, &conn);
        agent.inner.handler.done.notified().await;
        tokio::task::yield_now().await;
        assert!(agent.inner.handler.seen.lock().await.contains(&"lost"));
        assert!(session.lost() && agent.sessions().is_empty());
        assert!(agent.inner.state().connections.is_empty());
        agent.stop();
    }
}
