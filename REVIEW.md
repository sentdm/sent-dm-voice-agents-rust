# Review guide

What a reviewer, human or agent, checks on every PR to this repository.

1. **Protocol first.** Does the change keep what `docs/protocol.md` in `sent-dm-voice-protocol` says, at the tag
   `ci.yml` pins? Counting of frames, the resume rules, heartbeats, audio framing, the order of frames.
2. **Generated code untouched.** `src/frames.rs` changes only through the lane in
   `sent-dm-sdk-pipeline`. A PR editing it by hand is wrong by construction.
3. **Conformance green.** The conformance job passed on the PR's head commit.
4. **Handlers never block the runtime.** A handler runs on the session's task; a slow handler delays that session
   only. Nothing in a handler path may block the connection reader.
5. **No secrets in logs.** Resume tokens and API keys never reach a log line.
6. **Compatibility.** Unknown frames, fields, party kinds, end reasons and error codes are handled as the
   protocol's Compatibility section asks: ignored, counted, or treated as the fallback.
7. **Changelog line.** A `feat:` or `fix:` subject a customer can read.
