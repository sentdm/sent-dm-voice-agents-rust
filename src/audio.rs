//! The binary audio frame of an audio session: a 2-byte big-endian stream number, then 16-bit little-endian
//! mono PCM at the session's sample rate, a whole number of samples. The header keeps the samples aligned, so a
//! reader can view them as 16-bit integers without copying.

/// The size of the stream number before the samples.
pub const HEADER_BYTES: usize = 2;

/// The stream a frame belongs to: the session whose stream it is, on this connection.
pub fn stream_of(frame: &[u8]) -> u16 {
    u16::from_be_bytes([frame[0], frame[1]])
}

/// The samples of a frame, as 16-bit little-endian mono PCM.
pub fn samples_of(frame: &[u8]) -> &[u8] {
    &frame[HEADER_BYTES..]
}

/// A frame carrying `samples` for `stream`. Fails when the samples are not whole 16-bit samples.
pub fn audio_frame(stream: u16, samples: &[u8]) -> Result<Vec<u8>, &'static str> {
    if samples.len() % 2 != 0 {
        return Err("audio is a whole number of 16-bit samples");
    }
    let mut frame = Vec::with_capacity(HEADER_BYTES + samples.len());
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(samples);
    Ok(frame)
}

/// How many bytes of samples carry `milliseconds` of audio at `sample_rate`: 640 for 20 ms at 16000 Hz.
pub fn bytes_per_frame(sample_rate: usize, milliseconds: usize) -> usize {
    sample_rate * milliseconds / 1000 * 2
}
