use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PaginationParams {
    pub limit: i32,
    pub cursor: Option<CursorValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorValue {
    pub timestamp: DateTime<Utc>,
    pub id: Uuid,
}

impl PaginationParams {
    pub fn from_proto(page_size: i32, cursor: &str) -> Self {
        let limit = page_size.clamp(1, 50);
        let cursor = if cursor.is_empty() {
            None
        } else {
            Self::decode_cursor(cursor)
        };
        Self { limit, cursor }
    }

    fn decode_cursor(cursor: &str) -> Option<CursorValue> {
        let decoded = base64_decode(cursor)?;
        serde_json::from_str(&decoded).ok()
    }

    pub fn encode_cursor(value: &CursorValue) -> String {
        let json = serde_json::to_string(value).unwrap_or_default();
        base64_encode(&json)
    }
}

fn base64_encode(input: &str) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut encoder =
            base64_writer(&mut buf);
        encoder.write_all(input.as_bytes()).ok();
        encoder.finish().ok();
    }
    String::from_utf8(buf).unwrap_or_default()
}

fn base64_decode(input: &str) -> Option<String> {
    let bytes = base64_decode_bytes(input)?;
    String::from_utf8(bytes).ok()
}

fn base64_writer(writer: impl std::io::Write) -> impl std::io::Write {
    SimpleBase64Encoder { writer, buf: [0; 3], buf_len: 0 }
}

fn base64_decode_bytes(input: &str) -> Option<Vec<u8>> {
    let input = input.trim_end_matches('=');
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut accum: u32 = 0;
    let mut bits = 0;
    for ch in input.bytes() {
        let val = match ch {
            b'A'..=b'Z' => ch - b'A',
            b'a'..=b'z' => ch - b'a' + 26,
            b'0'..=b'9' => ch - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        accum = (accum << 6) | val as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accum >> bits) as u8);
            accum &= (1 << bits) - 1;
        }
    }
    Some(out)
}

struct SimpleBase64Encoder<W: std::io::Write> {
    writer: W,
    buf: [u8; 3],
    buf_len: usize,
}

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

impl<W: std::io::Write> SimpleBase64Encoder<W> {
    fn finish(mut self) -> std::io::Result<()> {
        if self.buf_len > 0 {
            let mut chunk = [0u8; 4];
            match self.buf_len {
                1 => {
                    chunk[0] = B64_CHARS[(self.buf[0] >> 2) as usize];
                    chunk[1] = B64_CHARS[((self.buf[0] & 0x03) << 4) as usize];
                    self.writer.write_all(&chunk[..2])?;
                    self.writer.write_all(b"==")?;
                }
                2 => {
                    chunk[0] = B64_CHARS[(self.buf[0] >> 2) as usize];
                    chunk[1] = B64_CHARS[((self.buf[0] & 0x03) << 4 | self.buf[1] >> 4) as usize];
                    chunk[2] = B64_CHARS[((self.buf[1] & 0x0f) << 2) as usize];
                    self.writer.write_all(&chunk[..3])?;
                    self.writer.write_all(b"=")?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl<W: std::io::Write> std::io::Write for SimpleBase64Encoder<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut pos = 0;
        while pos < data.len() {
            self.buf[self.buf_len] = data[pos];
            self.buf_len += 1;
            pos += 1;
            if self.buf_len == 3 {
                let mut out = [0u8; 4];
                out[0] = B64_CHARS[(self.buf[0] >> 2) as usize];
                out[1] = B64_CHARS[((self.buf[0] & 0x03) << 4 | self.buf[1] >> 4) as usize];
                out[2] = B64_CHARS[((self.buf[1] & 0x0f) << 2 | self.buf[2] >> 6) as usize];
                out[3] = B64_CHARS[(self.buf[2] & 0x3f) as usize];
                self.writer.write_all(&out)?;
                self.buf_len = 0;
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
