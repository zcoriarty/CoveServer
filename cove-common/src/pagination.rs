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
        let bytes = base64_decode(cursor.trim())?;
        let json = String::from_utf8(bytes).ok()?;
        serde_json::from_str(&json).ok()
    }

    pub fn encode_cursor(value: &CursorValue) -> String {
        let json = serde_json::to_string(value).unwrap_or_default();
        base64_encode(json.as_bytes())
    }
}

const B64_CHARS: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = Vec::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };

        out.push(B64_CHARS[(b0 >> 2) as usize]);
        out.push(B64_CHARS[((b0 & 0x03) << 4 | b1 >> 4) as usize]);

        if chunk.len() > 1 {
            out.push(B64_CHARS[((b1 & 0x0f) << 2 | b2 >> 6) as usize]);
        } else {
            out.push(b'=');
        }

        if chunk.len() > 2 {
            out.push(B64_CHARS[(b2 & 0x3f) as usize]);
        } else {
            out.push(b'=');
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    let input = input.trim_end_matches('=');
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut accum: u32 = 0;
    let mut bits: u32 = 0;

    for ch in input.bytes() {
        let val: u32 = match ch {
            b'A'..=b'Z' => (ch - b'A') as u32,
            b'a'..=b'z' => (ch - b'a' + 26) as u32,
            b'0'..=b'9' => (ch - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        accum = (accum << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accum >> bits) as u8);
            accum &= (1 << bits) - 1;
        }
    }
    Some(out)
}
