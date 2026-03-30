use cove_common::id::UserId;
use sqlx::PgPool;
use std::collections::HashSet;

pub const SEARCH_MENTION_PREFIX: &str = "__mention__:";
const MIN_USERNAME_LENGTH: usize = 3;
const MAX_USERNAME_LENGTH: usize = 32;

pub struct SearchScope {
    pub is_mention_query: bool,
    pub query: String,
}

pub fn parse_search_scope(raw_query: &str) -> SearchScope {
    let trimmed = raw_query.trim();
    if let Some(query) = trimmed.strip_prefix(SEARCH_MENTION_PREFIX) {
        SearchScope {
            is_mention_query: true,
            query: query.trim().to_string(),
        }
    } else {
        SearchScope {
            is_mention_query: false,
            query: trimmed.to_string(),
        }
    }
}

pub fn extract_mentioned_usernames(text: &str) -> Vec<String> {
    let mut usernames = Vec::new();
    let mut seen = HashSet::new();

    for (idx, ch) in text.char_indices() {
        if ch != '@' {
            continue;
        }

        if idx > 0 {
            if let Some(previous) = text[..idx].chars().next_back() {
                if is_username_char(previous) {
                    continue;
                }
            }
        }

        let mut cursor = idx + 1;
        let mut length = 0usize;

        while cursor < text.len() {
            let Some(next_char) = text[cursor..].chars().next() else {
                break;
            };

            if !is_username_char(next_char) {
                break;
            }

            length += 1;
            if length > MAX_USERNAME_LENGTH {
                break;
            }

            cursor += next_char.len_utf8();
        }

        if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&length) {
            continue;
        }

        let username = text[(idx + 1)..cursor].to_ascii_lowercase();
        if seen.insert(username.clone()) {
            usernames.push(username);
        }
    }

    usernames
}

pub async fn resolve_mentionable_user_ids(
    pool: &PgPool,
    viewer_id: UserId,
    usernames: &[String],
) -> Result<Vec<uuid::Uuid>, sqlx::Error> {
    if usernames.is_empty() {
        return Ok(vec![]);
    }

    let mut normalized = usernames
        .iter()
        .map(|username| username.trim().to_ascii_lowercase())
        .filter(|username| {
            let len = username.chars().count();
            (MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&len)
        })
        .collect::<Vec<_>>();

    normalized.sort_unstable();
    normalized.dedup();

    if normalized.is_empty() {
        return Ok(vec![]);
    }

    sqlx::query_scalar::<_, uuid::Uuid>(
        r#"
        SELECT u.id
        FROM users u
        LEFT JOIN profiles p ON p.user_id = u.id
        LEFT JOIN follows f
          ON f.follower_id = $1
         AND f.followee_id = u.id
         AND f.state = 'accepted'
        WHERE u.account_state != 'suspended'
          AND u.id <> $1
          AND lower(u.username) = ANY($2)
          AND (f.followee_id IS NOT NULL OR COALESCE(p.is_private, TRUE) = FALSE)
        ORDER BY array_position($2::text[], lower(u.username))
        "#,
    )
    .bind(viewer_id.as_uuid())
    .bind(&normalized)
    .fetch_all(pool)
    .await
}

fn is_username_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')
}
