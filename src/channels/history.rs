//! Persistent markdown channel history logging.
//!
//! This module records channel conversations to per-sender markdown files under:
//! `{workspace}/history/{channel}/{sender_id}.md`.

use anyhow::Context;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

const HISTORY_DIR: &str = "history";
const UNKNOWN_PATH_COMPONENT: &str = "unknown";
pub const CHANNEL_HISTORY_BOT_SENDER: &str = "zeroclaw_bot";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    Incoming,
    Outgoing,
}

impl MessageDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelHistoryMessage {
    pub channel: String,
    pub conversation_sender: String,
    pub message_sender: String,
    pub content: String,
    pub direction: MessageDirection,
    pub timestamp: DateTime<Utc>,
}

impl ChannelHistoryMessage {
    pub fn incoming(
        channel: String,
        conversation_sender: String,
        message_sender: String,
        content: String,
        timestamp_secs: u64,
    ) -> Self {
        Self {
            channel,
            conversation_sender,
            message_sender,
            content,
            direction: MessageDirection::Incoming,
            timestamp: unix_timestamp_to_utc(timestamp_secs),
        }
    }

    pub fn outgoing(
        channel: String,
        conversation_sender: String,
        message_sender: String,
        content: String,
    ) -> Self {
        Self {
            channel,
            conversation_sender,
            message_sender,
            content,
            direction: MessageDirection::Outgoing,
            timestamp: Utc::now(),
        }
    }
}

pub fn append_message_non_blocking(workspace_dir: Arc<PathBuf>, message: ChannelHistoryMessage) {
    drop(tokio::spawn(async move {
        if let Err(error) = append_message(workspace_dir.as_path(), &message).await {
            tracing::warn!(
                channel = %message.channel,
                sender = %message.conversation_sender,
                direction = %message.direction.as_str(),
                "Failed to persist channel history entry: {error}"
            );
        }
    }));
}

async fn append_message(
    workspace_dir: &Path,
    message: &ChannelHistoryMessage,
) -> anyhow::Result<()> {
    let history_path = history_file_path(
        workspace_dir,
        message.channel.as_str(),
        message.conversation_sender.as_str(),
    );

    if let Some(parent) = history_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let include_header = tokio::fs::metadata(&history_path).await.is_err();
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&history_path)
        .await
        .with_context(|| format!("failed to open {}", history_path.display()))?;

    if include_header {
        file.write_all(
            history_header(
                message.channel.as_str(),
                message.conversation_sender.as_str(),
            )
            .as_bytes(),
        )
        .await
        .with_context(|| format!("failed to write header to {}", history_path.display()))?;
    }

    file.write_all(history_entry(message).as_bytes())
        .await
        .with_context(|| format!("failed to append entry to {}", history_path.display()))?;
    Ok(())
}

fn history_header(channel: &str, sender_id: &str) -> String {
    format!("# Conversation with {channel}_{sender_id}\n\n")
}

fn history_entry(message: &ChannelHistoryMessage) -> String {
    let timestamp = message.timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);
    format!(
        "## {timestamp} ({direction})\n`channel={channel}` `sender={sender}`\n**{sender}**: {content}\n\n",
        direction = message.direction.as_str(),
        channel = message.channel.as_str(),
        sender = message.message_sender.as_str(),
        content = message.content.as_str()
    )
}

fn history_file_path(workspace_dir: &Path, channel: &str, sender_id: &str) -> PathBuf {
    workspace_dir
        .join(HISTORY_DIR)
        .join(sanitize_path_component(channel))
        .join(format!("{}.md", sanitize_path_component(sender_id)))
}

fn sanitize_path_component(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return UNKNOWN_PATH_COMPONENT.to_string();
    }

    let mut sanitized = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() {
        UNKNOWN_PATH_COMPONENT.to_string()
    } else {
        sanitized
    }
}

fn unix_timestamp_to_utc(timestamp_secs: u64) -> DateTime<Utc> {
    let seconds = i64::try_from(timestamp_secs).unwrap_or(i64::MAX);
    Utc.timestamp_opt(seconds, 0)
        .single()
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_path_component_replaces_unsafe_characters() {
        assert_eq!(sanitize_path_component("telegram"), "telegram");
        assert_eq!(sanitize_path_component("1234"), "1234");
        assert_eq!(
            sanitize_path_component("../../etc/passwd"),
            ".._.._etc_passwd"
        );
        assert_eq!(sanitize_path_component(""), "unknown");
        assert_eq!(sanitize_path_component("   "), "unknown");
    }

    #[tokio::test]
    async fn append_message_writes_header_once_and_appends_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let workspace = tmp.path().to_path_buf();

        let first = ChannelHistoryMessage::incoming(
            "telegram".to_string(),
            "8443845544".to_string(),
            "8443845544".to_string(),
            "hello".to_string(),
            1_706_382_600,
        );
        let second = ChannelHistoryMessage::outgoing(
            "telegram".to_string(),
            "8443845544".to_string(),
            CHANNEL_HISTORY_BOT_SENDER.to_string(),
            "hi".to_string(),
        );

        append_message(&workspace, &first)
            .await
            .expect("append first");
        append_message(&workspace, &second)
            .await
            .expect("append second");

        let history_path = workspace
            .join("history")
            .join("telegram")
            .join("8443845544.md");
        let content = tokio::fs::read_to_string(&history_path)
            .await
            .expect("read history file");

        assert_eq!(
            content
                .matches("# Conversation with telegram_8443845544")
                .count(),
            1
        );
        assert!(content.contains("(incoming)"));
        assert!(content.contains("(outgoing)"));
        assert!(content.contains("`channel=telegram`"));
        assert!(content.contains("**8443845544**: hello"));
        assert!(content.contains("**zeroclaw_bot**: hi"));
    }
}
