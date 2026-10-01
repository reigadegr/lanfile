//! Zero-copy file responses for a Salvo server.
//!
//! Hyper owns the connection socket and writes every response byte itself, so a
//! handler cannot call `sendfile(2)` on its own. This crate bridges that gap:
//!
//! 1. [`SendfileStream`] wraps an accepted connection's transport.
//! 2. [`upgrade_response`] replaces the body of a file response with a
//!    [`SendfileBody`], which reports the file's exact length but yields
//!    placeholder bytes instead of content.
//! 3. The stream recognises those placeholders and issues `sendfile(2)` for the
//!    same length, so the file never enters userspace.
//!
//! The caller owns the service wrapper, so it hands the [`SendfileSlot`] straight
//! to the handler: [`SendfileStream::new`] plus [`upgrade_response`] is what
//! this project's fast path does.
//!
//! Framing is untouched: the placeholder byte count equals the `Content-Length`
//! Hyper was given, so keep-alive, range responses and Hyper's own accounting
//! behave exactly as they do for an ordinary body.

use std::fs::File;
use std::sync::Arc;

use salvo::{
    http::{
        StatusCode,
        body::ResBody,
        header::{CONTENT_LENGTH, CONTENT_RANGE},
    },
    prelude::*,
};

mod body;
mod stream;

pub use body::SendfileSlot;
pub use stream::SendfileStream;

/// Replaces a file response body with a zero-copy `sendfile(2)` body.
///
/// Call this after the response headers and body have been produced, passing the
/// file that was opened for the same response. The response is left untouched
/// unless every condition holds:
///
/// - the status is `200 OK` or `206 Partial Content`;
/// - `Content-Length` is present and non-zero;
/// - the platform has `sendfile(2)`.
///
/// There is deliberately no size threshold: even for a few kilobytes `sendfile`
/// removes the read into userspace that an ordinary body needs, and the caller
/// is expected to have disabled `NamedFile`'s small-file preload so that read is
/// not paid before this is reached.
///
pub fn upgrade_response(slot: &SendfileSlot, res: &mut Response, file: Arc<File>) {
    let status = res.status_code;
    if status != Some(StatusCode::OK) && status != Some(StatusCode::PARTIAL_CONTENT) {
        return;
    }
    let Some(len) = header_u64(res, CONTENT_LENGTH) else {
        return;
    };
    let offset = header_str(res, CONTENT_RANGE)
        .and_then(range_start)
        .unwrap_or(0);
    let Some(body) = slot.arm(file, offset, len) else {
        return;
    };
    res.replace_body(ResBody::Boxed(Box::pin(body)));
}

/// 借出响应头里的字符串，不复制：调用方要么立刻解析成数字，要么马上解析成偏移量。
fn header_str(res: &Response, name: salvo::http::header::HeaderName) -> Option<&str> {
    res.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

fn header_u64(res: &Response, name: salvo::http::header::HeaderName) -> Option<u64> {
    header_str(res, name)?.parse().ok()
}

/// Start offset of a `Content-Range: bytes <start>-<end>/<total>` header.
fn range_start(value: &str) -> Option<u64> {
    let rest = value.strip_prefix("bytes ")?;
    let (start, _) = rest.split_once('-')?;
    start.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::range_start;

    #[test]
    fn range_start_parses_a_partial_content_header() {
        assert_eq!(
            range_start("bytes 1048576-2097151/8388608"),
            Some(1_048_576)
        );
        assert_eq!(range_start("bytes 0-99/100"), Some(0));
    }

    #[test]
    fn range_start_rejects_other_forms() {
        assert_eq!(range_start("items 0-99/100"), None);
        assert_eq!(range_start("bytes */100"), None);
    }
}
