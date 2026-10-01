//! 面向本项目的静态文件响应类型。构建文件时不经过 blocking pool，正文由
//! [`NamedFile`] 或 sendfile 升级路径写出。

mod chunked_file;

use std::cmp;
use std::fs::{File, Metadata};
#[cfg(not(unix))]
use std::io::Read as StdRead;
#[cfg(not(unix))]
use std::io::{Seek as StdSeek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use enumflags2::{BitFlags, bitflags};
use mime::Mime;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use salvo::http::body::ResBody;
use salvo::http::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_TYPE, IF_NONE_MATCH, LAST_MODIFIED, RANGE,
    X_CONTENT_TYPE_OPTIONS,
};
use salvo::http::headers::*;
use salvo::http::mime::{detect_text_mime, fill_mime_charset_if_need, is_charset_required_mime};
use salvo::http::{HttpRange, Response, StatusCode};
use salvo::{Error, Result};

use crate::chunked_file::ChunkedFile;

const CHUNK_SIZE: u64 = 1024 * 1024;
const PRELOAD_THRESHOLD: u64 = 1024 * 1024;
const RFC5987_ATTR_CHAR_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'%')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'{')
    .add(b'}');

#[bitflags(default = Etag | LastModified | ContentDisposition | ContentTypeOptions)]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum Flag {
    Etag = 0b0001,
    LastModified = 0b0010,
    ContentDisposition = 0b0100,
    ContentTypeOptions = 0b1000,
}

/// 服务一个文件所需要的元数据：只有 inode、长度与修改时间这三项。
///
/// 比 [`std::fs::Metadata`]（本机 144 字节）小得多，而缓存里每条要存一份、命中时每请求还要
/// 克隆一份，这两处的开销都随之变小。它可以直接由 `fstat(2)` 填出来：`std` 的
/// `File::metadata()` 在本目标上发的是 `statx(fd, AT_EMPTY_PATH)`（实测 353 ns），
/// `fstat` 只要 285 ns，两者给出的字段值完全相同。
#[derive(Clone, Debug)]
pub struct FileMeta {
    len: u64,
    ino: u64,
    mtime: i64,
    mtime_nsec: i64,
}

impl FileMeta {
    /// 用 `fstat(2)` 的结果构造。
    #[must_use]
    pub const fn from_raw(len: u64, ino: u64, mtime: i64, mtime_nsec: i64) -> Self {
        Self {
            len,
            ino,
            mtime,
            mtime_nsec,
        }
    }

    /// 从 [`std::fs::Metadata`] 转换，供拿不到 fd 的场景使用。
    ///
    /// 非 unix 平台没有 `MetadataExt`：inode 与 [`NamedFile::etag`] 的非 unix 分支一样取 0，
    /// mtime 改用 [`Metadata::modified`] 换算，换算结果与 [`Self::modified`] 严格互逆。
    #[must_use]
    pub fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            Self::from_raw(
                metadata.len(),
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
            )
        }
        #[cfg(not(unix))]
        {
            // 以 i128 纳秒为单位取欧几里得商余：1970 年之前的文件也会落回 [0, 1e9) 的纳秒
            // 区间，负的 mtime 因此仍能被上层（跳过 Last-Modified 与 ETag）识别出来
            let offset = match metadata.modified() {
                Ok(modified) => match modified.duration_since(UNIX_EPOCH) {
                    Ok(elapsed) => elapsed.as_nanos() as i128,
                    Err(before) => -(before.duration().as_nanos() as i128),
                },
                Err(_) => 0,
            };
            Self::from_raw(
                metadata.len(),
                0,
                offset.div_euclid(1_000_000_000) as i64,
                offset.rem_euclid(1_000_000_000) as i64,
            )
        }
    }

    /// 文件长度（字节）。
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// 长度是否为 0。
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// inode 号，用来判断是不是同一个文件。
    #[must_use]
    pub const fn ino(&self) -> u64 {
        self.ino
    }

    /// 修改时间的秒数。
    #[must_use]
    pub const fn mtime(&self) -> i64 {
        self.mtime
    }

    /// 修改时间的纳秒部分。
    #[must_use]
    pub const fn mtime_nsec(&self) -> i64 {
        self.mtime_nsec
    }

    /// 修改时间，语义与 [`std::fs::Metadata::modified`] 一致：1970 年之前的 mtime 会得到
    /// 一个早于 `UNIX_EPOCH` 的时间，调用方据此跳过 `Last-Modified` 与 `ETag`。
    #[must_use]
    pub fn modified(&self) -> std::io::Result<SystemTime> {
        if self.mtime >= 0 {
            return Ok(UNIX_EPOCH + Duration::new(self.mtime as u64, self.mtime_nsec as u32));
        }
        // 负时间的 timespec 约定是 tv_sec 向下取整、tv_nsec ∈ [0, 1e9)，
        // 所以 (tv_sec, tv_nsec) = (-1, 0) 表示"epoch 之前整 1 秒"。
        // 用 unsigned_abs()-1 秒 + (1e9 - nsec) 纳秒凑出时长；nsec==0 时
        // 纳秒部分为 1e9，Duration::new 会自动进位为一整秒，结果一致。
        Ok(UNIX_EPOCH
            - Duration::new(
                self.mtime.unsigned_abs() - 1,
                (1_000_000_000 - self.mtime_nsec) as u32,
            ))
    }
}

/// A file with an associated name and metadata for HTTP serving.
///
/// `NamedFile` wraps a file handle with HTTP-specific functionality including:
///
/// - Automatic MIME type detection based on file extension
/// - ETag generation for caching
/// - Last-Modified header support
/// - Content-Disposition header for downloads
/// - HTTP Range request support for partial content
/// - Chunked transfer for large files
///
/// 项目通过 `builder_shared` 传入已打开的文件与元数据，再调用 `send` 或
/// `send_head` 写入响应。
///
/// # Content-Disposition
///
/// By default, text, images, video, and audio files are served with
/// `Content-Disposition: inline`, while other files use `attachment`.
/// XML-based documents are the exception: `image/svg+xml`, `text/xml`, `text/xsl`
/// and anything else carrying an `xml` subtype or a `+xml` suffix default to
/// `attachment` even though their top-level type is `image` or `text`, because a
/// browser rendering one as a document will run any script it contains in the
/// serving origin.
///
/// # Caching Headers
///
/// By default, `NamedFile` generates `ETag` and `Last-Modified` headers
/// and respects conditional request headers (`If-None-Match`, `If-Modified-Since`, etc.).
/// 构建时可以通过 builder 选项关闭。
#[derive(Debug)]
pub struct NamedFile {
    path: Arc<Path>,
    /// 共享持有：缓存、响应体与 sendfile 升级共用同一个 fd，每请求因此不必再 `dup`。
    /// sendfile 带显式 offset，不会移动共享的文件偏移量，所以并发共用是安全的。
    file: Arc<File>,
    modified: Option<SystemTime>,
    metadata: FileMeta,
    flags: BitFlags<Flag>,
    content_type: Arc<Mime>,
    content_disposition: Option<HeaderValue>,
    /// Pre-read content for small files, avoiding ChunkedFile + spawn_blocking overhead.
    preread: Option<Bytes>,
    /// 调用方已经算好的 `ETag`：命中缓存时由 [`NamedFile::set_etag`] 给出，
    /// 这样每请求就不必再 `format!` 一次再解析回来。
    etag_override: Option<ETag>,
}

/// [`NamedFile`] 的构建器。
#[derive(Clone, Debug)]
pub struct NamedFileBuilder {
    path: Arc<Path>,
    content_type: Option<Arc<Mime>>,
    preload_threshold: Option<u64>,
    flags: BitFlags<Flag>,
}
impl NamedFileBuilder {
    /// Disable `Content-Disposition` header.
    ///
    /// By default, the `Content-Disposition` header is enabled.
    #[inline]
    pub fn disable_content_disposition(&mut self) {
        self.flags.remove(Flag::ContentDisposition);
    }

    /// Sets content type and returns `Self`.
    #[inline]
    #[must_use]
    pub fn content_type(mut self, content_type: Arc<Mime>) -> Self {
        self.content_type = Some(content_type);
        self
    }

    /// Sets small-file preload threshold and returns `Self`.
    ///
    /// Files whose size is less than or equal to this threshold are read during build and sent from
    /// memory. Larger files are streamed in chunks. Set this to `0` to disable
    /// preloading for non-empty files.
    #[inline]
    #[must_use]
    pub fn preload_threshold(mut self, threshold: u64) -> Self {
        self.preload_threshold = Some(threshold);
        self
    }

    /// Specifies whether to use ETag or not.
    ///
    /// Default is true.
    #[inline]
    #[must_use]
    pub fn use_etag(mut self, value: bool) -> Self {
        if value {
            self.flags.insert(Flag::Etag);
        } else {
            self.flags.remove(Flag::Etag);
        }
        self
    }

    /// Specifies whether to use Last-Modified or not.
    ///
    /// Default is true.
    #[inline]
    #[must_use]
    pub fn use_last_modified(mut self, value: bool) -> Self {
        if value {
            self.flags.insert(Flag::LastModified);
        } else {
            self.flags.remove(Flag::LastModified);
        }
        self
    }

    /// Build a new [`NamedFile`] from an already-opened file and its metadata.
    ///
    /// `metadata` must describe that same open file, so that build skips the
    /// `fstat` an ordinary build performs. Callers that cached an earlier
    /// `fstat` of the same descriptor — and revalidated it against the current
    /// path — can pass it here.
    ///
    /// The file is shared rather than owned so that the caller keeps using the
    /// same descriptor for `sendfile(2)` without duplicating it.
    pub async fn build_from_file_with_metadata(
        self,
        file: Arc<File>,
        metadata: FileMeta,
    ) -> Result<NamedFile> {
        self.build_inner(Some(file), Some(metadata)).await
    }

    /// Shared implementation of [`Self::build`] and the `build_from_file*` variants.
    async fn build_inner(
        self,
        file: Option<Arc<File>>,
        metadata: Option<FileMeta>,
    ) -> Result<NamedFile> {
        let Self {
            path,
            content_type,
            preload_threshold,
            flags,
        } = self;

        let preload_threshold = preload_threshold.unwrap_or(PRELOAD_THRESHOLD);

        // Determine what charset detection is needed before the blocking call.
        let inferred_mime = content_type
            .clone()
            .or_else(|| mime_infer::from_path(&path).first().map(Arc::new));
        let needs_charset = inferred_mime
            .as_ref()
            .map(|m| is_charset_required_mime(m) && m.get_param("charset").is_none())
            .unwrap_or(false);
        let needs_detect = content_type.is_none() && path.extension().is_none();

        let needs_detection_sample = needs_charset || needs_detect;

        // 这里原本是 salvo 的 `tokio::task::spawn_blocking`。本项目的 /files 是热路径，
        // 而 tokio 的 blocking pool 只有一把全局 Mutex + Condvar，每次派发都要抢锁并唤醒线程，
        // 压测下来这一次 spawn_blocking 就占掉每请求约 4 次 futex 等待，比它省下的阻塞还贵。
        // open/metadata 命中页缓存时是微秒级，因此直接在 worker 上同步执行。
        struct FileInfo {
            file: Arc<File>,
            metadata: FileMeta,
            preread: Option<Vec<u8>>,
            detection_sample: DetectionSample,
        }

        enum DetectionSample {
            Empty,
            Preread(usize),
            Owned(Vec<u8>),
        }

        impl DetectionSample {
            fn as_slice<'a>(&'a self, preread: &'a [u8]) -> &'a [u8] {
                match self {
                    Self::Empty => &[],
                    Self::Preread(end) => &preread[..*end],
                    Self::Owned(sample) => sample,
                }
            }
        }
        let info = (|| -> std::io::Result<FileInfo> {
            // 调用方可能已经打开过这个文件（例如用 openat2 解析过路径），那就直接用它，
            // 不要再按路径打开一次。
            let file = match file {
                Some(file) => file,
                None => Arc::new(File::open(&path)?),
            };
            // 调用方可能已经 fstat 过同一个描述符（并把结果缓存下来做了校验），那就直接用，
            // 省掉这次 fstat。它必须描述的就是这个已打开的文件。
            let metadata = match metadata {
                Some(metadata) => metadata,
                None => FileMeta::from_metadata(&file.metadata()?),
            };
            let file_size = metadata.len();

            // For small files (size <= preload_threshold), read the entire content now.
            // This avoids ChunkedFile's spawn_blocking overhead later.
            let preread = if file_size <= preload_threshold {
                let mut buf = vec![0u8; file_size as usize];
                // 用 pread 读、不动共享的文件偏移量（同一个 fd 可能被多个请求共用）
                #[cfg(unix)]
                file.read_exact_at(&mut buf, 0)?;
                #[cfg(not(unix))]
                {
                    let mut owned = file.try_clone()?;
                    owned.read_exact(&mut buf)?;
                }
                Some(buf)
            } else {
                None
            };

            let mut detection_sample = DetectionSample::Empty;
            if needs_detection_sample {
                if let Some(preread) = &preread {
                    detection_sample = DetectionSample::Preread(cmp::min(1024, preread.len()));
                } else {
                    let mut sample = vec![0u8; cmp::min(1024, file_size) as usize];
                    // 用 pread 读、不动文件偏移量：调用方可能把同一个 fd 缓存下来给多个请求
                    // 共用（dup 共享同一个 file description），移动偏移量会互相干扰。
                    #[cfg(unix)]
                    file.read_exact_at(&mut sample, 0)?;
                    #[cfg(not(unix))]
                    {
                        let mut owned = file.try_clone()?;
                        owned.seek(SeekFrom::Start(0))?;
                        owned.read_exact(&mut sample)?;
                    }
                    detection_sample = DetectionSample::Owned(sample);
                }
            }

            Ok(FileInfo {
                file,
                metadata,
                preread,
                detection_sample,
            })
        })()
        .map_err(Error::Io)?;

        let file = info.file;

        // Resolve content type, using preread bytes for charset detection if needed.
        // 只有需要补 charset 时才克隆一份出来改，其余情况直接共享缓存里那个 `Arc`
        let content_type = if let Some(mime) = inferred_mime {
            if needs_charset {
                let mut mime = (*mime).clone();
                let sample = info
                    .detection_sample
                    .as_slice(info.preread.as_deref().unwrap_or(&[]));
                fill_mime_charset_if_need(&mut mime, sample);
                Arc::new(mime)
            } else {
                mime
            }
        } else if needs_detect {
            let sample = info
                .detection_sample
                .as_slice(info.preread.as_deref().unwrap_or(&[]));
            Arc::new(detect_text_mime(sample).unwrap_or(mime::APPLICATION_OCTET_STREAM))
        } else {
            Arc::new(mime::APPLICATION_OCTET_STREAM)
        };

        let preread = info.preread.map(Bytes::from);

        let content_disposition = Some(build_content_disposition(&path, &content_type)?);
        Ok(NamedFile {
            path,
            file,
            content_type,
            content_disposition,
            modified: info.metadata.modified().ok(),
            metadata: info.metadata,
            flags,
            preread,
            etag_override: None,
        })
    }
}
/// Whether a browser rendering `content_type` as a document could run script in
/// the serving origin.
///
/// An SVG carries `<script>` elements and event handler attributes, and any XML
/// document can name an XSLT stylesheet through `<?xml-stylesheet ?>` and render
/// scripted HTML from it. Both execute against the origin that served the file, so
/// serving one inline turns an uploaded "image" into stored XSS.
///
/// `application/*` XML types already fell on the `attachment` side because their
/// top-level type is not in the inline list. What slipped past were the `image`
/// and `text` spellings, which is every case below.
fn is_scriptable_xml(content_type: &Mime) -> bool {
    let subtype = content_type.subtype().as_str();
    // The `+xml` structured syntax suffix: `image/svg+xml`, `application/xslt+xml`.
    content_type.suffix() == Some(mime::XML)
        // `text/xml`, plus RFC 7303's `xml-dtd` and `xml-external-parsed-entity`,
        // which are XML but carry no suffix. Compare only the segment before the
        // first hyphen, so a subtype merely starting with those letters — say
        // `xmlish` — is not swept along.
        || subtype
            .split_once('-')
            .map_or(subtype, |(head, _)| head)
            .eq_ignore_ascii_case("xml")
        // `text/xsl` is the legacy type that `<?xml-stylesheet ?>` itself names,
        // and browsers parse it as XML.
        || subtype.eq_ignore_ascii_case("xsl")
}

fn build_content_disposition(
    file_path: impl AsRef<Path>,
    content_type: &Mime,
) -> Result<HeaderValue> {
    let disposition_type = if is_scriptable_xml(content_type) {
        "attachment"
    } else {
        match (content_type.type_(), content_type.subtype()) {
            (mime::IMAGE | mime::TEXT | mime::VIDEO | mime::AUDIO, _)
            | (_, mime::JAVASCRIPT | mime::JSON) => "inline",
            _ => "attachment",
        }
    };
    let content_disposition = if disposition_type == "attachment" {
        let attached_name = file_path
            .as_ref()
            .file_name()
            .map(|file_name| file_name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into());
        let quoted_filename = escape_quoted_filename(&attached_name);
        if quoted_filename == attached_name {
            format!(r#"attachment; filename="{quoted_filename}""#)
        } else {
            let encoded_filename =
                utf8_percent_encode(&attached_name, RFC5987_ATTR_CHAR_ENCODE_SET);
            format!(
                r#"attachment; filename="{quoted_filename}"; filename*=UTF-8''{encoded_filename}"#
            )
        }
        .parse::<HeaderValue>()
        .map_err(Error::other)?
    } else {
        disposition_type
            .parse::<HeaderValue>()
            .map_err(Error::other)?
    };
    Ok(content_disposition)
}

fn escape_quoted_filename(filename: &str) -> String {
    let mut escaped = String::with_capacity(filename.len());
    for ch in filename.chars() {
        match ch {
            '"' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            '\t' => escaped.push(' '),
            ch if ch.is_ascii_control() || !ch.is_ascii() => escaped.push('_'),
            ch => escaped.push(ch),
        }
    }
    escaped
}

impl NamedFile {
    /// Creates a new [`NamedFileBuilder`] from an already shared path.
    ///
    /// 路径来自缓存的 `Arc<Path>` 时用它：命中路径上不必再拷贝一次路径。
    #[inline]
    pub fn builder_shared(path: Arc<Path>) -> NamedFileBuilder {
        NamedFileBuilder {
            path,
            content_type: None,
            preload_threshold: None,
            flags: BitFlags::default(),
        }
    }

    /// Get content type value.
    #[inline]
    pub fn content_type(&self) -> Arc<Mime> {
        Arc::clone(&self.content_type)
    }

    /// Sets the `Content-Disposition` for serving this file. This allows
    /// changing the inline/attachment disposition as well as the filename
    /// sent to the peer.
    ///
    /// By default the disposition is `inline` for text, image, video and audio
    /// content types other than XML-based ones, and `attachment` for everything
    /// else. The filename is taken from the path provided in the `open` method
    /// after converting it to UTF-8 using
    /// [to_string_lossy](https://doc.rust-lang.org/std/ffi/struct.OsStr.html#method.to_string_lossy).
    #[inline]
    pub fn set_content_disposition(&mut self, content_disposition: HeaderValue) {
        self.content_disposition = Some(content_disposition);
        self.flags.insert(Flag::ContentDisposition);
    }

    /// Disable `Content-Disposition` header.
    ///
    /// By default, the `Content-Disposition` header is enabled.
    #[inline]
    pub fn disable_content_disposition(&mut self) {
        self.flags.remove(Flag::ContentDisposition);
    }

    /// Get ETag value.
    pub fn etag(&self) -> Option<ETag> {
        // This etag format is similar to Apache's.
        self.modified.as_ref().and_then(|mtime| {
            let ino = {
                #[cfg(unix)]
                {
                    self.metadata.ino()
                }
                #[cfg(not(unix))]
                {
                    0
                }
            };

            let dur = match mtime.duration_since(UNIX_EPOCH) {
                Ok(dur) => dur,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        path = %self.path.display(),
                        "skip file etag for modification time before unix epoch"
                    );
                    return None;
                }
            };
            let etag_str = format!(
                "\"{:x}-{:x}-{:x}-{:x}\"",
                ino,
                self.metadata.len(),
                dur.as_secs(),
                dur.subsec_nanos()
            );
            match etag_str.parse::<ETag>() {
                Ok(etag) => Some(etag),
                Err(e) => {
                    tracing::error!(error = ?e, etag = %etag_str, "set file's etag failed");
                    None
                }
            }
        })
    }
    /// 复用调用方已经算好的 `ETag`。
    ///
    /// `ETag` 只由元数据（inode、长度、mtime）决定，调用方按同一份元数据把它缓存下来之后，
    /// 命中时就不必每请求再格式化一次再解析回来。传入的值必须与这份元数据一致。
    #[inline]
    pub fn set_etag(&mut self, etag: ETag) {
        self.etag_override = Some(etag);
    }

    /// Get last modified value.
    #[inline]
    fn last_modified(&self) -> Option<SystemTime> {
        self.modified
    }

    fn encodable_last_modified(&self, mtime: SystemTime) -> Option<SystemTime> {
        if let Err(err) = mtime.duration_since(UNIX_EPOCH) {
            tracing::warn!(
                error = ?err,
                path = %self.path.display(),
                "skip file last-modified header for modification time before unix epoch"
            );
            None
        } else {
            Some(mtime)
        }
    }
    /// Consume self and send content to [`Response`].
    pub async fn send(self, req_headers: &HeaderMap, res: &mut Response) {
        self.send_inner(req_headers, res, true).await;
    }

    /// Consume self and send only the headers to [`Response`].
    ///
    /// This follows the same conditional and range handling as [`Self::send`], but does not attach
    /// a response body.
    pub async fn send_head(self, req_headers: &HeaderMap, res: &mut Response) {
        self.send_inner(req_headers, res, false).await;
    }

    async fn send_inner(mut self, req_headers: &HeaderMap, res: &mut Response, send_body: bool) {
        // 调用方给了算好的 ETag 就用它，否则按这份元数据现算
        let etag = if self.flags.contains(Flag::Etag) {
            self.etag_override.take().or_else(|| self.etag())
        } else {
            None
        };
        let last_modified = if self.flags.contains(Flag::LastModified) {
            self.last_modified()
        } else {
            None
        };

        // check preconditions
        let precondition_failed = if !any_match(etag.as_ref(), req_headers) {
            true
        } else if let (Some(last_modified), Some(since)) =
            (&last_modified, req_headers.typed_get::<IfUnmodifiedSince>())
        {
            let since: SystemTime = since.into();
            since < http_date_precision(*last_modified)
        } else {
            false
        };

        // check last modified
        let not_modified = if !none_match(etag.as_ref(), req_headers) {
            true
        } else if req_headers.contains_key(IF_NONE_MATCH) {
            false
        } else if let (Some(last_modified), Some(since)) =
            (&last_modified, req_headers.typed_get::<IfModifiedSince>())
        {
            let since: SystemTime = since.into();
            since >= http_date_precision(*last_modified)
        } else {
            false
        };

        // A caller may have already chosen the response's Content-Type. Default
        // the disposition from the type the client will actually receive, not
        // necessarily the type detected for the file on disk. Treat an invalid
        // pre-existing value conservatively as opaque binary data.
        // `take()` 得赶在下面 `effective_content_type` 之前：那个绑定会不可变借用
        // `self.content_type`，之后再想可变借用 `self` 就借不到了。标志同时存成一个 bool，
        // 下面插头时就不必隔着十几行再读一次 `self.flags`（读者也不用去确认它中间没被改）
        let wants_disposition = self.flags.contains(Flag::ContentDisposition);
        let content_disposition = if wants_disposition {
            self.content_disposition.take()
        } else {
            None
        };
        // 这里只借用、不克隆：`mime::Mime` 的 `Clone` 会深拷贝它内部的 `String`
        // （mime 0.3 的 `Source` 就是 `String`），每请求一次堆分配
        let borrowed_content_type;
        let effective_content_type = if res.headers().contains_key(CONTENT_TYPE) {
            borrowed_content_type = res.content_type().unwrap_or(mime::APPLICATION_OCTET_STREAM);
            &borrowed_content_type
        } else {
            &self.content_type
        };

        if wants_disposition {
            if let Some(content_disposition) = content_disposition {
                res.headers_mut()
                    .insert(CONTENT_DISPOSITION, content_disposition);
            } else if !res.headers().contains_key(CONTENT_DISPOSITION) {
                // skip to set CONTENT_DISPOSITION header if it is already set.
                match build_content_disposition(&self.path, effective_content_type) {
                    Ok(content_disposition) => {
                        res.headers_mut()
                            .insert(CONTENT_DISPOSITION, content_disposition);
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "build file's content disposition failed");
                    }
                }
            }
        }
        if !res.headers().contains_key(CONTENT_TYPE) {
            // 同上：只要类型的字符串形式，不必克隆整个 `Mime`
            if let Ok(value) = HeaderValue::from_str(AsRef::<str>::as_ref(&*self.content_type)) {
                res.headers_mut().insert(CONTENT_TYPE, value);
            }
        }
        if self.flags.contains(Flag::ContentTypeOptions)
            && !res.headers().contains_key(X_CONTENT_TYPE_OPTIONS)
        {
            res.headers_mut()
                .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
        }
        if let Some(lm) = last_modified.and_then(|lm| self.encodable_last_modified(lm)) {
            if !res.headers().contains_key(LAST_MODIFIED) {
                res.headers_mut().typed_insert(LastModified::from(lm));
            }
        }
        if let Some(etag) = etag {
            res.headers_mut().typed_insert(etag);
        }
        res.headers_mut()
            .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));

        let mut length = self.metadata.len();
        // Conditional request handling must precede Range processing: per RFC 7232
        // a `304 Not Modified` / `412 Precondition Failed` takes priority over the
        // `206`/`416` produced by a Range request.
        if precondition_failed {
            res.status_code(StatusCode::PRECONDITION_FAILED);
            return;
        } else if not_modified {
            res.status_code(StatusCode::NOT_MODIFIED);
            return;
        }

        let file_size = self.metadata.len();
        let mut offset = 0;
        let mut is_partial = false;

        // check for range header
        if let Some(range) = req_headers.get(RANGE) {
            let Ok(range) = range.to_str() else {
                res.status_code(StatusCode::BAD_REQUEST);
                return;
            };
            match HttpRange::parse(range, length) {
                // A single range is served as `206 Partial Content`.
                Ok(ranges) if ranges.len() == 1 => {
                    offset = ranges[0].start;
                    length = ranges[0].length;
                    is_partial = true;
                }
                // Multiple ranges would require a `multipart/byteranges` body, which
                // is not supported here. Per RFC 7233 the server may ignore the Range
                // header and return the full `200 OK` representation instead.
                Ok(ranges) if ranges.len() > 1 => {}
                // Empty / unsatisfiable range.
                _ => {
                    res.headers_mut()
                        .typed_insert(ContentRange::unsatisfied_bytes(length));
                    res.status_code(StatusCode::RANGE_NOT_SATISFIABLE);
                    return;
                }
            }
        }

        if is_partial {
            // Range request
            res.status_code(StatusCode::PARTIAL_CONTENT);
            // Single source of truth for the byte count, clamped to the file so the
            // `Content-Range`, `Content-Length` and the body always agree.
            let total_size = length.min(file_size.saturating_sub(offset));
            match ContentRange::bytes(offset..offset.saturating_add(total_size), file_size) {
                Ok(content_range) => {
                    res.headers_mut().typed_insert(content_range);
                }
                Err(e) => {
                    tracing::error!(error = ?e, "set file's content range failed");
                }
            }
            res.headers_mut().typed_insert(ContentLength(total_size));

            if !send_body {
                return;
            }

            // Fast path: slice from preread bytes if available
            if let Some(preread) = self.preread.take() {
                let end = cmp::min(offset.saturating_add(total_size) as usize, preread.len());
                let start = cmp::min(offset as usize, end);
                res.replace_body(ResBody::Once(preread.slice(start..end)));
            } else {
                // 回退到普通响应体：pread 不移动共享 fd 的偏移量。
                let reader =
                    ChunkedFile::new(Arc::clone(&self.file), offset, total_size, CHUNK_SIZE);
                res.stream(reader);
            }
        } else {
            // Full file response
            res.status_code(StatusCode::OK);
            res.headers_mut().typed_insert(ContentLength(length));

            if !send_body {
                return;
            }

            // Fast path: send preread bytes directly — zero spawn_blocking calls
            if let Some(preread) = self.preread.take() {
                res.replace_body(ResBody::Once(preread));
            } else {
                // 回退到普通响应体：pread 不移动共享 fd 的偏移量。
                let reader = ChunkedFile::new(Arc::clone(&self.file), offset, length, CHUNK_SIZE);
                res.stream(reader);
            }
        }
    }
}

fn http_date_precision(time: SystemTime) -> SystemTime {
    match time.duration_since(UNIX_EPOCH) {
        Ok(dur) => UNIX_EPOCH + Duration::from_secs(dur.as_secs()),
        Err(err) => {
            let dur = err.duration();
            let secs = dur.as_secs() + u64::from(dur.subsec_nanos() > 0);
            UNIX_EPOCH
                .checked_sub(Duration::from_secs(secs))
                .unwrap_or(time)
        }
    }
}

/// Returns true if `req_headers` has no `If-Match` header or one which matches `etag`.
fn any_match(etag: Option<&ETag>, req_headers: &HeaderMap) -> bool {
    match req_headers.typed_get::<IfMatch>() {
        None => true,
        Some(if_match) => {
            if if_match == IfMatch::any() {
                true
            } else if let Some(etag) = etag {
                if_match.precondition_passes(etag)
            } else {
                false
            }
        }
    }
}

/// Returns true if `req_headers` doesn't have an `If-None-Match` header matching `req`.
fn none_match(etag: Option<&ETag>, req_headers: &HeaderMap) -> bool {
    match req_headers.typed_get::<IfNoneMatch>() {
        None => true,
        Some(if_none_match) => {
            if if_none_match == IfNoneMatch::any() {
                false
            } else if let Some(etag) = etag {
                if_none_match.precondition_passes(etag)
            } else {
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn build(path: &Path) -> NamedFile {
        let file = File::open(path).expect("open file");
        let metadata = FileMeta::from_metadata(&file.metadata().expect("read metadata"));
        NamedFile::builder_shared(Arc::from(path))
            .build_from_file_with_metadata(Arc::new(file), metadata)
            .await
            .expect("build named file")
    }

    async fn build_preload_zero(path: &Path, content_type: Option<Arc<Mime>>) -> NamedFile {
        let file = File::open(path).expect("open file");
        let metadata = FileMeta::from_metadata(&file.metadata().expect("read metadata"));
        let mut builder = NamedFile::builder_shared(Arc::from(path)).preload_threshold(0);
        if let Some(content_type) = content_type {
            builder = builder.content_type(content_type);
        }
        builder
            .build_from_file_with_metadata(Arc::new(file), metadata)
            .await
            .expect("build named file")
    }

    #[test]
    fn content_disposition_escapes_quoted_filename() {
        let value =
            build_content_disposition("report\"\\\r\n.txt", &mime::APPLICATION_OCTET_STREAM)
                .unwrap();

        assert_eq!(
            value.to_str().unwrap(),
            r#"attachment; filename="report\"\\__.txt"; filename*=UTF-8''report%22%5C%0D%0A.txt"#
        );
    }

    fn default_disposition_for(content_type: &Mime) -> String {
        build_content_disposition("upload.bin", content_type)
            .expect("build content disposition")
            .to_str()
            .expect("header is ascii")
            .to_owned()
    }

    #[test]
    fn scriptable_xml_defaults_to_attachment() {
        // An SVG can carry <script>/onload, and any XML document can pull in an
        // XSLT stylesheet that renders scripted HTML. Serving either inline from
        // the app's own origin turns an uploaded "image" into stored XSS.
        for content_type in [
            mime::IMAGE_SVG,
            mime::TEXT_XML,
            "application/xml".parse().expect("parse mime"),
            "application/xhtml+xml".parse().expect("parse mime"),
            "application/rss+xml".parse().expect("parse mime"),
        ] {
            assert!(
                default_disposition_for(&content_type).starts_with("attachment"),
                "{content_type} must not default to inline"
            );
        }
    }

    #[test]
    fn non_xml_media_still_defaults_to_inline() {
        // The XML carve-out must not pull ordinary media off the inline path.
        for content_type in [
            mime::IMAGE_PNG,
            mime::TEXT_PLAIN,
            // A static file server exists to serve HTML documents inline.
            mime::TEXT_HTML,
            "video/mp4".parse().expect("parse mime"),
            "audio/mpeg".parse().expect("parse mime"),
            "text/javascript".parse().expect("parse mime"),
        ] {
            assert_eq!(
                default_disposition_for(&content_type),
                "inline",
                "{content_type} must stay inline"
            );
        }
    }

    #[test]
    fn xml_types_without_the_suffix_default_to_attachment() {
        // These name XML without carrying an `xml` subtype or a `+xml` suffix, so
        // the structural checks alone would let them through on `text`.
        for content_type in [
            // The type `<?xml-stylesheet type="text/xsl" ?>` itself names.
            "text/xsl",
            "text/xml-external-parsed-entity",
            "text/xml-dtd",
            // Matching is case-insensitive, as MIME comparisons are.
            "TEXT/XSL",
            "Text/XML",
        ] {
            let content_type = content_type.parse().expect("parse mime");
            assert!(
                default_disposition_for(&content_type).starts_with("attachment"),
                "{content_type} must not default to inline"
            );
        }
    }

    #[test]
    fn xml_lookalike_subtypes_stay_inline() {
        // The XML carve-out keys on the type actually being XML; a subtype that
        // merely starts with the same letters must not be dragged along.
        for content_type in ["text/xmlish", "image/xslfoo", "text/xsl-but-not"] {
            let content_type = content_type.parse().expect("parse mime");
            assert_eq!(
                default_disposition_for(&content_type),
                "inline",
                "{content_type} must stay inline"
            );
        }
    }

    #[tokio::test]
    async fn svg_is_served_as_attachment_with_nosniff() {
        use std::io::Write as _;

        let mut file = tempfile::Builder::new()
            .suffix(".svg")
            .tempfile()
            .expect("create temp file");
        file.write_all(
            br#"<svg xmlns="http://www.w3.org/2000/svg" onload="alert(document.domain)"/>"#,
        )
        .expect("write svg");
        file.flush().expect("flush");

        let named = build(file.path()).await;
        // The file must be recognised *as* an SVG, otherwise this test would also
        // pass on an unidentified file falling back to `application/octet-stream`.
        assert_eq!(*named.content_type(), mime::IMAGE_SVG);

        let mut res = Response::new();
        named.send(&HeaderMap::new(), &mut res).await;

        let disposition = res
            .headers()
            .get(CONTENT_DISPOSITION)
            .expect("content-disposition is set")
            .to_str()
            .expect("header is ascii");
        assert!(
            disposition.starts_with("attachment"),
            "svg served with `{disposition}`"
        );
        assert_eq!(
            res.headers()
                .get(X_CONTENT_TYPE_OPTIONS)
                .map(|v| v.to_str().expect("header is ascii")),
            Some("nosniff")
        );
    }

    #[tokio::test]
    async fn preload_threshold_zero_still_detects_extensionless_text_mime() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("README");
        std::fs::write(&path, b"plain text content").expect("write extensionless file");

        let named = build_preload_zero(&path, None).await;

        assert_eq!(named.content_type().type_(), mime::TEXT);
        assert_eq!(named.content_type().subtype(), mime::PLAIN);
        assert!(named.preread.is_none());
    }

    #[tokio::test]
    async fn preload_threshold_zero_still_sniffs_charset() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("data.json");
        std::fs::write(&path, br#"{"message":"hello"}"#).expect("write json file");

        let named = build_preload_zero(&path, None).await;

        assert_eq!(named.content_type().type_(), mime::APPLICATION);
        assert_eq!(named.content_type().subtype(), mime::JSON);
        assert_eq!(
            named
                .content_type()
                .get_param("charset")
                .map(|v| v.as_str()),
            Some("utf-8")
        );
        assert!(named.preread.is_none());
    }

    #[tokio::test]
    async fn send_head_sets_headers_without_body() {
        use salvo::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_TYPE};
        use salvo::http::{HeaderMap, Response};

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("hello.txt");
        std::fs::write(&path, b"hello").expect("write file");

        let named = build_preload_zero(&path, Some(Arc::new(mime::TEXT_PLAIN))).await;
        let mut res = Response::new();
        named.send_head(&HeaderMap::new(), &mut res).await;

        assert_eq!(res.status_code, Some(StatusCode::OK));
        assert_eq!(res.headers().get(CONTENT_LENGTH).unwrap(), "5");
        assert_eq!(res.headers().get(ACCEPT_RANGES).unwrap(), "bytes");
        assert!(
            res.headers()
                .get(CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        assert!(res.body.is_none());
    }

    #[tokio::test]
    async fn set_etag_is_sent_instead_of_the_computed_one() {
        use salvo::http::header::ETAG;
        use salvo::http::{HeaderMap, Response};

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("hello.txt");
        std::fs::write(&path, b"hello").expect("write file");

        let mut named = build_preload_zero(&path, None).await;
        let computed = named.etag().expect("regular file has an etag");
        let reused = "\"cached-1\"".parse::<ETag>().expect("parse etag");
        named.set_etag(reused.clone());
        let mut res = Response::new();
        named.send_head(&HeaderMap::new(), &mut res).await;

        assert_ne!(computed, reused, "复用的 ETag 得与现算的不一样才有意义");
        assert_eq!(
            res.headers().typed_get::<ETag>(),
            Some(reused),
            "调用方给的 ETag 必须盖掉现算的那个"
        );
        assert_eq!(res.headers().get(ETAG).unwrap(), "\"cached-1\"");
    }

    #[tokio::test]
    async fn etag_returns_none_for_pre_epoch_modified_time() {
        use std::io::Write as _;
        use std::time::Duration;

        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(b"hello").expect("write file");
        file.flush().expect("flush");

        let mut named = build(file.path()).await;
        named.modified = Some(UNIX_EPOCH - Duration::from_secs(1));

        assert_eq!(named.etag(), None);
    }

    #[tokio::test]
    async fn send_skips_last_modified_for_pre_epoch_modified_time() {
        use std::io::Write as _;
        use std::time::Duration;

        use salvo::http::header::{IF_MODIFIED_SINCE, LAST_MODIFIED};
        use salvo::http::{HeaderMap, Response};

        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(b"hello").expect("write file");
        file.flush().expect("flush");

        let mut named = build(file.path()).await;
        let pre_epoch = UNIX_EPOCH - Duration::from_secs(1);
        named.modified = Some(pre_epoch);
        named.flags.remove(Flag::Etag);
        assert_eq!(named.last_modified(), Some(pre_epoch));

        let mut headers = HeaderMap::new();
        headers.insert(
            IF_MODIFIED_SINCE,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"),
        );
        let mut res = Response::new();
        named.send(&headers, &mut res).await;

        assert_eq!(res.status_code, Some(StatusCode::NOT_MODIFIED));
        assert!(!res.headers().contains_key(LAST_MODIFIED));
    }

    #[tokio::test]
    async fn send_if_modified_since_uses_http_date_precision() {
        use std::io::Write as _;
        use std::time::Duration;

        use salvo::http::header::IF_MODIFIED_SINCE;
        use salvo::http::{HeaderMap, Response};

        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(b"hello").expect("write file");
        file.flush().expect("flush");

        let mut named = build(file.path()).await;
        named.modified =
            Some(UNIX_EPOCH + Duration::from_secs(100) + Duration::from_nanos(500_000_000));
        named.flags.remove(Flag::Etag);

        let mut headers = HeaderMap::new();
        headers.insert(
            IF_MODIFIED_SINCE,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:01:40 GMT"),
        );
        let mut res = Response::new();
        named.send(&headers, &mut res).await;

        assert_eq!(res.status_code, Some(StatusCode::NOT_MODIFIED));
    }

    #[tokio::test]
    async fn send_if_unmodified_since_uses_http_date_precision() {
        use std::io::Write as _;
        use std::time::Duration;

        use salvo::http::header::IF_UNMODIFIED_SINCE;
        use salvo::http::{HeaderMap, Response};

        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(b"hello").expect("write file");
        file.flush().expect("flush");

        let mut named = build(file.path()).await;
        named.modified =
            Some(UNIX_EPOCH + Duration::from_secs(100) + Duration::from_nanos(500_000_000));
        named.flags.remove(Flag::Etag);

        let mut headers = HeaderMap::new();
        headers.insert(
            IF_UNMODIFIED_SINCE,
            HeaderValue::from_static("Thu, 01 Jan 1970 00:01:40 GMT"),
        );
        let mut res = Response::new();
        named.send(&headers, &mut res).await;

        assert_eq!(res.status_code, Some(StatusCode::OK));
    }

    #[test]
    fn content_disposition_preserves_non_ascii_with_filename_star() {
        let value = build_content_disposition("报告.csv", &mime::APPLICATION_OCTET_STREAM).unwrap();

        assert_eq!(
            value.to_str().unwrap(),
            "attachment; filename=\"__.csv\"; filename*=UTF-8''%E6%8A%A5%E5%91%8A.csv"
        );
    }
}
