use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use lanfile_namedfile::{FileMeta, NamedFile};
use lanfile_sendfile::{SendfileSlot, upgrade_response};
use mime::Mime;
use rust_embed::RustEmbed;
#[cfg(any(target_os = "linux", target_os = "android"))]
use rustix::fd::OwnedFd;
#[cfg(any(target_os = "linux", target_os = "android"))]
use rustix::fs::{self as rfs, Advice, FileType, Mode, OFlags, ResolveFlags};
#[cfg(any(target_os = "linux", target_os = "android"))]
use salvo::http::header::CONTENT_DISPOSITION;
use salvo::http::header::LAST_MODIFIED;
use salvo::{
    http::{HeaderValue, Method, headers::ETag},
    prelude::*,
    routing::filters,
    serve_static::static_embed,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
mod file_cache;

#[cfg(any(target_os = "linux", target_os = "android"))]
use file_cache::FileCache;

#[derive(RustEmbed)]
#[folder = "static/"]
pub struct Asset;

/// `/pull` 的类型固定是 `application/octet-stream`：`Arc<Mime>` 只建一次，之后每次请求
/// 只加一次引用计数，不必每请求都 `Arc::new` 一份（`Mime` 内部是 `String`，那是真的堆分配）。
static OCTET_STREAM: LazyLock<Arc<Mime>> =
    LazyLock::new(|| Arc::new(mime::APPLICATION_OCTET_STREAM));

/// 缓存里可以跨请求复用的那一部分：解析好的类型与已经编码好的响应头。
///
/// 这几项只由（路径, 元数据）决定，而缓存命中又要求元数据逐项一致，所以命中时直接拿来用就是
/// 对的：`ETag` 省掉每请求一次 `format!` 加解析，`Content-Disposition` 省掉每请求一次的转义
/// 与拼接。缓存只在 Linux/Android 上启用，其他平台上这个类型只会以 `None` 出现。
#[derive(Clone)]
struct CachedHeaders {
    /// 解析出来的 `Content-Type`（需要时已带上 `charset=`）。必须交给 `NamedFileBuilder`：
    /// 不给它的话，`NamedFile` 会自己 `pread` 一段文件样本去嗅探类型，那是一次系统调用。
    /// 用 `Arc` 共享：`Mime` 的 `Clone` 会深拷贝它内部的 `String`，命中路径上不该付这份钱
    content_type: Arc<Mime>,
    /// 已经编码好的 `Last-Modified`（文件时间早于 epoch 时没有），省掉每请求一次日期格式化
    last_modified: Option<HeaderValue>,
    /// 已经编码好的 `ETag`（文件时间早于 epoch 时没有）
    etag: Option<ETag>,
    /// 已经编码好的 `Content-Disposition`
    disposition: Option<HeaderValue>,
}

pub struct ServeFiles {
    root: PathBuf,
    /// root 的目录 fd：`openat2` 相对它解析路径，越界由内核直接拦下。
    #[cfg(any(target_os = "linux", target_os = "android"))]
    root_fd: Option<OwnedFd>,
    /// seccomp filter 可能直接杀死 `openat2` 调用者，因此必须先一次性探测。
    #[cfg(any(target_os = "linux", target_os = "android"))]
    openat2_allowed: bool,
    /// 已打开文件的缓存：命中时省掉 metadata、open 与 fadvise
    #[cfg(any(target_os = "linux", target_os = "android"))]
    cache: FileCache,
}

/// [`ServeFiles::open`] 的返回值：拼好的路径、fd、元数据，以及命中时已经编码好的响应头。
struct Opened {
    path: Arc<Path>,
    file: Arc<File>,
    metadata: FileMeta,
    cached_headers: Option<Arc<CachedHeaders>>,
}

/// 从已经写完响应头的 `Response` 里取回编码好的 `Last-Modified`。
///
/// 它是 `send_inner` 在发送时按同一份元数据写上去的，取回来存缓存即可，不必自己再编码。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn encoded_last_modified(res: &Response) -> Option<HeaderValue> {
    res.headers().get(LAST_MODIFIED).cloned()
}

/// 把已经构造好的 [`NamedFile`] 写进响应，需要时升级成 sendfile 零拷贝体。
///
/// `HEAD` 与「本平台确实有 sendfile」这两种情况都只写响应头，正文交给 [`upgrade_response`]
/// 换成零拷贝体；只有没有 sendfile 可用的平台才让 [`NamedFile`] 自己把正文写出来。
/// `file` 只在真的要升级时才克隆：`/files` 缓存未命中时还需要它去插缓存，所以按引用进来。
async fn send_named_file(
    named_file: NamedFile,
    req: &Request,
    res: &mut Response,
    slot: &SendfileSlot,
    file: &Arc<File>,
) {
    let head_only = req.method() == Method::HEAD;
    if head_only || cfg!(any(target_os = "linux", target_os = "android")) {
        named_file.send_head(req.headers(), res).await;
    } else {
        named_file.send(req.headers(), res).await;
    }
    if !head_only {
        upgrade_response(slot, res, Arc::clone(file));
    }
}

impl ServeFiles {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            root_fd: rfs::open(
                &root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .ok(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            openat2_allowed: !seccomp_filter_installed(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            cache: FileCache::default(),
            root,
        }
    }

    /// 打开请求路径对应的文件。
    ///
    /// 有效期外先打开文件并用 fd 元数据校验缓存；命中时直接给出缓存里的 fd、
    /// 元数据以及解析好的响应头，未命中才沿用刚打开的文件。
    fn open(&self, sub: &str) -> Option<Opened> {
        // 有效期内直接复用缓存条目，不打开文件。
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(hit) = self.cache.get_fresh(sub) {
            return Some(Opened {
                path: hit.joined,
                file: hit.file,
                metadata: hit.metadata,
                cached_headers: Some(hit.headers),
            });
        }
        let joined = self.root.join(sub);
        let opened = self.open_confirmed(sub, &joined);
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if opened.is_none() {
            self.cache.remove(sub);
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some((_, metadata)) = &opened
            && let Some(hit) = self.cache.get(sub, metadata)
        {
            return Some(Opened {
                path: hit.joined,
                file: hit.file,
                metadata: hit.metadata,
                cached_headers: Some(hit.headers),
            });
        }
        let (file, metadata) = opened?;
        Self::advise_sequential(&file, &metadata);
        Some(Opened {
            path: Arc::from(joined),
            file,
            metadata,
            cached_headers: None,
        })
    }

    /// `/pull` 的打开：与 [`Self::open`] 同构，但不查缓存、不写缓存。
    ///
    /// `lanfile get` 每个文件只请求一次，缓存不会有命中，却要为它加一次分片锁、分配一个 key，
    /// 分片满时还得扫一遍 LRU；下载出来的 fd 还会把 `/files` 缓存里的热文件挤出去。这条路上
    /// 整段跳过 [`FileCache`]。
    fn open_no_cache(&self, sub: &str) -> Option<(Arc<Path>, Arc<File>, FileMeta)> {
        let joined = self.root.join(sub);
        let (file, metadata) = self.open_confirmed(sub, &joined)?;
        Self::advise_sequential(&file, &metadata);
        Some((Arc::from(joined), file, metadata))
    }

    /// 打开文件并取 fd 自己的元数据，同时确认它是普通文件。
    fn open_confirmed(&self, sub: &str, joined: &Path) -> Option<(Arc<File>, FileMeta)> {
        let file = self.open_uncached(sub, joined)?;
        let (metadata, is_file) = fd_meta(&file).ok()?;
        if !is_file {
            return None;
        }
        Some((Arc::new(file), metadata))
    }

    /// 内核顺序读提示：扩大预读窗口，大文件连续传输更快；仅设置标志、立即返回。
    fn advise_sequential(file: &File, metadata: &FileMeta) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if metadata.len() > 4096 {
            let _ = rfs::fadvise(file, 0, None, Advice::Sequential);
        }
    }

    #[cfg_attr(
        not(any(target_os = "linux", target_os = "android")),
        allow(unused_variables)
    )]
    fn open_uncached(&self, sub: &str, joined: &Path) -> Option<File> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.openat2_allowed
            && let Some(file) = self.root_fd.as_ref().and_then(|fd| open_beneath(fd, sub))
        {
            return Some(file);
        }
        open_via_canonicalize(&self.root, joined)
    }
}

/// 取已打开 fd 的元数据，并确认它是普通文件。
#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
fn fd_meta(file: &File) -> std::io::Result<(FileMeta, bool)> {
    let stat = rfs::fstat(file)?;
    Ok((
        FileMeta::from_raw(
            stat.st_size as u64,
            stat.st_ino as u64,
            stat.st_mtime as i64,
            stat.st_mtime_nsec as i64,
        ),
        FileType::from_raw_mode(stat.st_mode).is_file(),
    ))
}

/// 其他平台没有直接发 `fstat` 的分支，退回 `std` 的元数据，字段值一致。
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn fd_meta(file: &File) -> std::io::Result<(FileMeta, bool)> {
    file.metadata()
        .map(|metadata| (FileMeta::from_metadata(&metadata), metadata.is_file()))
}

/// 一次 `openat2` 完成路径解析、越界检查与打开。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn open_beneath(root_fd: &OwnedFd, sub: &str) -> Option<File> {
    rfs::openat2(
        root_fd,
        sub,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
        ResolveFlags::BENEATH | ResolveFlags::NO_MAGICLINKS,
    )
    .ok()
    .map(File::from)
}

/// `openat2` 不可用时的回退路径。
fn open_via_canonicalize(root: &Path, joined: &Path) -> Option<File> {
    let canonical = std::fs::canonicalize(joined).ok()?;
    if !canonical.starts_with(root) {
        return None;
    }
    File::open(canonical).ok()
}

/// 判断当前进程是否装了 seccomp filter；读不到时按已安装处理。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn seccomp_filter_installed() -> bool {
    std::fs::read_to_string("/proc/self/status").map_or(true, |status| has_seccomp_filter(&status))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn has_seccomp_filter(status: &str) -> bool {
    status.lines().any(|line| {
        line.strip_prefix("Seccomp:")
            .is_some_and(|v| v.trim() == "2")
    })
}

impl ServeFiles {
    /// `/files` 的实际实现，`sub` 是已经解码好的子路径。
    ///
    /// salvo 的 handler 与 hyper 快路径共用这一个入口：两条路唯一的差别是错误页由谁补
    /// （salvo 侧是 catcher，快路径自己渲染），响应本身完全一致。找不到时只设状态码。
    pub async fn serve(&self, sub: &str, req: &Request, res: &mut Response, slot: &SendfileSlot) {
        // 路径解析直接在 worker 上做：只有 metadata + open，命中页缓存时是微秒级，
        // 而 spawn_blocking 的线程交接本身就要几十微秒，还得分摊 blocking pool 的全局锁。
        // 用阻塞线程池反而更慢：压测显示这一次 spawn_blocking 就占掉每请求约 7 次 futex 等待
        let Some(Opened {
            path,
            file,
            metadata,
            cached_headers: cached,
        }) = self.open(sub)
        else {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        };

        // 关闭 NamedFile 的小文件预读：预读会把内容读进用户态，而 sendfile 直接从页缓存发，
        // 那次读纯属浪费；关掉后 Linux/Android 上的正文都交给 sendfile 零拷贝发，
        // 其他平台退回 NamedFile 自己的流式响应体，HEAD 本来也不需要预读。
        // 路径走共享的 `Arc<Path>`：命中时它来自缓存，不必每请求再拼一次
        let mut builder = NamedFile::builder_shared(Arc::clone(&path)).preload_threshold(0);
        // 命中时做两件事：类型交给 builder（否则它会自己去 pread 样本嗅探，那是系统调用），
        // 编码好的 `Last-Modified` 直接塞进响应头——`send_inner` 见到已经存在就不会再格式化。
        // 这里**不能**预置 `Content-Type`：`send_inner` 见到它就会走 `res.content_type()`，
        // 把头部重新解析成一个 `Mime`，比它省掉的那次 `from_str` 贵得多
        //
        // 先降成 `Option<&CachedHeaders>`：ETag 与 Content-Disposition 下面还要用，都从这一个
        // 绑定上取，不必各自再借一次
        let cached = cached.as_deref();
        if let Some(cached) = cached {
            builder = builder.content_type(Arc::clone(&cached.content_type));
            if let Some(last_modified) = &cached.last_modified {
                res.headers_mut()
                    .insert(LAST_MODIFIED, last_modified.clone());
            }
        }
        // 元数据跟着缓存一起给出来（未命中时是刚 fstat 的），所以这里不必再 fstat 一次
        let Ok(mut named_file) = builder
            .build_from_file_with_metadata(Arc::clone(&file), metadata.clone())
            .await
        else {
            res.render(StatusError::internal_server_error().brief("read file failed"));
            return;
        };
        // 命中时连 ETag 与 Content-Disposition 也一起复用：这两项同样只由（路径, 元数据,
        // 类型）决定，命中既然要求元数据逐项一致，缓存里那份就是这次该发的那份。
        // 未命中则在这里按同一份元数据算一次 ETag 交给它，既省掉它在 send 里再算一遍，
        // 也留一份给下面写缓存，不必再从响应头里解析回来
        let etag = match cached {
            Some(cached) => cached.etag.clone(),
            None => named_file.etag(),
        };
        if let Some(etag) = &etag {
            named_file.set_etag(etag.clone());
        }
        if let Some(disposition) = cached.and_then(|cached| cached.disposition.clone()) {
            named_file.set_content_disposition(disposition);
        }
        // send 会消费掉 named_file，未命中时要写进缓存的那份类型得先取出来
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let resolved_type = cached.is_none().then(|| named_file.content_type());
        send_named_file(named_file, req, res, slot, &file).await;

        // 未命中：编码好的头 `send` 已经写进 `res` 了，直接取回来存缓存，存下来的就是这次
        // 真正发出去的那一份；类型得在 send 之前取，因为 send 会消费掉 named_file
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(content_type) = resolved_type {
            self.cache.insert(
                sub,
                path,
                file,
                metadata,
                Arc::new(CachedHeaders {
                    content_type,
                    last_modified: encoded_last_modified(res),
                    etag,
                    disposition: res.headers().get(CONTENT_DISPOSITION).cloned(),
                }),
            );
        }
    }

    /// `/pull` 的实际实现：与 [`Self::serve`] 同构，但不碰 fd 缓存，且只写最少的响应头。
    ///
    /// 面向 `lanfile get` 的一次性批量拉取：响应头它一个都不看，所以 `ETag`、`Last-Modified`
    /// 与 `Content-Disposition` 都不编码，类型固定成 `application/octet-stream`——调用方给了
    /// 类型，`NamedFile` 建响应体时就不必再 `pread` 一段样本去嗅探。正文照旧交给 sendfile 零拷贝发送。
    pub async fn serve_raw(
        &self,
        sub: &str,
        req: &Request,
        res: &mut Response,
        slot: &SendfileSlot,
    ) {
        let Some((path, file, metadata)) = self.open_no_cache(sub) else {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        };

        let mut builder = NamedFile::builder_shared(Arc::clone(&path))
            .preload_threshold(0)
            .content_type(Arc::clone(&OCTET_STREAM))
            .use_etag(false)
            .use_last_modified(false);
        // 拉取客户端不保存也不展示，用不到 disposition 的转义与拼接
        builder.disable_content_disposition();
        let Ok(named_file) = builder
            .build_from_file_with_metadata(Arc::clone(&file), metadata)
            .await
        else {
            res.render(StatusError::internal_server_error().brief("read file failed"));
            return;
        };
        // 与 `/files` 一致：HEAD 与 sendfile 都只写响应头，正文交给 upgrade_response 换成零拷贝体
        send_named_file(named_file, req, res, slot, &file).await;
    }
}

/// `/`（内嵌的 `index.html`）与 `/static/*`（内嵌资源）这两条 salvo 路由。
///
/// `/files/*` 与 `/pull/*` 不在这里登记：它们由 `app` 的 hyper 快路径在 salvo 路由之前
/// 直接服务掉，salvo 这边永远收不到这两条。原来给它们挂的 salvo 路由只服务于测试、且会
/// 让人误以为生产里也走 salvo，已移除；需要这两条的测试改走真正的 `serve`（快路径）。
#[must_use]
pub fn static_routes() -> Router {
    Router::new()
        .push(
            Router::new()
                .filter(filters::get())
                .goal(static_embed::<Asset>().fallback("index.html")),
        )
        .push(
            Router::with_path("/static/{**path}")
                .filter(filters::get())
                .goal(static_embed::<Asset>()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 临时 root：`root/ok.txt`、`root/sub/deep.txt`
    struct Fixture {
        base: PathBuf,
        root: PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> std::io::Result<Self> {
            let base =
                std::env::temp_dir().join(format!("lanfile-assets-{tag}-{}", std::process::id()));
            let root = base.join("root");
            std::fs::create_dir_all(root.join("sub"))?;
            std::fs::write(root.join("ok.txt"), b"hello")?;
            std::fs::write(root.join("sub/deep.txt"), b"deep")?;
            Ok(Self { base, root })
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.base);
        }
    }

    /// 普通文件能打开，目录与缺失路径不能当作文件服务
    #[test]
    fn open_handles_basic_paths() -> std::io::Result<()> {
        let fixture = Fixture::new("basic")?;
        let files = ServeFiles::new(fixture.root.clone());
        assert!(files.open("ok.txt").is_some());
        assert!(files.open("sub/deep.txt").is_some());
        assert!(files.open("missing.txt").is_none());
        assert!(files.open("sub").is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_paths_leaving_root() -> std::io::Result<()> {
        let fixture = Fixture::new("escapes")?;
        std::fs::write(fixture.base.join("outside.txt"), b"secret")?;
        std::fs::create_dir_all(fixture.base.join("outside-dir"))?;
        std::fs::write(fixture.base.join("outside-dir/secret.txt"), b"secret")?;
        std::os::unix::fs::symlink(fixture.base.join("outside-dir"), fixture.root.join("link"))?;
        std::os::unix::fs::symlink(
            fixture.base.join("outside.txt"),
            fixture.root.join("outside-link.txt"),
        )?;
        let files = ServeFiles::new(fixture.root.clone());
        assert!(files.open("../outside.txt").is_none());
        assert!(files.open("outside-link.txt").is_none());
        assert!(files.open("link/secret.txt").is_none());
        Ok(())
    }

    /// 缓存命中时 `open` 要把 fd、元数据与已经编码好的响应头一起给出来：构建时不再 fstat、
    /// 不再读嗅探样本，也不再重算 `ETag` 与 `Content-Disposition`
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn cache_hit_reuses_the_open_file() -> std::io::Result<()> {
        let fixture = Fixture::new("cache")?;
        let files = ServeFiles::new(fixture.root.clone());
        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(async {
                let Some(Opened {
                    path: joined,
                    file,
                    metadata,
                    cached_headers: cached,
                }) = files.open("ok.txt")
                else {
                    panic!("第一次应当打开成功");
                };
                assert!(cached.is_none(), "第一次不该命中");
                let named =
                    NamedFile::builder_shared(Arc::from(fixture.root.join("ok.txt").as_path()))
                        .preload_threshold(0)
                        .build_from_file_with_metadata(Arc::clone(&file), metadata.clone())
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                let Ok(etag) = "\"ok-1\"".parse::<ETag>() else {
                    unreachable!("写死的 ETag 应当能解析");
                };
                let disposition = HeaderValue::from_static("inline");
                files.cache.insert(
                    "ok.txt",
                    joined,
                    file,
                    metadata,
                    Arc::new(CachedHeaders {
                        content_type: named.content_type(),
                        last_modified: None,
                        etag: Some(etag.clone()),
                        disposition: Some(disposition.clone()),
                    }),
                );
                let Some(Opened {
                    cached_headers: cached,
                    ..
                }) = files.open("ok.txt")
                else {
                    panic!("第二次应当打开成功");
                };
                let Some(cached) = cached else {
                    panic!("第二次应当命中");
                };
                assert_eq!(cached.content_type, named.content_type());
                assert_eq!(cached.etag, Some(etag), "命中时 ETag 也从缓存来");
                assert_eq!(
                    cached.disposition,
                    Some(disposition),
                    "命中时 Content-Disposition 也从缓存来"
                );
                Ok::<(), std::io::Error>(())
            })
    }
}
