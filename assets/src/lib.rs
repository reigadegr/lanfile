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
use rustix::fs::{self as rfs, Advice, Mode, OFlags, ResolveFlags};
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
    /// root 的目录 fd：`openat2` 相对它解析路径，越界由内核直接拦下
    #[cfg(any(target_os = "linux", target_os = "android"))]
    root_fd: Option<Arc<OwnedFd>>,
    /// 是否允许调用 `openat2`：装了 seccomp filter 的环境里它不在白名单，调用即被 SIGSYS 杀死
    #[cfg(any(target_os = "linux", target_os = "android"))]
    openat2_allowed: bool,
    /// 已打开文件的缓存：命中时省掉 openat、4 次 readlink 与 fadvise
    #[cfg(any(target_os = "linux", target_os = "android"))]
    cache: FileCache,
}

/// [`ServeFiles::open`] 的返回值：拼好的路径、fd、元数据，以及命中时已经编码好的响应头。
type Opened = (Arc<Path>, Arc<File>, FileMeta, Option<Arc<CachedHeaders>>);

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
    slot: Option<&SendfileSlot>,
    file: &Arc<File>,
) {
    let head_only = req.method() == Method::HEAD;
    if head_only || (slot.is_some() && cfg!(any(target_os = "linux", target_os = "android"))) {
        named_file.send_head(req.headers(), res).await;
    } else {
        named_file.send(req.headers(), res).await;
    }
    if !head_only && let Some(slot) = slot {
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
            .ok()
            .map(Arc::new),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            openat2_allowed: !seccomp_filter_installed(),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            cache: FileCache::default(),
            root,
        }
    }

    /// 打开请求路径对应的文件，且必须位于 root 之内（防目录穿越）。
    ///
    /// 先用 `symlink_metadata` 判断类型，符号链接不会被当作文件服务；这一步同时用来校验
    /// 缓存是否还有效。命中时直接给出缓存里的 fd、它的元数据以及解析好的 `Content-Type`
    /// 与已经编码好的 `ETag`、`Content-Disposition`（第四个元素为 `Some`）。未命中才真正去
    /// 解析路径：让内核用一次 `openat2(RESOLVE_BENEATH)` 同时完成路径解析、越界检查与打开，
    /// 省掉 `canonicalize` 对每一层路径各一次的 `readlink`。装了 seccomp filter 的环境
    /// （Android）根本不调用 `openat2`（调用会被 SIGSYS 杀掉进程，见
    /// [`seccomp_filter_installed`]），旧内核上它会返回错误，两种情况都回退到 canonicalize，
    /// 因此对外行为与改动前一致。
    ///
    /// 校验结果在 `REVALIDATE_MILLIS`（1 秒）内直接复用：这段时间里连上面那次
    /// `symlink_metadata` 都不做，所以文件被改写、替换或删除后，最长 1 秒内仍按上一次校验过的
    /// 元数据与 fd 响应。
    fn open(&self, sub: &str) -> Option<Opened> {
        // 有效期内的快路径：连 `symlink_metadata` 都省掉（本机 1.03 µs，占每请求 CPU 的 3%），
        // 连路径也不必再拼——缓存里存着上次拼好的那一份
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some((joined, file, metadata, headers)) = self.cache.get_fresh(sub) {
            return Some((joined, file, metadata, Some(headers)));
        }
        let joined = self.root.join(sub);
        let metadata = self.regular_metadata(sub, &joined, true)?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some((joined, file, metadata, headers)) = self.cache.get(sub, &metadata) {
            return Some((joined, file, metadata, Some(headers)));
        }
        let (file, metadata) = self.open_confirmed(sub, &joined)?;
        Some((Arc::from(joined), file, metadata, None))
    }

    /// `/pull` 的打开：与 [`Self::open`] 同构，但不查缓存、不写缓存。
    ///
    /// `lanfile get` 每个文件只请求一次，缓存不会有命中，却要为它加一次分片锁、分配一个 key，
    /// 分片满时还得扫一遍 LRU；下载出来的 fd 还会把 `/files` 缓存里的热文件挤出去。这条路上
    /// 整段跳过 [`FileCache`]：先 `lstat` 确认路径仍是普通文件，再用打开后 fd 的 `fstat`
    /// 防止路径在两次检查之间被替换。
    fn open_no_cache(&self, sub: &str) -> Option<(Arc<Path>, Arc<File>, FileMeta)> {
        let joined = self.root.join(sub);
        self.regular_metadata(sub, &joined, false)?;
        let (file, metadata) = self.open_confirmed(sub, &joined)?;
        Some((Arc::from(joined), file, metadata))
    }

    /// Returns path metadata after confirming that the path names a regular file.
    #[cfg_attr(
        not(any(target_os = "linux", target_os = "android")),
        allow(unused_variables)
    )]
    fn regular_metadata(
        &self,
        sub: &str,
        joined: &Path,
        evict_stale: bool,
    ) -> Option<std::fs::Metadata> {
        let Ok(metadata) = std::fs::symlink_metadata(joined) else {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            if evict_stale {
                self.cache.remove(sub);
            }
            return None;
        };
        metadata.is_file().then_some(metadata)
    }

    /// 已确认路径是普通文件之后：打开、取 fd 自己的元数据、下顺序读提示。
    ///
    /// `/files` 未命中缓存时与 `/pull` 全程都走这里，两条路的这一段完全一致。
    fn open_confirmed(&self, sub: &str, joined: &Path) -> Option<(Arc<File>, FileMeta)> {
        let file = self.open_uncached(sub, joined)?;
        // 取这个 fd 自己的元数据：它会随缓存一起给出去，命中时就不必再 fstat 一次。
        // 缓存里必须记 fd 的属性而不是路径的 lstat，否则文件被换掉时会串味。
        let metadata = fd_meta(&file).ok()?;
        // 内核顺序读提示：扩大预读窗口，大文件连续传输更快；仅设置标志、立即返回。
        // 提示作用在 fd 上，缓存命中的那个 fd 早就设过，所以缓存路径上只在未命中时调一次。
        // 一页以内的文件整个读完也只有一页，预读窗口开多大结果都一样，这次系统调用可以省掉。
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if metadata.len() > 4096 {
            let _ = rfs::fadvise(&file, 0, None, Advice::Sequential);
        }
        Some((Arc::new(file), metadata))
    }

    /// 缓存未命中时真正去解析并打开文件（类型检查已由 [`Self::open`] 完成）。
    ///
    /// `sub` 只有 Linux/Android 的 `openat2` 快路径读得到，其他平台上它确实没人用。
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

/// 取已打开 fd 的元数据。
///
/// 不用 `std::fs::File::metadata()`：它在本目标上发的是 `statx(fd, AT_EMPTY_PATH)`
/// （实测 353 ns），而 `fstat` 只要 285 ns，两者给出的 inode、长度与 mtime 完全相同。
///
/// `Stat` 字段的符号性随 rustix 后端而变（`linux_raw` 与 `libc` 不同），这里统一按非负的 stat
/// 字段转换宽度，所以显式关掉这两条 cast 检查。
#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
fn fd_meta(file: &File) -> std::io::Result<FileMeta> {
    let stat = rfs::fstat(file)?;
    Ok(FileMeta::from_raw(
        stat.st_size as u64,
        stat.st_ino as u64,
        stat.st_mtime as i64,
        stat.st_mtime_nsec as i64,
    ))
}

/// 其他平台没有直接发 `fstat` 的分支，退回 `std` 的元数据，字段值一致。
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn fd_meta(file: &File) -> std::io::Result<FileMeta> {
    file.metadata()
        .map(|metadata| FileMeta::from_metadata(&metadata))
}

/// 一次 `openat2` 完成路径解析、越界检查与打开。
///
/// `RESOLVE_BENEATH` 要求解析结果不得越出 `root_fd`，`O_NOFOLLOW` 保证末级不是符号链接。
/// 任何失败都返回 `None`，交给调用方回退到 canonicalize。
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

/// `openat2` 不可用时的回退路径：先 canonicalize 再打开，与改动前的行为一致。
fn open_via_canonicalize(root: &Path, joined: &Path) -> Option<File> {
    let canonical = std::fs::canonicalize(joined).ok()?;
    if !canonical.starts_with(root) {
        return None;
    }
    File::open(canonical).ok()
}

/// 判断当前进程是否装了 seccomp filter（`SECCOMP_MODE_FILTER`）。
///
/// Android 的 `untrusted_app` 域由 zygote 装一个系统调用白名单 filter，不在白名单里的调用
/// 会被 `SECCOMP_RET_TRAP` 处理：内核直接发 SIGSYS 杀掉进程，而不是返回错误码——手机上实测
/// `openat2` 就是这样（`si_code=1` 即 `SYS_SECCOMP`，进程立即终止），"失败就回退"来不及生效。
/// 读不到状态时按"装了"处理：猜错的代价是进程被杀。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn seccomp_filter_installed() -> bool {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return true;
    };
    has_seccomp_filter(&status)
}

/// `/proc/self/status` 里 `Seccomp:` 为 2 即 `SECCOMP_MODE_FILTER`
///
/// 自己用 `memchr` 扫换行，不走 `lines()`：状态文件约 800 字节，这一遍扫描是整函数的主体，
/// release 下 `memchr` 的 SIMD 比逐行迭代快约 1.3×（基准见 `bench_has_seccomp_filter`）。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn has_seccomp_filter(status: &str) -> bool {
    // 内核生成的状态文件全是 ASCII，`trim_ascii` 与 `trim` 在这里等价
    let is_filter = |line: &[u8]| {
        line.strip_prefix(b"Seccomp:")
            .is_some_and(|value| value.trim_ascii() == b"2")
    };
    let mut rest = status.as_bytes();
    loop {
        match memchr::memchr(b'\n', rest) {
            Some(at) => {
                if is_filter(&rest[..at]) {
                    return true;
                }
                rest = &rest[at + 1..];
            }
            // 末行没有换行符
            None => return is_filter(rest),
        }
    }
}

impl ServeFiles {
    /// `/files` 的实际实现，`sub` 是已经解码好的子路径。
    ///
    /// salvo 的 handler 与 hyper 快路径共用这一个入口：两条路唯一的差别是错误页由谁补
    /// （salvo 侧是 catcher，快路径自己渲染），响应本身完全一致。找不到时只设状态码。
    pub async fn serve(
        &self,
        sub: &str,
        req: &Request,
        res: &mut Response,
        slot: Option<&SendfileSlot>,
    ) {
        // 路径解析直接在 worker 上做：只有 lstat + openat2，命中页缓存时是微秒级，
        // 而 spawn_blocking 的线程交接本身就要几十微秒，还得分摊 blocking pool 的全局锁。
        // 用阻塞线程池反而更慢：压测显示这一次 spawn_blocking 就占掉每请求约 7 次 futex 等待
        let Some((path, file, metadata, cached)) = self.open(sub) else {
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
        slot: Option<&SendfileSlot>,
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

#[handler]
impl ServeFiles {
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn handle(&self, req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        // 方法判断从路由过滤器挪到这里：salvo 的过滤器是 `#[async_trait]`，挂在路由上的
        // 每个过滤器每请求都要装箱一个 future 并动态分发一次（实测 `Or<Method, Method>`
        // 每请求两次分配），而在 handler 里只是一次比较。
        // 语义不变：非 GET/HEAD 依旧是 404，空响应体交给 catcher 补错误页
        if req.method() != Method::GET && req.method() != Method::HEAD {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        }

        // 直接从路由参数里借一个 &str：`param::<String>` 会为每个请求分配一个 String，
        // 再走一遍 serde 反序列化；通配参数就在这里，借出来就够了
        let sub = req.params().get("path").map_or("", String::as_str);
        self.serve(sub, req, res, None).await;
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

    /// 临时 root：`root/ok.txt`、`root/sub/deep.txt`，以及 root 之外的一个文件用于穿越测试
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
            let outside = base.join("outside.txt");
            std::fs::write(&outside, b"secret")?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&outside, root.join("outside-link.txt"))?;
            Ok(Self { base, root })
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.base);
        }
    }

    /// `openat2` 是快路径，结论必须和改动前的逻辑（lstat 判类型 + canonicalize 判越界）逐条一致
    #[test]
    fn open_keeps_previous_behaviour() -> std::io::Result<()> {
        let fixture = Fixture::new("parity")?;
        let files = ServeFiles::new(fixture.root.clone());
        for sub in [
            "ok.txt",
            "sub/deep.txt",
            "missing.txt",
            "sub",
            "outside-link.txt",
            "../outside.txt",
        ] {
            let joined = fixture.root.join(sub);
            let is_file = std::fs::symlink_metadata(&joined).is_ok_and(|meta| meta.is_file());
            let expected = is_file && open_via_canonicalize(&fixture.root, &joined).is_some();
            assert_eq!(
                files.open(sub).is_some(),
                expected,
                "{sub} 的结论必须与改动前一致"
            );
        }
        // 目录、符号链接、越界路径都必须拒绝
        assert!(files.open("sub").is_none(), "目录不是文件");
        assert!(files.open("outside-link.txt").is_none(), "符号链接不服务");
        assert!(files.open("../outside.txt").is_none(), "不得穿越出 root");
        // 回退路径本身必须可用：手机上 openat2 可能被 SELinux/seccomp 拦下
        assert!(
            open_via_canonicalize(&fixture.root, &fixture.root.join("ok.txt")).is_some(),
            "回退路径必须能打开普通文件"
        );
        Ok(())
    }

    /// 手机上实测的 /proc/self/status 片段：`Seccomp:` 为 2 表示装了 filter，必须放弃 openat2
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn detects_seccomp_filter() {
        assert!(has_seccomp_filter(
            "Name:\tlanfile\nSeccomp:\t2\nSeccomp_filters:\t1\n"
        ));
        assert!(!has_seccomp_filter("Name:\tlanfile\nSeccomp:\t0\n"));
        // 只有 `Seccomp:` 字段本身算数，`Seccomp_filters:` 不算
        assert!(!has_seccomp_filter("Seccomp_filters:\t1\n"));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn openat2_handles_ordinary_paths() -> std::io::Result<()> {
        let fixture = Fixture::new("fast")?;
        let files = ServeFiles::new(fixture.root.clone());
        // seccomp 环境里 openat2 不在白名单，硬调会被 SIGSYS 杀掉：这时快路径本就不会走，
        // 直接跳过这些断言，回退路径的正确性由 open_keeps_previous_behaviour 覆盖
        if !files.openat2_allowed {
            return Ok(());
        }
        let Some(fd) = files.root_fd.as_ref() else {
            panic!("root 目录 fd 应当打开成功");
        };
        assert!(
            open_beneath(fd, "ok.txt").is_some(),
            "快路径应当能打开普通文件"
        );
        assert!(
            open_beneath(fd, "sub/deep.txt").is_some(),
            "快路径应当能打开子目录里的文件"
        );
        assert!(
            open_beneath(fd, "../outside.txt").is_none(),
            "快路径必须拦下目录穿越"
        );
        assert!(
            open_beneath(fd, "outside-link.txt").is_none(),
            "O_NOFOLLOW 必须拦下符号链接"
        );
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
                let Some((joined, file, metadata, cached)) = files.open("ok.txt") else {
                    panic!("第一次应当打开成功");
                };
                assert!(cached.is_none(), "第一次不该命中");
                let named = NamedFile::builder(fixture.root.join("ok.txt"))
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
                let Some((_, _, _, cached)) = files.open("ok.txt") else {
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

    /// 基准：`memchr` 扫换行 vs `lines()`，输入尺寸对齐真实的 `/proc/self/status`。
    ///
    /// `cargo test` 默认跑在 `opt-level = 0`：std 是预编译的优化产物而 `memchr` 不是，
    /// 那种 profile 下这一项偏向 `lines()`；要看真实差距得加 `--release`。
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    #[ignore = "微基准，需 cargo test --release -- --ignored 显式运行"]
    fn bench_has_seccomp_filter() {
        use std::hint::black_box;
        use std::time::Instant;

        fn time(iters: u32, f: impl Fn() -> bool) -> f64 {
            for _ in 0..iters / 10 {
                black_box(f());
            }
            let start = Instant::now();
            for _ in 0..iters {
                black_box(f());
            }
            start.elapsed().as_secs_f64() * 1e9 / f64::from(iters)
        }

        // `Seccomp:` 放在中后段，整份尺寸与真实状态文件同量级
        let mut status = "Name:\tlanfile\nState:\tS (sleeping)\n".repeat(10);
        status.push_str("Seccomp:\t2\n");
        status.push_str(&"Tgid:\t1234\n".repeat(40));

        let old = |status: &str| {
            status.lines().any(|line| {
                line.strip_prefix("Seccomp:")
                    .is_some_and(|value| value.trim() == "2")
            })
        };
        assert!(old(&status), "原实现应当认出 filter");
        assert!(has_seccomp_filter(&status), "memchr 版应当认出 filter");

        let lines_ns = time(20_000, || old(&status));
        let memchr_ns = time(20_000, || has_seccomp_filter(&status));
        println!(
            "基准 has_seccomp_filter（{}B）: lines() {lines_ns:.1} ns vs memchr {memchr_ns:.1} ns",
            status.len()
        );
        // 只卡数量级：未优化的测试 profile 抖动大，这里不追求证明「更快」
        assert!(
            memchr_ns < lines_ns * 10.0,
            "memchr 版比原实现慢了一个数量级: {memchr_ns:.1} vs {lines_ns:.1} ns"
        );
    }
}
