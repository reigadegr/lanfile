//! `lanfile get` 的拉取客户端：把远端 lanfile 掌管的一棵目录树原样镜像到本地，或拉单个文件，
//! 不打压缩包、不占服务端额外空间。
//!
//! 来源两种：
//! - 裸 host：`http://h [remote] [local]`——`remote` 缺省拉根；给了名字先试目录，
//!   `/api/list` 返回 200 当目录拉，404 当单个文件拉；
//! - 直链：URL 的路径或 fragment 直接指明远端——`http://h/files/<sub>`、`http://h/pull/<sub>`
//!   当文件，`http://h/api/zip/<sub>`、`http://h/api/list/<sub>`、`http://h/#<sub>` 当目录，
//!   其余非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、文件还是目录交给 `/api/list` 探测。
//!
//! 只走服务端两个 GET 端点：
//! - `/api/list/<dir>` 拿到一层目录的条目（name/type/size）；
//! - `/pull/<sub>` 逐个文件落盘。`/pull` 是拉取专用的端点：不碰 `/files` 那套 fd 缓存，
//!   也不编码拉取端用不到的 `ETag`、`Last-Modified` 与 `Content-Disposition`（见 `lanfile_assets`）。
//!
//! v1 顺序拉取：一个文件一个文件，但共用一条 keep-alive 连接——一棵目录树只握一次手，
//! 省掉每个文件的三次握手与慢启动。正文严格按响应声明的 `Content-Length` 读满即停：长度
//! 不再是事后校验，而是读取本身的停止条件，读满的连接干净、直接归还池子复用。响应必须带
//! `Content-Length`（见 `NO_CONTENT_LENGTH`）：缺了当场报错，不猜长度、也不退化成读到
//! EOF——keep-alive 下对端不会关连接，那只会在空等之后撞上读取超时。连接与单次读取都设了
//! 空闲超时，服务器半路哑掉不会把客户端挂死；复用的连接若被对端悄悄关掉，下一次请求会换
//! 一条新连接重试一次。结构上每个文件的抓取收口在 [`fetch_file`]、目录枚举收口在
//! [`list_entries`]，未来要做有限并发时把它们解耦、对文件任务套一层 `buffer_unordered`
//! 即可，不必重写本模块。

mod args;
mod error;
mod fetch;
mod http;

pub use error::{BoxError, Error};

use crate::args::{Kind, Parsed, parse_args};
use crate::fetch::{RemoteEntry, fetch_file, list_entries};
use crate::http::Pool;
use std::path::{Path, PathBuf};

/// 子命令入口：`lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]`。
///
/// 两种来源：
/// - 裸 host（`http://h <remote> [local] [--flat]`）：`remote` 必给——拉根被禁，会在连服务端前
///   直接报错；给了名字则先试目录，`/api/list` 返回 200 当目录拉，404 当单个文件拉（对
///   `lanfile get http://h a.tgz` 不再因 `/api/list` 404 直接失败，而是改走 `/pull` 把文件拉下来）。
/// - 直链（URL 的路径/fragment 已指明远端）：`http://h/files/<sub>`、`http://h/pull/<sub>` 当
///   文件，`http://h/api/zip/<sub>`、`http://h/api/list/<sub>`、`http://h/#<sub>` 当目录，其余
///   非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、kind 交给 `/api/list` 探测；不给
///   `local` 则落进当前目录（文件取末段为名）。
///
/// 落盘语义对齐 `scp -r`：默认拉目录时在 `local` 下套一层以远端目录名命名的子目录
/// （`local/dir/`）；`--flat`/`-f` 不套层，目录内容直接落 `local`（恢复 8f8a234 前的默认）。
/// 拉单个文件时直接落 `local/<basename>`，不套层。不给 `local` 时，命名远端/文件缺省当前目录。
pub async fn run(args: &[String]) -> Result<(), BoxError> {
    let p = parse_args(args)?;
    let mut pool = Pool::default();
    match p.kind {
        // 直链已指明 kind：文件直接拉、目录当目录拉。
        Kind::File => run_file(&mut pool, &p).await,
        Kind::Dir => run_dir(&mut pool, &p, false).await,
        // 裸 host：先试目录，`/api/list` 404 再当文件。
        Kind::Auto => run_dir(&mut pool, &p, true).await,
    }
}

#[derive(Default)]
struct Stats {
    files: u64,
    dirs: u64,
    bytes: u64,
}

/// 把 404 转成"远端不存在"；其他错误原样返回。
///
/// `/api/list` 与 `/pull` 都用 404 表示"这条远端路径不存在"，目录探测与单文件拉取两处
/// 都需要做同一个转换，所以收在这里。
fn to_not_found(error: Error, remote: &str) -> Error {
    match error {
        Error::Http { status: 404, .. } => Error::NotFound {
            remote: remote.to_string(),
        },
        other => other,
    }
}

/// 当文件拉 `/pull/<remote>`；404 统一转成「远端不存在」（裸 host 探测到这一步即目录与文件都不是）。
async fn run_file(pool: &mut Pool, p: &Parsed) -> Result<(), BoxError> {
    pull_file_run(pool, p)
        .await
        .map_err(|error| to_not_found(error, &p.remote).into())
}

/// 当目录拉 `/api/list/<remote>`；404 时按 `fallback_file` 决定下一步：
/// - `true`（裸 host）：改走 `/pull` 试单个文件；
/// - `false`（目录直链）：URL 已经说清楚是目录，直接报"远端不存在"。
async fn run_dir(pool: &mut Pool, p: &Parsed, fallback_file: bool) -> Result<(), BoxError> {
    match list_entries(pool, &p.host, &p.remote).await {
        Ok(entries) => pull_dir_run(pool, p, entries).await.map_err(Into::into),
        Err(Error::Http { status: 404, .. }) if fallback_file => run_file(pool, p).await,
        Err(error) => Err(to_not_found(error, &p.remote).into()),
    }
}

/// 拉目录到 `local`（默认在 `local` 下套一层远端目录名，对齐 `scp -r`；`--flat` 不套层）。
async fn pull_dir_run(pool: &mut Pool, p: &Parsed, entries: Vec<RemoteEntry>) -> Result<(), Error> {
    let remote = &p.remote;
    let target = local_target(&p.local, remote, p.flat);
    tokio::fs::create_dir_all(&target).await?;
    let stats = pull_entries(pool, &p.host, remote, &target, entries).await?;
    eprintln!(
        "lanfile get: {}/{remote} -> {}（{} 文件，{} 字节，{} 目录）",
        p.base,
        target.display(),
        stats.files,
        stats.bytes,
        stats.dirs
    );
    Ok(())
}

/// 拉单个文件到 `local/<basename>`：不套层，落盘根目录按需建。
async fn pull_file_run(pool: &mut Pool, p: &Parsed) -> Result<(), Error> {
    let remote = &p.remote;
    let name = basename(remote).unwrap_or("download");
    let target = p.local.join(name);
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let bytes = fetch_file(pool, &p.host, remote, &target).await?;
    eprintln!(
        "lanfile get: {}/{remote} -> {}（{bytes} 字节）",
        p.base,
        target.display()
    );
    Ok(())
}

/// 实际落盘根目录：默认在 `local` 下套一层以远端目录名命名的子目录（对齐
/// `scp -r host:dir local` 落成 `local/dir/` 的语义）；`flat` 为真或拉 root（无名字可套）
/// 时直接用 `local`。
fn local_target(local: &Path, remote: &str, flat: bool) -> PathBuf {
    if !flat && let Some(name) = basename(remote) {
        return local.join(name);
    }
    local.to_path_buf()
}

/// 远端路径的末段目录名；root（去首尾斜杠后为空）返回 `None`。
///
/// 用 `memrchr` 从尾部找最后一个 `/`，省掉 `trim_matches` + `rsplit` 两层迭代器
/// （基准见 `bench_basename`）。
fn basename(remote: &str) -> Option<&str> {
    // 先跳过尾部的 `/`，等价于 `trim_matches('/')` 的右侧
    let end = remote.as_bytes().iter().rposition(|b| *b != b'/')? + 1;
    let head = &remote[..end];
    Some(match memchr::memrchr(b'/', head.as_bytes()) {
        Some(at) => &head[at + 1..],
        None => head,
    })
}

/// 递归拉取 `remote` 目录到 `local`：先取这层条目，再逐条落盘。
async fn pull_dir(pool: &mut Pool, host: &str, remote: &str, local: &Path) -> Result<Stats, Error> {
    let entries = list_entries(pool, host, remote).await?;
    pull_entries(pool, host, remote, local, entries).await
}

/// 把一层条目落到 `local`：目录递归，文件逐个抓。单文件失败只记一条警告并继续。
///
/// 与 [`list_entries`] 拆开是为了让顶层那一次列表请求的失败（404）能被 [`run_dir`] 捕获、
/// 转成"远端不存在"，而不是在这里被当成"递归里某层目录没了"。
async fn pull_entries(
    pool: &mut Pool,
    host: &str,
    remote: &str,
    local: &Path,
    entries: Vec<RemoteEntry>,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();
    for entry in entries {
        let remote_child = if remote.is_empty() {
            entry.name.clone()
        } else {
            format!("{remote}/{}", entry.name)
        };
        let local_child = local.join(&entry.name);
        if entry.is_dir() {
            tokio::fs::create_dir_all(&local_child).await?;
            stats.dirs += 1;
            // async 递归必须装箱，否则 future 尺寸无限
            let sub = Box::pin(pull_dir(pool, host, &remote_child, &local_child)).await?;
            stats.files += sub.files;
            stats.dirs += sub.dirs;
            stats.bytes += sub.bytes;
        } else {
            let remote_size = entry.size;
            if !skip_existing(&local_child, remote_size).await {
                match fetch_file(pool, host, &remote_child, &local_child).await {
                    Ok(n) => stats.bytes += n,
                    Err(error) => eprintln!("  跳过 {remote_child}：{error}"),
                }
            }
            stats.files += 1;
        }
    }
    Ok(stats)
}

/// 本地已存在且尺寸与远端一致就跳过（尺寸级幂等，避免重复落盘）。
async fn skip_existing(path: &Path, remote_size: Option<u64>) -> bool {
    let Some(remote) = remote_size else {
        return false;
    };
    matches!(tokio::fs::metadata(path).await, Ok(m) if m.len() == remote)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// 跑 `iters` 次取平均纳秒，先热身 `iters/10` 次。
    fn time<R>(iters: u32, f: impl Fn() -> R) -> f64 {
        use std::hint::black_box;
        use std::time::Instant;

        for _ in 0..iters / 10 {
            black_box(f());
        }
        let start = Instant::now();
        for _ in 0..iters {
            black_box(f());
        }
        start.elapsed().as_secs_f64() * 1e9 / f64::from(iters)
    }

    #[test]
    fn local_target_wraps_named_remote_in_basename_layer() {
        assert_eq!(
            local_target(Path::new("./dst"), "sub", false),
            PathBuf::from("./dst/sub")
        );
        assert_eq!(
            local_target(Path::new("./dst"), "a/b", false),
            PathBuf::from("./dst/b")
        );
        assert_eq!(
            local_target(Path::new("./dst"), "/sub/", false),
            PathBuf::from("./dst/sub")
        );
    }

    #[test]
    fn local_target_root_has_no_wrap() {
        assert_eq!(
            local_target(Path::new("./dst"), "", false),
            PathBuf::from("./dst")
        );
        assert_eq!(
            local_target(Path::new("./dst"), "/", false),
            PathBuf::from("./dst")
        );
    }

    #[test]
    fn local_target_flat_drops_basename_layer() {
        // --flat：不套 basename 一层，直接落 local；根无名字可套，flat 是 no-op。
        assert_eq!(
            local_target(Path::new("./dst"), "sub", true),
            PathBuf::from("./dst")
        );
        assert_eq!(
            local_target(Path::new("./dst"), "a/b", true),
            PathBuf::from("./dst")
        );
        assert_eq!(
            local_target(Path::new("./dst"), "", true),
            PathBuf::from("./dst")
        );
    }

    /// 基准：`memrchr` 找末段 vs `trim_matches` + `rsplit`
    #[test]
    #[ignore = "微基准，需 cargo test --release -- --ignored 显式运行"]
    fn bench_basename() {
        for remote in ["sub", "a/b", "sub/deeper/more/leaf", "sub/deeper/", "///"] {
            // 原实现：去首尾斜杠后为空即 `None`（`///` 走的就是这一支）
            let old = || {
                let trimmed = remote.trim_matches('/');
                if trimmed.is_empty() {
                    None
                } else {
                    trimmed.rsplit('/').next()
                }
            };
            assert_eq!(old(), basename(remote), "{remote} 取值不一致");

            let old_ns = time(500_000, old);
            let memchr_ns = time(500_000, || basename(remote));
            println!(
                "基准 basename（{}B）: rsplit {old_ns:.1} ns vs memrchr {memchr_ns:.1} ns",
                remote.len()
            );
            // 只卡数量级：未优化的测试 profile 抖动大，这里不追求证明「更快」
            assert!(
                memchr_ns < old_ns * 10.0,
                "memchr 版比原实现慢了一个数量级: {memchr_ns:.1} vs {old_ns:.1} ns"
            );
        }
    }
}
