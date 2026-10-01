//! `lanfile get` 的拉取客户端：把远端 lanfile 掌管的一棵目录树原样镜像到本地，或拉单个文件，
//! 不打压缩包、不占服务端额外空间。
//!
//! 来源两种：
//! - 裸 host：`http://h [remote] [local]`——`remote` 必须给出；给了名字先试目录，
//!   `/api/manifest` 返回 200 当目录拉，404 再当单个文件拉；
//! - 直链：URL 的路径或 fragment 直接指明远端——`http://h/files/<sub>`、`http://h/pull/<sub>`
//!   当文件，`http://h/api/zip/<sub>`、`http://h/api/list/<sub>`、`http://h/#<sub>` 当目录，
//!   其余非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、文件还是目录交给 manifest 探测。
//!
//! 服务端三个 GET 端点：
//! - `/api/manifest/<dir>` 一次拿到整棵子树的元数据；
//! - `/stream-batch/<dir>` 按 manifest 索引并发接收正文；
//! - `/pull/<sub>` 单个文件直落；
//!
//! 目录统一走清单模式：先一次取整棵树的元数据，再按目录亲和分成 4 个 shard 并发接收正文。
//!
//! 正文严格按响应声明的 `Content-Length` 读满即停：长度不再是事后校验，而是读取本身的停止
//! 条件，读满的连接干净、直接归还池子复用。响应必须带 `Content-Length`
//! （见 `NO_CONTENT_LENGTH`）：缺了当场报错，不猜长度、也不退化成读到 EOF——keep-alive 下
//! 对端不会关连接，那只会在空等之后撞上读取超时。连接与单次读取都设了空闲超时，服务器半路
//! 哑掉不会把客户端挂死；复用的连接若被对端悄悄关掉，下一次请求会换一条新连接重试一次。
//! 正文在 Linux/Android 且目标文件系统支持时走 `splice(2)` 零拷贝落盘，其余平台或文件系统
//! 退回用户态读写，落盘内容与截断判定两边一致。结构上单个文件抓取收口在 [`fetch_file`]，
//! 清单分片流收口在 [`streaming::fetch_stream_shard`]。

mod args;
mod error;
mod fetch;
mod http;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod splice;
mod streaming;

pub use error::{BoxError, Error};

use crate::args::{Kind, Parsed, parse_args};
use crate::fetch::{ManifestEntry, Via, fetch_file, fetch_manifest};
use crate::http::Pool;
use crate::streaming::fetch_stream_shard;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

/// 分片流固定使用 4 条连接。
const STREAM_SHARDS: u32 = 4;
/// 单个目录亲和任务的最大文件数或字节数，超过后拆成连续小任务。
const STREAM_TASK_MAX_FILES: usize = 512;
const STREAM_TASK_MAX_BYTES: u64 = 16 * 1024 * 1024;
/// 清单分片流读短或尺寸变化后，单个文件改走 `/pull` 的最大重试次数。
const FILE_RETRIES: usize = 3;

/// 子命令入口：`lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]`。
///
/// 来源两种：
/// - 裸 host（`http://h <remote> [local] [--flat]`）：`remote` 必给——拉根被禁，会在连服务端前
///   直接报错；给了名字先试目录清单，404 再当单个文件拉。
/// - 直链（URL 的路径/fragment 已指明远端）：`http://h/files/<sub>`、`http://h/pull/<sub>` 当
///   文件；`http://h/api/zip/<sub>`、`http://h/api/list/<sub>`、`http://h/#<sub>` 当目录；
///   其余非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、kind 交给 manifest 探测；
///   不给 `local` 则落进当前目录（文件取末段为名）。
///
/// 落盘语义对齐 `scp -r`：默认拉目录时在 `local` 下套一层以远端目录名命名的子目录
/// （`local/dir/`）；`--flat`/`-f` 不套层，目录内容直接落 `local`（恢复 8f8a234 前的默认）。
/// 拉单个文件时直接落 `local/<basename>`，不套层。不给 `local` 时，命名远端/文件缺省当前目录。
pub async fn run(args: &[String]) -> Result<(), BoxError> {
    let p = parse_args(args)?;
    let pool = Pool::default();
    match p.kind {
        // 目录：一次取元数据，然后并发拉正文。
        Kind::Stream | Kind::Dir => run_manifest(&pool, &p, false).await,
        Kind::File => run_file(&pool, &p).await,
        // 裸 host：与清单直链同路；manifest 404 再当文件。
        Kind::Auto => run_manifest(&pool, &p, true).await,
    }
}

#[derive(Default)]
struct Stats {
    dirs: u64,
    /// 因为本地已有同名同尺寸文件而跳过的文件数。
    ///
    /// 单独记一档，是为了让"本次真正传输多少"和"本地已符合条件跳过多少"不混在一起。
    skipped: u64,
}

/// 落盘文件按正文搬运方式分类的文件数。
///
/// `splice(2)` 走不走得通是平台与文件系统的事，一趟拉取下来通常是同一个结果；这里按文件
/// 数记，是为了让「确实吃上了没有」一眼可见——小文件正文在 `BufReader` 的预读里，
/// 压根没东西可搬，单独记一档，免得跟「不支持」混在一起。
#[derive(Default)]
struct ViaCounts {
    spliced: u64,
    copied: u64,
    prebuffered: u64,
}

impl ViaCounts {
    /// 记一次落盘走的搬运方式。
    const fn record(&mut self, via: Via) {
        match via {
            Via::Splice => self.spliced += 1,
            Via::Copy => self.copied += 1,
            Via::Prebuffered => self.prebuffered += 1,
        }
    }

    const fn from_stream(stats: &crate::streaming::StreamStats) -> Self {
        Self {
            spliced: stats.spliced,
            copied: stats.copied,
            prebuffered: stats.prebuffered,
        }
    }

    /// 拉完在末尾汇报：几个文件吃上了 `splice(2)` 零拷贝、几个没吃上退回用户态读写、几个
    /// 压根不用搬。一个文件都没落盘就不吭声。
    fn report(&self) {
        if self.spliced + self.copied + self.prebuffered == 0 {
            return;
        }
        eprintln!(
            "lanfile get: 正文搬运：{} 个文件经 splice(2) 零拷贝，{} 个文件退回用户态读写，{} 个文件正文未超过预读缓冲",
            self.spliced, self.copied, self.prebuffered
        );
    }
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

/// 清单模式：一次取 manifest，目录先建好，文件按目录亲和分片并发拉。
async fn run_manifest(pool: &Pool, p: &Parsed, fallback_file: bool) -> Result<(), BoxError> {
    let target = local_target(&p.local, &p.remote, p.flat);
    let entries = match fetch_manifest(pool, &p.host, &p.remote).await {
        Ok(entries) => entries,
        Err(Error::Http { status: 404, .. }) if fallback_file => {
            return run_file(pool, p).await;
        }
        Err(error) => return Err(to_not_found(error, &p.remote).into()),
    };
    let (stats, files) = prepare_manifest(&target, &entries).await?;
    let (stream_stats, failures) =
        pull_stream_shards(pool, &p.host, &p.remote, &target, files).await?;
    let via = ViaCounts::from_stream(&stream_stats);
    eprintln!(
        "lanfile get: {}/{} -> {}（清单分片流：{} 文件，{} 目录，{} 字节，跳过已存在 {} 个）",
        p.base,
        p.remote,
        target.display(),
        stream_stats.files,
        stats.dirs,
        stream_stats.bytes,
        stats.skipped,
    );
    via.report();
    eprintln!(
        "lanfile get: 拉取完成：成功 {} 个（含已存在跳过 {} 个），失败 {} 个",
        stats.skipped + stream_stats.files,
        stats.skipped,
        failures.len()
    );
    for path in failures {
        eprintln!("lanfile get: {path} 因为重试次数达到上限无法拉取");
    }
    Ok(())
}

/// 当文件拉 `/pull/<remote>`；404 统一转成「远端不存在」（裸 host 探测到这一步即目录与文件都不是）。
async fn run_file(pool: &Pool, p: &Parsed) -> Result<(), BoxError> {
    pull_file_run(pool, p)
        .await
        .map_err(|error| to_not_found(error, &p.remote).into())
}

/// 拉单个文件到 `local/<basename>`：不套层，落盘根目录按需建。
async fn pull_file_run(pool: &Pool, p: &Parsed) -> Result<(), Error> {
    let remote = &p.remote;
    let name = basename(remote).unwrap_or("download");
    let target = p.local.join(name);
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let fetched = fetch_file(pool, &p.host, remote, &target).await?;
    eprintln!(
        "lanfile get: {}/{remote} -> {}（{} 字节）",
        p.base,
        target.display(),
        fetched.bytes
    );
    let mut via = ViaCounts::default();
    via.record(fetched.via);
    via.report();
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

/// 先按 manifest 建出全部目录（包括空目录），并收集文件大小供 shard 校验。
async fn prepare_manifest(
    local: &Path,
    entries: &[ManifestEntry],
) -> Result<(Stats, HashMap<String, u64>), Error> {
    tokio::fs::create_dir_all(local).await?;
    let mut stats = Stats::default();
    let mut files = HashMap::with_capacity(entries.len() / 2);
    for entry in entries {
        if entry.is_dir() {
            tokio::fs::create_dir_all(local.join(&entry.path)).await?;
            stats.dirs += 1;
        } else if let Some(size) = entry.size {
            if skip_existing(&local.join(&entry.path), Some(size)).await {
                stats.skipped += 1;
            } else {
                files.insert(entry.path.clone(), size);
            }
        } else {
            return Err(Error::Malformed("清单文件缺少大小"));
        }
    }
    Ok((stats, files))
}

/// 4 个 shard 并发接收；每个任务持有 manifest 中自己的文件集合并做精确校验。
async fn pull_stream_shards(
    pool: &Pool,
    host: &str,
    remote: &str,
    local: &Path,
    mut files: HashMap<String, u64>,
) -> Result<(crate::streaming::StreamStats, Vec<String>), Error> {
    let shards = build_shard_tasks(&mut files);

    let mut tasks = Vec::with_capacity(STREAM_SHARDS as usize);
    for expected in shards {
        let host = host.to_string();
        let remote = remote.to_string();
        let local = local.to_path_buf();
        tasks.push(async move { fetch_stream_shard(&host, &remote, &local, expected).await });
    }
    let results = futures_util::future::try_join_all(tasks).await?;
    let mut stats = crate::streaming::StreamStats::default();
    let mut retry_files = std::collections::BTreeSet::new();
    for result in results {
        stats.merge(&result.stats);
        retry_files.extend(result.retry_files);
    }
    let failures = retry_failed_files(
        pool,
        host,
        remote,
        local,
        retry_files.into_iter().collect(),
        &mut stats,
    )
    .await;
    Ok((stats, failures))
}

/// Retry stream failures one file at a time, returning files that still failed.
async fn retry_failed_files(
    pool: &Pool,
    host: &str,
    remote: &str,
    local: &Path,
    files: Vec<String>,
    stats: &mut crate::streaming::StreamStats,
) -> Vec<String> {
    let mut failures = Vec::new();
    for rel in files {
        let mut last_error = None;
        let remote_path = if remote.is_empty() {
            rel.clone()
        } else {
            format!("{remote}/{rel}")
        };
        for attempt in 1..=FILE_RETRIES {
            match fetch_file(pool, host, &remote_path, &local.join(&rel)).await {
                Ok(fetched) => {
                    stats.files += 1;
                    stats.bytes += fetched.bytes;
                    match fetched.via {
                        Via::Splice => stats.spliced += 1,
                        Via::Copy => stats.copied += 1,
                        Via::Prebuffered => stats.prebuffered += 1,
                    }
                    last_error = None;
                    break;
                }
                Err(error) => {
                    eprintln!("lanfile get: {rel} 第 {attempt}/{FILE_RETRIES} 次重试失败：{error}");
                    last_error = Some(error);
                }
            }
        }
        if last_error.is_some() {
            failures.push(rel);
        }
    }
    failures
}

struct ShardTask {
    files: Vec<(String, u64)>,
    bytes: u64,
}

/// 以父目录为任务单位做目录亲和，再按负载贪心分配。
///
/// 大目录拆成连续小块，避免一个目录拖慢单个 shard；大文件无法在当前协议内拆块，
/// 会作为独立任务优先放到最空的 shard。
fn build_shard_tasks(files: &mut HashMap<String, u64>) -> Vec<Vec<(String, u64)>> {
    fn parent(path: &str) -> &str {
        match path.rsplit_once('/') {
            Some((parent, _)) => parent,
            None => "",
        }
    }

    let mut files = Vec::from_iter(files.drain());
    files.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut tasks = Vec::new();
    let mut current = ShardTask {
        files: Vec::new(),
        bytes: 0,
    };
    for file in files {
        let (path, size) = &file;
        if !current.files.is_empty()
            && (current.files.len() >= STREAM_TASK_MAX_FILES
                || current.bytes.saturating_add(*size) > STREAM_TASK_MAX_BYTES
                || parent(&current.files[0].0) != parent(path))
        {
            tasks.push(current);
            current = ShardTask {
                files: Vec::new(),
                bytes: 0,
            }
        }
        current.bytes += *size;
        current.files.push(file);
    }
    if !current.files.is_empty() {
        tasks.push(current);
    }

    // 最长处理时间优先：大任务先落位，小任务填空，减少尾部等待。
    tasks.sort_unstable_by_key(|task| std::cmp::Reverse(task.bytes));
    let mut shards = vec![Vec::new(); STREAM_SHARDS as usize];
    let mut loads = vec![0_u64; STREAM_SHARDS as usize];
    for task in tasks {
        let mut lightest = 0;
        for shard in 1..loads.len() {
            if loads[shard] < loads[lightest] {
                lightest = shard;
            }
        }
        loads[lightest] += task.bytes;
        shards[lightest].extend(task.files);
    }
    for files in &mut shards {
        files.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    }
    shards
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

    #[test]
    fn build_shard_tasks_balances_directory_tasks_without_losing_files() {
        let mut files = HashMap::new();
        for directory in 0..8 {
            for file in 0..4 {
                files.insert(format!("dir{directory}/file{file}"), 10_u64);
            }
        }

        let shards = build_shard_tasks(&mut files);
        let loads = shards
            .iter()
            .map(|files| files.iter().map(|(_, size)| size).sum::<u64>())
            .collect::<Vec<_>>();
        let count = shards.iter().map(Vec::len).sum::<usize>();
        let paths = shards
            .iter()
            .flat_map(|files| files.iter().map(|(path, _)| path.as_str()))
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(count, 32);
        assert_eq!(paths.len(), 32);
        assert_eq!(loads, [80, 80, 80, 80]);
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn retry_failed_files_gives_up_after_three_attempts() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..FILE_RETRIES {
                let (mut conn, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let _ = conn.read(&mut request).await;
                conn.write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            }
        });

        let local =
            std::env::temp_dir().join(format!("lanfile-retry-failed-{}", std::process::id()));
        let mut stats = crate::streaming::StreamStats::default();
        let failures = retry_failed_files(
            &Pool::default(),
            &addr.to_string(),
            "sub",
            &local,
            vec!["bad.txt".to_string()],
            &mut stats,
        )
        .await;

        assert_eq!(failures, ["bad.txt"]);
        assert_eq!(stats.files, 0);
        assert!(!local.join("bad.txt").exists());
        server.await.unwrap();
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
