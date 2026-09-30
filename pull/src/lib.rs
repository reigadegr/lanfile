//! `lanfile get` 的拉取客户端：把远端 lanfile 掌管的一棵目录树原样镜像到本地，或拉单个文件，
//! 不打压缩包、不占服务端额外空间。
//!
//! 来源两种：
//! - 裸 host：`http://h [remote] [local]`——`remote` 缺省拉根；给了名字先试目录，
//!   `/api/list` 返回 200 当目录拉，404 当单个文件拉；
//! - 直链：URL 的路径或 fragment 直接指明远端——`http://h/files/<sub>`、`http://h/pull/<sub>`
//!   当文件，`http://h/api/zip/<sub>`、`http://h/api/list/<sub>` 当目录，
//!   `http://h/api/stream/<sub>` 与 `http://h/#<sub>` 走清单并发模式（旧服务端回退流式），
//!   其余非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、文件还是目录交给 `/api/list` 探测。
//!
//! 服务端三个 GET 端点：
//! - `/api/list/<dir>` 拿到一层目录的条目（name/type/size）；
//! - `/pull/<sub>` 逐个文件落盘（逐个拉取时用）；
//! - `/api/manifest/<sub>` 把整棵子树的元数据一次交给客户端（清单模式用）；
//! - `/stream/<sub>` 把整棵子树流成紧凑格式（旧服务端兼容模式用，一次事务）。
//!
//! 清单模式：先一次取整棵树的元数据，再并发拉正文。目录展开只需一个请求，文件下载
//! 可重叠等待。旧服务端没有 manifest 端点时回退单连接流式。
//!
//! 并发拉取（逐个/清单模式）：文件各起一个任务，用
//! `buffer_unordered(DOWNLOAD_CONCURRENCY)` 限制同时在飞的任务数。目录递归保持串行——
//! 树是流式处理的，先把目录攒起来再并发会让整棵树的展开碎掉、内存上界也失控；同层文件并发
//! 已经能吃满客户端的多核。连接池 [`Pool`] 内部有锁，每个任务各借一条连接，互不影响。
//!
//! 正文严格按响应声明的 `Content-Length` 读满即停：长度不再是事后校验，而是读取本身的停止
//! 条件，读满的连接干净、直接归还池子复用。响应必须带 `Content-Length`
//! （见 `NO_CONTENT_LENGTH`）：缺了当场报错，不猜长度、也不退化成读到 EOF——keep-alive 下
//! 对端不会关连接，那只会在空等之后撞上读取超时。连接与单次读取都设了空闲超时，服务器半路
//! 哑掉不会把客户端挂死；复用的连接若被对端悄悄关掉，下一次请求会换一条新连接重试一次。
//! 正文在 Linux/Android 且目标文件系统支持时走 `splice(2)` 零拷贝落盘，其余平台或文件系统
//! 退回用户态读写，落盘内容与截断判定两边一致。结构上每个文件的抓取收口在 [`fetch_file`]、
//! 目录枚举收口在 [`list_entries`]、整树流式收口在 [`streaming::fetch_stream`]。

mod args;
mod error;
mod fetch;
mod http;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod splice;
mod streaming;

pub use error::{BoxError, Error};

use crate::args::{Kind, Parsed, parse_args};
use crate::fetch::{
    Fetched, ManifestEntry, RemoteEntry, Via, fetch_file, fetch_manifest, list_entries,
};
use crate::http::Pool;
use crate::streaming::fetch_stream_shard;
use futures_util::stream::{self, StreamExt};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

/// 同时在飞的文件任务数（逐个/清单模式）。
///
/// 客户端与服务端都在本机、服务端几乎不占 CPU 时，串行拉取被逐个文件的往返时延卡住；
/// 8 路并发把等待重叠起来，同时不会让服务端的 `FileCache` 分片锁或客户端磁盘写成为瓶颈。
/// 一个文件一个连接，pool 的容量由这个数自然定住。
///
const DOWNLOAD_CONCURRENCY: usize = 8;
/// 分片流固定使用 4 条连接。
const STREAM_SHARDS: u32 = 4;
/// 单个目录亲和任务的最大文件数或字节数，超过后拆成连续小任务。
const STREAM_TASK_MAX_FILES: usize = 512;
const STREAM_TASK_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// 子命令入口：`lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]`。
///
/// 来源两种：
/// - 裸 host（`http://h <remote> [local] [--flat]`）：`remote` 必给——拉根被禁，会在连服务端前
///   直接报错；给了名字则先试目录，`/api/list` 返回 200 当目录拉，404 当单个文件拉。
/// - 直链（URL 的路径/fragment 已指明远端）：`http://h/files/<sub>`、`http://h/pull/<sub>` 当
///   文件；`http://h/api/zip/<sub>`、`http://h/api/list/<sub>` 当目录（逐个拉）；
///   `http://h/api/stream/<sub>` 与 `http://h/#<sub>` 走清单并发（旧服务端回退流式）；
///   其余非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、kind 交给 `/api/list` 探测；
///   不给 `local` 则落进当前目录（文件取末段为名）。
///
/// 落盘语义对齐 `scp -r`：默认拉目录时在 `local` 下套一层以远端目录名命名的子目录
/// （`local/dir/`）；`--flat`/`-f` 不套层，目录内容直接落 `local`（恢复 8f8a234 前的默认）。
/// 拉单个文件时直接落 `local/<basename>`，不套层。不给 `local` 时，命名远端/文件缺省当前目录。
pub async fn run(args: &[String]) -> Result<(), BoxError> {
    let p = parse_args(args)?;
    let pool = Pool::default();
    match p.kind {
        // 清单直链：一次取元数据，然后并发拉正文；旧服务端回退单连接流式。
        Kind::Stream => run_manifest(&pool, &p).await,
        // 直链已指明 kind：文件直接拉、目录当目录拉。
        Kind::File => run_file(&pool, &p).await,
        Kind::Dir => run_dir(&pool, &p, false).await,
        // 裸 host：先试目录，`/api/list` 404 再当文件。
        Kind::Auto => run_dir(&pool, &p, true).await,
    }
}

#[derive(Default)]
struct Stats {
    files: u64,
    dirs: u64,
    bytes: u64,
    /// 因为本地已有同名同尺寸文件而跳过的文件数。
    ///
    /// 单独记一档，是为了让"拉了多少"与"整棵树有多少"不再混在一个数里：
    /// 输出里的 `files` 是走完整棵树看到的文件总数，而它减去 `skipped` 才是这一趟
    /// 真正拉下来的数量。
    skipped: u64,
    /// 正文搬运方式的文件计数，拉完汇报「几个吃上 `splice`」用。
    via: ViaCounts,
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

    /// 把子目录递归上来的计数并进来。
    const fn merge(&mut self, sub: &Self) {
        self.spliced += sub.spliced;
        self.copied += sub.copied;
        self.prebuffered += sub.prebuffered;
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
///
/// 旧服务端没有 manifest 端点时回退到原单连接流式，保持新旧版本可以互通。
async fn run_manifest(pool: &Pool, p: &Parsed) -> Result<(), BoxError> {
    let target = local_target(&p.local, &p.remote, p.flat);
    let entries = match fetch_manifest(pool, &p.host, &p.remote).await {
        Ok(entries) => entries,
        Err(Error::Http { status: 404, .. }) => return run_stream(p).await,
        Err(error) => return Err(error.into()),
    };
    let (mut stats, files) = prepare_manifest(&target, &entries).await?;
    let stream_stats = pull_stream_shards(&p.host, &p.remote, &target, files).await?;
    stats.files = stream_stats.files;
    stats.bytes = stream_stats.bytes;
    stats.via.spliced = stream_stats.spliced;
    stats.via.copied = stream_stats.copied;
    stats.via.prebuffered = stream_stats.prebuffered;
    eprintln!(
        "lanfile get: {}/{} -> {}（清单分片流：{} 文件，{} 目录，{} 字节，跳过已存在 {} 个）",
        p.base,
        p.remote,
        target.display(),
        stats.files,
        stats.dirs,
        stats.bytes,
        stats.skipped,
    );
    stats.via.report();
    Ok(())
}

/// 流式模式：一次 `GET /stream/<remote>` 把整棵树拉下来。
async fn run_stream(p: &Parsed) -> Result<(), BoxError> {
    let target = local_target(&p.local, &p.remote, p.flat);
    let stats = streaming::fetch_stream(&p.host, &p.remote, &target).await?;
    eprintln!(
        "lanfile get: {}/{} -> {}（流式：{} 文件，{} 目录，{} 字节）",
        p.base,
        p.remote,
        target.display(),
        stats.files,
        stats.dirs,
        stats.bytes,
    );
    Ok(())
}

/// 当文件拉 `/pull/<remote>`；404 统一转成「远端不存在」（裸 host 探测到这一步即目录与文件都不是）。
async fn run_file(pool: &Pool, p: &Parsed) -> Result<(), BoxError> {
    pull_file_run(pool, p)
        .await
        .map_err(|error| to_not_found(error, &p.remote).into())
}

/// 当目录拉 `/api/list/<remote>`；404 时按 `fallback_file` 决定下一步：
/// - `true`（裸 host）：改走 `/pull` 试单个文件；
/// - `false`（目录直链）：URL 已经说清楚是目录，直接报"远端不存在"。
async fn run_dir(pool: &Pool, p: &Parsed, fallback_file: bool) -> Result<(), BoxError> {
    match list_entries(pool, &p.host, &p.remote).await {
        Ok(entries) => pull_dir_run(pool, p, entries).await.map_err(Into::into),
        Err(Error::Http { status: 404, .. }) if fallback_file => run_file(pool, p).await,
        Err(error) => Err(to_not_found(error, &p.remote).into()),
    }
}

/// 拉目录到 `local`（默认在 `local` 下套一层远端目录名，对齐 `scp -r`；`--flat` 不套层）。
async fn pull_dir_run(pool: &Pool, p: &Parsed, entries: Vec<RemoteEntry>) -> Result<(), Error> {
    let remote = &p.remote;
    let target = local_target(&p.local, remote, p.flat);
    tokio::fs::create_dir_all(&target).await?;
    let stats = pull_entries(pool, &p.host, remote, &target, entries).await?;
    eprintln!(
        "lanfile get: {}/{remote} -> {}（{} 文件，{} 字节，{} 目录，跳过已存在 {} 个）",
        p.base,
        target.display(),
        stats.files,
        stats.bytes,
        stats.dirs,
        stats.skipped,
    );
    stats.via.report();
    Ok(())
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

/// 递归拉取 `remote` 目录到 `local`：先取这层条目，再逐条落盘。
async fn pull_dir(pool: &Pool, host: &str, remote: &str, local: &Path) -> Result<Stats, Error> {
    let entries = list_entries(pool, host, remote).await?;
    pull_entries(pool, host, remote, local, entries).await
}

/// 一个文件条目处理完后的结果，供并发结果汇总用。
///
/// 拆成三态而不是 `Option<Result<Fetched, Error>>`：跳过和失败是两件不同的事，
/// 后者要打警告，前者要计入 `skipped`——硬塞进一个 `Option` 里还得再判一次。
enum FileOutcome {
    /// 本地已存在且尺寸一致：跳过，不拉。
    Skipped,
    /// 拉下来了。
    Fetched(Fetched),
    /// 拉失败：远端路径与错误。
    Failed(String, Error),
}

/// 把一层条目落到 `local`：目录递归，文件并发抓（[`DOWNLOAD_CONCURRENCY`] 路）。
///
/// 文件先攒成 future 列表、由 `buffer_unordered` 并发驱动、在主线程汇总——同一个目录下
/// 的文件互不依赖，串行只会让各自的往返时延白白累加。目录递归仍串行：整棵树是流式展开的，
/// 并发目录会让一层攒下所有子树的 future，内存不再有上界。
///
/// 单文件失败只记一条警告并继续，与串行版本语义一致。
async fn pull_entries(
    pool: &Pool,
    host: &str,
    remote: &str,
    local: &Path,
    entries: Vec<RemoteEntry>,
) -> Result<Stats, Error> {
    let mut stats = Stats::default();
    let mut file_tasks = Vec::new();
    let mut subdirs = Vec::new();

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
            subdirs.push((remote_child, local_child));
        } else {
            stats.files += 1;
            let remote_size = entry.size;
            // `pool: &Pool` 与 `host: &str` 都是共享引用，每个 async 块各捕获一份；
            // future 只在本函数内被驱动，所以引用不必 `'static`
            file_tasks.push(async move {
                if skip_existing(&local_child, remote_size).await {
                    return FileOutcome::Skipped;
                }
                let result = fetch_file(pool, host, &remote_child, &local_child).await;
                match result {
                    Ok(fetched) => FileOutcome::Fetched(fetched),
                    Err(error) => FileOutcome::Failed(remote_child, error),
                }
            });
        }
    }

    let outcomes = stream::iter(file_tasks)
        .buffer_unordered(DOWNLOAD_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for outcome in outcomes {
        match outcome {
            FileOutcome::Skipped => stats.skipped += 1,
            FileOutcome::Fetched(fetched) => {
                stats.bytes += fetched.bytes;
                stats.via.record(fetched.via);
            }
            FileOutcome::Failed(name, error) => eprintln!("  跳过 {name}：{error}"),
        }
    }

    for (remote_child, local_child) in subdirs {
        // async 递归必须装箱，否则 future 尺寸无限
        let sub = Box::pin(pull_dir(pool, host, &remote_child, &local_child)).await?;
        stats.files += sub.files;
        stats.dirs += sub.dirs;
        stats.bytes += sub.bytes;
        stats.skipped += sub.skipped;
        stats.via.merge(&sub.via);
    }

    Ok(stats)
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
    host: &str,
    remote: &str,
    local: &Path,
    mut files: HashMap<String, u64>,
) -> Result<crate::streaming::StreamStats, Error> {
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
    for result in results {
        stats.merge(&result);
    }
    Ok(stats)
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
    let mut directories: BTreeMap<String, Vec<(String, u64)>> = BTreeMap::new();
    for (path, size) in files.drain() {
        let directory = path.rsplit_once('/').map_or("", |(directory, _)| directory);
        directories
            .entry(directory.to_owned())
            .or_default()
            .push((path, size));
    }

    let mut tasks = Vec::new();
    for (_, mut group) in directories {
        group.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut current = ShardTask {
            files: Vec::new(),
            bytes: 0,
        };
        for file @ (_, size) in group {
            if !current.files.is_empty()
                && (current.files.len() >= STREAM_TASK_MAX_FILES
                    || current.bytes.saturating_add(size) > STREAM_TASK_MAX_BYTES)
            {
                tasks.push(current);
                current = ShardTask {
                    files: Vec::new(),
                    bytes: 0,
                };
            }
            current.bytes += size;
            current.files.push(file);
        }
        if !current.files.is_empty() {
            tasks.push(current);
        }
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

        assert_eq!(count, 32);
        assert_eq!(loads, [80, 80, 80, 80]);
        assert!(files.is_empty());
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
