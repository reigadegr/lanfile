//! 决定文件正文能不能交给 `sendfile(2)`。
//!
//! `sendfile` 把「起始偏移」直接交给内核。在 FUSE 类挂载上（Android `/sdcard` 的
//! scoped-storage FUSE 层是最典型的一个），以某些大偏移起步时它可能既不返回错误、
//! 也不推进：响应头用 `MSG_MORE` 等着和正文合并发送，于是客户端连响应头都收不到，
//! 最后被重置。续传的 `Range` 起点恰好落在上一次断掉的偏移上，重试就变成了死循环。
//!
//! 这类根目录下的正文改由 `NamedFile` 的 `pread` 流式响应体发送，正确性优先于零拷贝。

use std::{
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

/// 强制关闭 sendfile 的环境变量，用于自动探测覆盖不到的文件系统。
/// 设为 `0` 或 `false` 相当于不设置。
pub const FORCE_OFF_ENV: &str = "LANFILE_NO_SENDFILE";

static ENABLED: AtomicBool = AtomicBool::new(true);

/// 当前是否允许使用 `sendfile(2)`。
#[must_use]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// 运行时发现 `sendfile` 不被支持后关掉它。
///
/// 当前连接按原来的方式结束，之后的连接会走用户态正文，不会反复卡在同一个系统调用上。
pub fn disable() {
    ENABLED.store(false, Ordering::Relaxed);
}

/// 这个请求是否该走零拷贝正文。
///
/// `sendfile` 只负责从文件开头开始的完整响应：带 `Range` 起点的续传一律交给
/// `NamedFile` 的 `pread` 流式体，避免以大偏移作为 `sendfile` 的起始偏移。
/// 调用方在返回 `false` 时必须让 `NamedFile` 自己写出正文，不能只写响应头。
#[must_use]
pub fn should_upgrade(has_range: bool) -> bool {
    !has_range && cfg!(any(target_os = "linux", target_os = "android")) && enabled()
}

/// 启动时按共享根目录所在的文件系统配置策略。
///
/// 返回关闭原因交给调用方打日志；`None` 表示保持零拷贝。`statfs` 探测失败时不改变
/// 现状，运行时若真报「不支持」会由 [`disable`] 兜底。
#[must_use]
pub fn configure(root: &Path) -> Option<&'static str> {
    let decision = decision_for(root, std::env::var(FORCE_OFF_ENV).ok().as_deref());
    if decision.is_some() {
        disable();
    }
    decision
}

/// 纯判定，方便在不触碰全局开关的情况下测试。
fn decision_for(root: &Path, forced: Option<&str>) -> Option<&'static str> {
    if forced.is_some_and(|value| !matches!(value, "" | "0" | "false")) {
        return Some(FORCE_OFF_ENV);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Ok(stat) = rustix::fs::statfs(root)
        && is_fuse_magic(u64::try_from(stat.f_type).unwrap_or(0))
    {
        return Some("FUSE 文件系统");
    }
    let _ = root;
    None
}

/// `statfs` 的 `f_type` 是不是 FUSE 用户态文件系统的魔数。
#[cfg(any(target_os = "linux", target_os = "android"))]
const fn is_fuse_magic(fs_type: u64) -> bool {
    const FUSE_SUPER_MAGIC: u64 = 0x6573_5546;
    fs_type == FUSE_SUPER_MAGIC
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{FORCE_OFF_ENV, decision_for, should_upgrade};

    #[test]
    fn range_requests_never_use_sendfile() {
        // 与全局开关无关：带 Range 起点的响应必须始终走用户态正文
        assert!(!should_upgrade(true));
    }

    #[test]
    fn plain_requests_follow_the_global_switch() {
        assert_eq!(should_upgrade(false), super::enabled());
    }

    #[test]
    fn force_off_env_decides_without_touching_the_switch() {
        assert_eq!(decision_for(Path::new("/"), Some("1")), Some(FORCE_OFF_ENV));
        assert_eq!(decision_for(Path::new("/"), Some("false")), None);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn only_fuse_magic_disables_sendfile() {
        assert!(super::is_fuse_magic(0x6573_5546));
        assert!(!super::is_fuse_magic(0x0000_EF53)); // ext4
    }
}
