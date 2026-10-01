use std::{
    fmt::Write as _,
    io::{IsTerminal, Write},
    sync::{Arc, Mutex},
    time::Instant,
};

const BAR_WIDTH: usize = 24;
const REDRAW_INTERVAL_MS: u128 = 100;

#[derive(Clone)]
pub struct SharedProgress {
    inner: Arc<Mutex<Progress>>,
}

struct Progress {
    enabled: bool,
    label: &'static str,
    unit: &'static str,
    total_files: u64,
    total_bytes: u64,
    done_files: u64,
    done_bytes: u64,
    last_draw: Instant,
}

impl SharedProgress {
    pub(crate) fn new(
        label: &'static str,
        unit: &'static str,
        total_files: u64,
        total_bytes: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Progress {
                enabled: std::io::stderr().is_terminal() && (total_files > 0 || total_bytes > 0),
                label,
                unit,
                total_files,
                total_bytes,
                done_files: 0,
                done_bytes: 0,
                last_draw: Instant::now(),
            })),
        }
    }

    #[cfg(test)]
    pub(crate) fn hidden() -> Self {
        Self::new("拉取文件", "个文件", 0, 0)
    }

    pub(crate) fn complete_existing(&self, files: u64, bytes: u64) {
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_files += files;
            progress.done_bytes += bytes;
            progress.draw(true);
        }
    }

    pub(crate) fn add_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_bytes += bytes;
            progress.draw(false);
        }
    }

    pub(crate) fn subtract_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_bytes = progress.done_bytes.saturating_sub(bytes);
        }
    }

    pub(crate) fn finish_file(&self, bytes: u64) {
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_files += 1;
            progress.done_bytes += bytes;
            progress.done_bytes = progress.done_bytes.min(progress.total_bytes);
            progress.draw(false);
        }
    }

    pub(crate) fn finish_stream_file(&self) {
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_files += 1;
            progress.draw(false);
        }
    }

    pub(crate) fn add_dir(&self) {
        if let Ok(mut progress) = self.inner.lock() {
            progress.done_files += 1;
            progress.draw(false);
        }
    }

    pub(crate) fn finish(&self) {
        if let Ok(mut progress) = self.inner.lock() {
            progress.draw(true);
            if progress.enabled {
                let _ = writeln!(std::io::stderr());
            }
        }
    }
}

impl Progress {
    fn draw(&mut self, force: bool) {
        if !self.enabled
            || (!force
                && self.done_files < self.total_files
                && self.last_draw.elapsed().as_millis() < REDRAW_INTERVAL_MS)
        {
            return;
        }
        self.last_draw = Instant::now();

        let progress = if self.total_bytes == 0 {
            self.done_files
        } else {
            self.done_bytes
        };
        let total = if self.total_bytes == 0 {
            self.total_files
        } else {
            self.total_bytes
        };
        let filled = filled_units(progress, total);
        let mut line = String::with_capacity(128);
        line.push_str("\r\x1b[Klanfile get: ");
        line.push_str(self.label);
        line.push_str(" [");
        for at in 0..BAR_WIDTH {
            line.push(if at < filled { '#' } else { '-' });
        }
        let _ = write!(
            line,
            "] {}%/{} {}，{}/{}",
            self.done_files,
            self.total_files,
            self.unit,
            format_bytes(self.done_bytes),
            format_bytes(self.total_bytes),
        );
        let _ = write!(std::io::stderr(), "{line}");
        let _ = std::io::stderr().flush();
    }
}

fn filled_units(done: u64, total: u64) -> usize {
    if total == 0 {
        return BAR_WIDTH;
    }
    let done = u128::from(done.min(total));
    let total = u128::from(total);
    usize::try_from(done * BAR_WIDTH as u128 / total).unwrap_or(BAR_WIDTH)
}

fn format_bytes(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / UNIT as f64;
    let mut suffix = "KiB";
    for next in ["MiB", "GiB", "TiB"] {
        if value < UNIT as f64 {
            break;
        }
        value /= UNIT as f64;
        suffix = next;
    }
    format!("{value:.1} {suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_bytes_uses_binary_units() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
    }
}
