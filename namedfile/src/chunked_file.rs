use std::fmt::{self, Debug, Formatter};
use std::fs::File;
use std::io::{Error as IoError, Result as IoResult};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_util::stream::Stream;

/// Internal state machine for [`ChunkedFile`].
enum ChunkedState {
    /// Holding the file, ready to start the next read operation.
    Idle,
    /// Waiting for a blocking read operation to complete.
    Future(tokio::task::JoinHandle<IoResult<Bytes>>),
}

/// A streaming file reader that yields data in configurable chunks.
///
/// `ChunkedFile` implements [`Stream`], yielding
/// [`Bytes`] chunks as the file is read. This allows large files to be served
/// without loading the entire content into memory.
///
/// # How It Works
///
/// 1. Reading is performed in a blocking thread pool via `spawn_blocking`
/// 2. Each read operation yields a chunk of up to `buffer_size` bytes
/// 3. The stream completes when `total_size` bytes have been read
///
/// # Example
///
/// ```ignore
/// use salvo_core::fs::ChunkedFile;
/// use futures_util::StreamExt;
/// use std::fs::File;
///
/// let file = File::open("large_file.bin").unwrap();
/// let metadata = file.metadata().unwrap();
///
/// let mut stream = ChunkedFile::new(file, metadata.len(), 65536);
///
/// while let Some(chunk) = stream.next().await {
///     match chunk {
///         Ok(bytes) => println!("Read {} bytes", bytes.len()),
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// ```
pub struct ChunkedFile {
    total_size: u64,
    read_size: u64,
    buffer_size: u64,
    offset: u64,
    file: Arc<File>,
    state: ChunkedState,
}

impl ChunkedFile {
    /// Creates a reader for `total_size` bytes starting at `offset`.
    pub(crate) fn new(file: Arc<File>, offset: u64, total_size: u64, buffer_size: u64) -> Self {
        Self {
            total_size,
            read_size: 0,
            buffer_size,
            offset,
            file,
            state: ChunkedState::Idle,
        }
    }
}
impl Debug for ChunkedFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkedFile")
            .field("total_size", &self.total_size)
            .field("read_size", &self.read_size)
            .field("buffer_size", &self.buffer_size)
            .field("offset", &self.offset)
            .finish()
    }
}

impl Stream for ChunkedFile {
    type Item = IoResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.total_size == self.read_size {
            return Poll::Ready(None);
        }

        match self.state {
            ChunkedState::Idle => {
                let max_bytes = self
                    .total_size
                    .saturating_sub(self.read_size)
                    .min(self.buffer_size) as usize;
                let offset = self.offset;
                let file = Arc::clone(&self.file);
                let fut = tokio::task::spawn_blocking(move || {
                    let mut buf = vec![0_u8; max_bytes];
                    read_exact_at(&file, &mut buf, offset)?;
                    Ok(Bytes::from(buf))
                });

                self.state = ChunkedState::Future(fut);
                self.poll_next(cx)
            }
            ChunkedState::Future(ref mut fut) => {
                let bytes = ready!(Pin::new(fut).poll(cx))
                    .map_err(|_| IoError::other("`ChunkedFile` block error"))??;
                self.state = ChunkedState::Idle;

                self.offset += bytes.len() as u64;
                self.read_size += bytes.len() as u64;

                Poll::Ready(Some(Ok(bytes)))
            }
        }
    }
}

/// Reads exactly `buf.len()` bytes without changing the shared file offset.
#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> IoResult<()> {
    use std::os::unix::fs::FileExt as _;

    file.read_exact_at(buf, offset)
}

/// Reads exactly `buf.len()` bytes without changing the shared file offset.
#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> IoResult<()> {
    use std::os::windows::fs::FileExt as _;

    let mut done = 0;
    while done < buf.len() {
        let read = file.seek_read(&mut buf[done..], offset + done as u64)?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        done += read;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(_file: &File, _buf: &mut [u8], _offset: u64) -> IoResult<()> {
    Err(IoError::other(
        "positioned file reads are not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use futures_util::StreamExt as _;

    #[tokio::test]
    async fn shared_file_streams_keep_their_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shared.bin");
        let payload: Vec<u8> = (0..100_000_u32).map(|index| (index % 251) as u8).collect();
        std::fs::write(&path, &payload).unwrap();

        let file = Arc::new(File::open(&path).unwrap());
        let mut first = ChunkedFile::new(Arc::clone(&file), 0, 4096, 4096);
        let mut second = ChunkedFile::new(Arc::clone(&file), 8192, 4096, 4096);

        let (first, second) = futures_util::future::join(first.next(), second.next()).await;
        assert_eq!(first.unwrap().unwrap(), &payload[..4096]);
        assert_eq!(second.unwrap().unwrap(), &payload[8192..12_288]);
    }

    #[tokio::test]
    async fn stream_reads_a_file_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream.txt");
        std::fs::write(&path, b"hello world").unwrap();
        let mut stream = ChunkedFile::new(Arc::new(File::open(&path).unwrap()), 0, 11, 5);

        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(out, b"hello world");
    }
}
