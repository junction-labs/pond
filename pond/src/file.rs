use std::{io::SeekFrom, pin::Pin};

use futures::{FutureExt, future::BoxFuture};
use pond_core::FileAttr;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::Volume;

/// A File handle for a file within Pond. Not thread-safe.
pub struct File {
    fd: pond_core::Fd,
    attr: FileAttr,
    offset: u64,
    handle: Volume,
    read_fut: Option<BoxFuture<'static, pond_core::Result<bytes::Bytes>>>,
    write_fut: Option<BoxFuture<'static, pond_core::Result<usize>>>,
}

impl File {
    pub(crate) fn new(fd: pond_core::Fd, attr: FileAttr, handle: Volume) -> Self {
        Self {
            fd,
            attr,
            offset: 0,
            handle,
            read_fut: None,
            write_fut: None,
        }
    }
}

/// Close the Fd attached to this file.
impl Drop for File {
    fn drop(&mut self) {
        // attempt to release the handle synchronously first, and if that fails then fire off an
        // tokio task that'll do asynchronously.
        match self.handle.try_release_fd(self.fd) {
            Ok(()) => (),
            Err(fd) => {
                tokio::spawn(self.handle.release_fd(fd).expect(
                    "BUG: release_fd does not do any input validation before returning the future",
                ));
            }
        }
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.read_fut.as_mut() {
            Some(fut) => match fut.poll_unpin(cx) {
                std::task::Poll::Ready(Ok(bytes)) => {
                    let n = bytes.len();
                    buf.put_slice(&bytes);
                    self.offset += n as u64;
                    self.read_fut = None;

                    std::task::Poll::Ready(Ok(()))
                }
                std::task::Poll::Ready(Err(e)) => {
                    std::task::Poll::Ready(Err(std::io::Error::other(e)))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                let fut = self
                    .handle
                    .read_at(self.fd, self.offset, buf.remaining())
                    .map_err(std::io::Error::other)?
                    .boxed();
                self.read_fut = Some(fut);
                // we just created the fut, let them know they can poll it again and the next time
                // we'll attach the context to the fut
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}

impl AsyncSeek for File {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        let new_offset = match position {
            SeekFrom::Start(delta) => Some(delta),
            SeekFrom::Current(delta) => self.offset.checked_add_signed(delta),
            SeekFrom::End(delta) => self.attr.size.checked_add_signed(delta),
        };

        let Some(new_offset) = new_offset else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek resulted in an offset overflow",
            ));
        };

        if new_offset != self.offset {
            self.read_fut = None;
            self.write_fut = None;
            self.offset = new_offset;
        }
        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.offset))
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.write_fut.as_mut() {
            Some(fut) => match fut.poll_unpin(cx) {
                std::task::Poll::Ready(Ok(n)) => {
                    self.offset += n as u64;
                    std::task::Poll::Ready(Ok(n))
                }
                std::task::Poll::Ready(Err(e)) => {
                    std::task::Poll::Ready(Err(std::io::Error::other(e)))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                let fut = {
                    let handle = self.handle.clone();
                    let fd = self.fd;
                    let offset = self.offset;
                    let bytes = bytes::Bytes::copy_from_slice(buf);
                    async move { handle.write_at(fd, offset, bytes).unwrap().await }.boxed()
                };
                self.write_fut = Some(fut);

                // we just created the fut, let them know they can poll it again and the next time
                // we'll attach the context to the fut
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
