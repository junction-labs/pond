use std::{io::SeekFrom, pin::Pin};

use futures::FutureExt;
use pond_core::FileAttr;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::Volume;

pub struct ReadOnlyFile {
    fd: pond_core::Fd,
    attr: FileAttr,
    offset: u64,
    handle: Volume,
    fut: Option<Pin<Box<dyn Future<Output = pond_core::Result<bytes::Bytes>>>>>,
}

impl ReadOnlyFile {
    pub(crate) fn new(fd: pond_core::Fd, attr: FileAttr, handle: Volume) -> Self {
        Self {
            fd,
            attr,
            offset: 0,
            handle,
            fut: None,
        }
    }
}

/// Close the Fd attached to this file.
impl Drop for ReadOnlyFile {
    fn drop(&mut self) {
        // attempt to release the handle synchronously first, and if that fails then fire off an
        // tokio task that'll do asynchronously.
        match self.handle.try_release_fd(self.fd) {
            Ok(()) => (),
            Err(fd) => {
                let handle = self.handle.clone();
                tokio::spawn(async move { handle.release_fd(fd).await });
            }
        }
    }
}

impl AsyncRead for ReadOnlyFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.fut.as_mut() {
            Some(fut) => match fut.poll_unpin(cx) {
                std::task::Poll::Ready(Ok(bytes)) => {
                    let n = bytes.len();
                    buf.put_slice(&bytes);
                    self.offset += n as u64;
                    self.fut = None;

                    std::task::Poll::Ready(Ok(()))
                }
                std::task::Poll::Ready(Err(e)) => {
                    std::task::Poll::Ready(Err(std::io::Error::other(e)))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => {
                let fut = {
                    let fd = self.fd;
                    let offset = self.offset;
                    let size = std::cmp::min((self.attr.size - offset) as usize, buf.remaining());
                    let handle = self.handle.clone();
                    async move { handle.read_at(fd, offset, size).await }
                };
                self.fut = Some(Box::pin(fut));

                // we just created the fut, let them know they can poll it again and the next time
                // we'll attach the context to the fut
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}

impl AsyncSeek for ReadOnlyFile {
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
            self.fut = None;
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

pub struct ReadWriteFile {
    fd: pond_core::Fd,
    attr: FileAttr,
    offset: u64,
    handle: Volume,
    read_fut: Option<Pin<Box<dyn Future<Output = pond_core::Result<bytes::Bytes>>>>>,
    write_fut: Option<Pin<Box<dyn Future<Output = pond_core::Result<usize>>>>>,
}

impl ReadWriteFile {
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
impl Drop for ReadWriteFile {
    fn drop(&mut self) {
        // attempt to release the handle synchronously first, and if that fails then fire off an
        // tokio task that'll do asynchronously.
        match self.handle.try_release_fd(self.fd) {
            Ok(()) => (),
            Err(fd) => {
                let handle = self.handle.clone();
                tokio::spawn(async move { handle.release_fd(fd).await });
            }
        }
    }
}

impl AsyncRead for ReadWriteFile {
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
                let fut = {
                    let handle = self.handle.clone();
                    let fd = self.fd;
                    let offset = self.offset;
                    let size = buf.remaining();
                    async move { handle.read_at(fd, offset, size).await }
                };
                self.read_fut = Some(Box::pin(fut));

                // we just created the fut, let them know they can poll it again and the next time
                // we'll attach the context to the fut
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}

impl AsyncSeek for ReadWriteFile {
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

impl AsyncWrite for ReadWriteFile {
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
                    async move { handle.write_at(fd, offset, bytes).await }
                };
                self.write_fut = Some(Box::pin(fut));

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
