use std::{io::SeekFrom, pin::Pin};

use futures::FutureExt;
use pond::FileAttr;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::Volume;

pub struct ReadOnlyFile {
    fd: pond::Fd,
    attr: FileAttr,
    offset: u64,
    handle: Volume,
    fut: Option<Pin<Box<dyn Future<Output = pond::Result<bytes::Bytes>>>>>,
}

impl ReadOnlyFile {
    pub(crate) fn new(fd: pond::Fd, attr: FileAttr, handle: Volume) -> Self {
        Self {
            fd,
            attr,
            offset: 0,
            handle,
            fut: None,
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
                    let size = buf.remaining();
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
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
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
            let mut_inner = self.get_mut();
            mut_inner.fut = None;
            mut_inner.offset = new_offset;
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
