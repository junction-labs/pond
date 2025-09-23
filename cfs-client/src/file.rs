use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Poll, ready};

use cfs_core::ByteRange;
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt};

pub(crate) struct FileView {
    file: tokio::fs::File,
    range: ByteRange,
    pos: u64,
}

impl FileView {
    pub(crate) async fn from_file(
        mut file: tokio::fs::File,
        range: ByteRange,
    ) -> std::io::Result<Self> {
        let pos = file.seek(SeekFrom::Start(range.offset)).await?;
        Ok(Self { file, pos, range })
    }
}

impl AsyncRead for FileView {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // clamp buf based on the current offset and range. buf.take deals
        // with clamping this to buf's max possible size. do the read with
        // the taken slice.
        let buf_size = self
            .range
            .len
            .saturating_sub(self.pos.saturating_sub(self.range.offset));
        let mut b = buf.take(buf_size as usize);

        let file = Pin::new(&mut self.file);
        ready!(file.poll_read(cx, &mut b))?;

        // have to mark the original readbuf as initialized and advance it. the
        // file read got done with a new slice, so the original was not modifed.
        let n = b.filled().len();
        unsafe {
            buf.assume_init(n);
        }
        buf.advance(n);
        // update our own internal bookkeeping
        self.pos += n as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FileView {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        // adjust the seek so that it's always within the range.
        let position = clamp(self.pos, self.range, position);
        let file = Pin::new(&mut self.file);
        file.start_seek(position)
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        let file = Pin::new(&mut self.file);
        // pass on the op to the underlying file, and then adjust the output to the
        // absolute position within the range once we're done.
        let pos = ready!(file.poll_complete(cx))?;
        self.pos = pos.saturating_sub(self.range.offset);
        Poll::Ready(Ok(pos))
    }
}

fn clamp(pos: u64, range: ByteRange, seek: SeekFrom) -> SeekFrom {
    match seek {
        // make sure we're in [offset, range + offset), since the file will
        // happily seek beyond our view.
        SeekFrom::Start(n) => {
            let n = range.offset + n.min(range.len);
            SeekFrom::Start(n)
        }
        // translate this to a seek from start, because we know the
        // end of our range, but don't know the file's total size.
        SeekFrom::End(n) => {
            let view_end = (range.offset + range.len) as i64;
            let n = view_end.saturating_add(n) as u64;
            let n = n.min(view_end as u64).max(range.offset);
            SeekFrom::Start(n)
        }
        // clamping (pos +n) to [offset, len + offset) implies that
        // (offset - pos) <= n and n < (len + offset - pos)
        SeekFrom::Current(n) => {
            let pos = pos as i64;
            let range_min = (range.offset as i64).saturating_sub(pos);
            let range_max = range_min.saturating_add(range.len as i64);

            let n = n.max(range_min as i64).min(range_max as i64);
            SeekFrom::Current(n)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;
    use tokio::io::AsyncReadExt;

    // TODO: arbtest for clamping range
    #[test]
    fn clamp_current_interior() {
        let range = ByteRange { offset: 4, len: 7 };
        for pos in (range.offset + 1)..(range.offset + range.len - 1) {
            // with some space on either side should always be able to seek 1 in
            // either direction
            assert_eq!(
                clamp(pos, range, SeekFrom::Current(1)),
                SeekFrom::Current(1)
            );
            assert_eq!(
                clamp(pos, range, SeekFrom::Current(-1)),
                SeekFrom::Current(-1)
            );
        }
    }

    #[test]
    fn clamp_current_start() {
        let range = ByteRange { offset: 4, len: 7 };
        for pos in range.offset..(range.offset + range.len) {
            // try ot seek way past the start
            let input = -(range.len as i64) * 5;
            let expected = -1 * (pos - range.offset) as i64;
            assert_eq!(
                SeekFrom::Current(expected),
                clamp(pos, range, SeekFrom::Current(input)),
                "pos={pos} range={range:?}",
            );
        }
    }

    #[test]
    fn clamp_current_end() {
        let range = ByteRange { offset: 4, len: 7 };
        for pos in range.offset..(range.offset + range.len) {
            // try to seek way past the end
            let input = range.len as i64 * 5;
            let expected = range.len as i64 - (pos - range.offset) as i64;
            assert_eq!(
                SeekFrom::Current(expected),
                clamp(pos, range, SeekFrom::Current(input)),
                "pos={pos} range={range:?}",
            );
        }
    }

    #[tokio::test]
    async fn read_to_end() {
        let test_file = test_file(b"aaaabbbbcccc");
        let mut view = FileView::from_file(test_file.into(), ByteRange { offset: 4, len: 4 })
            .await
            .unwrap();

        let mut content = String::with_capacity(1024);
        view.read_to_string(&mut content).await.unwrap();
        assert_eq!(content, "bbbb");
    }

    #[tokio::test]
    async fn read() {
        let test_file = test_file(b"aaaabbbbcccc");
        let mut view = FileView::from_file(test_file.into(), ByteRange { offset: 4, len: 4 })
            .await
            .unwrap();

        let mut buf = [0, 0, 0];
        assert_eq!(3, view.read(&mut buf).await.unwrap());
        assert_eq!(&buf[..3], b"bbb");
        assert_eq!(1, view.read(&mut buf).await.unwrap());
        assert_eq!(&buf[..1], b"b");
        assert_eq!(0, view.read(&mut buf).await.unwrap());
    }

    fn test_file(content: &[u8]) -> tokio::fs::File {
        let mut f = tempfile::tempfile().unwrap();
        f.write_all(content).expect("failed to write test content");
        tokio::fs::File::from_std(f)
    }
}
