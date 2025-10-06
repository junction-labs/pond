use crate::{
    ByteRange, Location,
    read::{BytesFuture, ChunkCache, ReadError},
};
use futures::FutureExt;
use std::{cmp::min, io::SeekFrom, pin::Pin, sync::Arc, task::Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

enum State {
    Loading(BytesFuture),
    Ready(bytes::Bytes),
}

/// Handle for a file on a volume. Not thread-safe.
// TODO: this only handles reads for a file that lives completely on one volume. if it spans two
// volumes, then we need to keep a list of potential volumes and per-volume ranges that we'll
// iterate over.
// TODO: handle unexpected EOFs (e.g. end of volume when we requested a range that's too big)
pub struct File {
    location: Location,

    /// Offset that represents what we've read so far in the volume, an absolute offset relative to
    /// the start of the volume.
    volume_read_cursor: u64,

    /// Overall byte range to read in the volume, the range is represented by absolute offsets
    /// relative to the start of the volume.
    volume_read_range: ByteRange,

    /// Byte range for reads in `chunk`, offsets are relative to `chunk`.
    chunk_read_range: ByteRange,

    /// State of the current chunk. Either a future that will resolve into Bytes (which represents
    /// the chunk) or the Bytes backing the chunk itself.
    state: State,

    /// Number of errors encountered while trying to read this chunk. Reset after each successful
    /// read.
    num_read_errors: u8,

    /// Handle into our chunked object store cache.
    cache: Arc<ChunkCache>,
}

impl File {
    const READ_ERROR_THRESHOLD: u8 = 3;

    pub fn new(
        mut volume_chunk_store: Arc<ChunkCache>,
        location: Location,
        volume_read_range: ByteRange,
    ) -> Self {
        // read the first chunk into memory
        let (absolute_chunk_range, chunk) =
            volume_chunk_store.get(location.clone(), volume_read_range.offset);
        let offset = volume_read_range.offset - absolute_chunk_range.offset;
        let relative_chunk_range = ByteRange {
            offset,
            len: min(
                // remaining bytes in the fetched chunk from our position
                absolute_chunk_range.end() - volume_read_range.offset,
                // remaining bytes in our read range from our position
                volume_read_range.len,
            ),
        };

        Self {
            location,
            volume_read_cursor: volume_read_range.offset,
            volume_read_range,
            chunk_read_range: relative_chunk_range,
            state: State::Loading(chunk),
            cache: volume_chunk_store,
            num_read_errors: 0,
        }
    }

    /// Swap out the internal `chunk` for the subsequent chunk and perform read-ahead.
    /// no-op if VolumeFile is finished reading the entire volume range.
    fn load_next_chunk(&mut self) {
        if self.finished() {
            return;
        }

        // fetch the next chunk and do readahead
        let (absolute_chunk_range, chunk) = self
            .cache
            .get(self.location.clone(), self.volume_read_cursor);

        // update the internal current chunk state
        let offset = self.volume_read_cursor - absolute_chunk_range.offset;
        self.chunk_read_range = ByteRange {
            offset,
            len: min(
                // end of fetched chunk to our offset
                absolute_chunk_range.end() - self.volume_read_cursor,
                // end of volume_read_range to our offset (when our read_range ends somewhere in
                // the middle of the chunk)
                self.volume_read_range.end() - self.volume_read_cursor,
            ),
        };

        self.state = State::Loading(chunk);
    }

    fn poll_chunk(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<bytes::Bytes>> {
        loop {
            match &mut self.state {
                State::Ready(bytes) => return Poll::Ready(Ok(bytes.clone())),
                State::Loading(fut) => {
                    let Poll::Ready(result) = fut.poll_unpin(cx) else {
                        return Poll::Pending;
                    };

                    match self.try_load_chunk(result)? {
                        Some(bytes) => return Poll::Ready(Ok(bytes)),
                        None => continue, // retry after re-seeking
                    }
                }
            }
        }
    }

    fn try_load_chunk(
        &mut self,
        poll_result: Result<bytes::Bytes, ReadError>,
    ) -> std::io::Result<Option<bytes::Bytes>> {
        match poll_result {
            Ok(bytes) => {
                self.num_read_errors = 0;
                self.state = State::Ready(bytes.clone());
                Ok(Some(bytes))
            }
            // enough bad reads that we're just giving up
            Err(e) if self.num_read_errors >= Self::READ_ERROR_THRESHOLD => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    format!(
                        "failed to seek to file pos {} (pos {} in volume): {e}",
                        self.relative_read_cursor(),
                        self.volume_read_cursor
                    ),
                ))
            }
            // bad read, but try again (by re-seeking which resets the chunk)
            Err(_) => {
                self.num_read_errors += 1;
                let mut pinned = std::pin::pin!(self);
                match pinned.as_mut().start_seek(SeekFrom::Current(0)) {
                    Ok(()) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Returns the offset of what we've seeked to, relative to the start of the file.
    fn relative_read_cursor(&self) -> u64 {
        self.volume_read_cursor - self.volume_read_range.offset
    }

    /// Return whether the VolumeFile has exhausted the reads within self.volume_read_range.
    fn finished(&self) -> bool {
        self.volume_read_cursor == self.volume_read_range.end()
    }
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut_inner = self.get_mut();
        let mut made_progress = false;

        loop {
            // read_range is exhausted or there's no space in the buffer, we're done
            if mut_inner.finished() || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let bytes = match mut_inner.poll_chunk(cx) {
                Poll::Ready(Ok(bytes)) => bytes,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    if made_progress {
                        return Poll::Ready(Ok(()));
                    }
                    return Poll::Pending;
                }
            };

            let buf_capacity = buf.remaining();
            if buf_capacity as u64 >= mut_inner.chunk_read_range.len {
                // can read the entire remaining chunk into the buf. load the next chunk
                // after pushing these into the buf, to see if we can jam more things into
                // it.
                buf.put_slice(&bytes.slice(mut_inner.chunk_read_range.as_range_usize()));
                mut_inner.volume_read_cursor += mut_inner.chunk_read_range.len;
                mut_inner.load_next_chunk();
                made_progress = true;

                // since we haven't finished pushing everything we can into the buf, continue on in
                // a loop. if we return Poll::Ready(Ok(())) with a buffer that isn't full then
                // we're signaling that it's an EOF. so instead we loop over again here
                continue;
            } else {
                // partial read of the chunk since buf is too small for the chunk. push what we
                // can and update the internal cursor + range.
                buf.put_slice(&bytes.slice(std::ops::Range {
                    start: mut_inner.chunk_read_range.offset as usize,
                    end: mut_inner.chunk_read_range.offset as usize + buf_capacity,
                }));
                mut_inner.volume_read_cursor += buf_capacity as u64;
                mut_inner.chunk_read_range.advance(buf_capacity as u64);

                return Poll::Ready(Ok(()));
            }
        }
    }
}

impl AsyncSeek for File {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let new_read_cursor = match position {
            SeekFrom::Start(delta) => self.volume_read_range.offset.checked_add(delta),
            SeekFrom::End(delta) => self.volume_read_range.end().checked_add_signed(delta),
            SeekFrom::Current(delta) => self.volume_read_cursor.checked_add_signed(delta),
        };
        let Some(new_read_cursor) = new_read_cursor else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek resulted in an u64 offset overflow",
            ));
        };
        if !self.volume_read_range.contains(new_read_cursor) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "out of bounds seek",
            ));
        }

        let mut_inner = self.get_mut();
        // kick off an object store fetch for the chunk for new_read_cursor
        let (absolute_chunk_range, chunk) = mut_inner
            .cache
            .get(mut_inner.location.clone(), new_read_cursor);

        // update the internal current chunk state
        mut_inner.volume_read_cursor = new_read_cursor;
        let offset = mut_inner.volume_read_cursor - absolute_chunk_range.offset;
        mut_inner.chunk_read_range = ByteRange {
            offset,
            len: min(
                // remaining bytes in the fetched chunk from our position
                absolute_chunk_range.end() - new_read_cursor,
                // remaining bytes in our read range from our position
                mut_inner.volume_read_range.end() - new_read_cursor,
            ),
        };
        mut_inner.state = State::Loading(chunk);

        Ok(())
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        // EOF seek
        if self.finished() {
            return Poll::Ready(Ok(self.relative_read_cursor()));
        }

        let mut_inner = self.get_mut();

        match mut_inner.poll_chunk(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(mut_inner.relative_read_cursor())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::read::ClientBuilder;

    use super::*;
    use bytes::Bytes;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn random_bytes(n: usize) -> Bytes {
        Bytes::from(rand::random_iter().take(n).collect::<Vec<_>>())
    }

    async fn object_store_with_data(key: String, bytes: Bytes) -> ClientBuilder {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&Path::from(key), PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");

        Arc::new(move |_| object_store.clone())
    }

    #[tokio::test]
    async fn objectstore_full_volume() {
        assert_objectstore_full_volume(1024).await;
        assert_objectstore_full_volume(7).await;
        assert_objectstore_full_volume(1234).await;
    }

    async fn assert_objectstore_full_volume(chunksize: u64) {
        let data = random_bytes(1 << 16); // 64 KiB
        let client_builder = object_store_with_data("volume".to_string(), data.clone()).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
        };

        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            chunksize,
            Default::default(),
            client_builder,
        ));

        let mut file = File::new(
            volume_chunk_store.clone(),
            location,
            ByteRange {
                offset: 0,
                len: data.len() as u64,
            },
        );

        let mut file_data = Vec::with_capacity(data.len());
        let bytes_read = file
            .read_to_end(&mut file_data)
            .await
            .expect("read should be ok");
        assert_eq!(bytes_read, data.len(), "chunksize={chunksize}");
        assert_eq!(data.as_ref(), file_data, "chunksize={chunksize}");
    }

    #[tokio::test]
    async fn objectstore_partial_volume() {
        assert_objectstore_partial_volume(1024).await;
        assert_objectstore_partial_volume(7).await;
        assert_objectstore_partial_volume(1234).await;
    }

    async fn assert_objectstore_partial_volume(chunksize: u64) {
        let data = random_bytes(1 << 16); // 64 KiB
        let client_builder = object_store_with_data("volume".to_string(), data.clone()).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
        };

        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            chunksize,
            Default::default(),
            client_builder,
        ));

        // read the middle 1/3 of the volume, this is a weird number so partial chunks should be
        // read. this maps to reading bytes (21845..43690] which doesn't get chunked up cleanly!
        let read_range = ByteRange {
            offset: (data.len() / 3) as u64,
            len: (data.len() / 3) as u64,
        };
        let mut file = File::new(volume_chunk_store.clone(), location, read_range);
        let mut file_data = Vec::with_capacity(data.len());
        let bytes_read = file
            .read_to_end(&mut file_data)
            .await
            .expect("read should be ok");
        assert_eq!(bytes_read, read_range.len as usize);

        let slice = data.slice(read_range.as_range_usize());
        assert_eq!(slice.as_ref(), file_data);
    }

    #[tokio::test]
    async fn objectstore_full_volume_with_buf() {
        assert_objectstore_full_volume_with_buf(1024, 23).await;
        assert_objectstore_full_volume_with_buf(1024, 123).await;
        assert_objectstore_full_volume_with_buf(500, 1333).await;
        assert_objectstore_full_volume_with_buf(777, 777).await;
        assert_objectstore_full_volume_with_buf(1001, 77).await;
    }

    async fn assert_objectstore_full_volume_with_buf(chunksize: u64, bufsize: usize) {
        let data = random_bytes(1 << 16); // 64 KiB
        let client_builder = object_store_with_data("volume".to_string(), data.clone()).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
        };

        // volume store fetches/caches 8 KiB chunks
        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            chunksize,
            Default::default(),
            client_builder,
        ));

        let mut file = File::new(
            volume_chunk_store.clone(),
            location,
            ByteRange {
                offset: 0,
                len: data.len() as u64,
            },
        );

        let mut file_data: Vec<u8> = Vec::with_capacity(data.len());
        loop {
            let mut buf = vec![0u8; bufsize];
            let n = file.read(&mut buf).await.unwrap();
            file_data.extend(buf[..n].iter());
            if n == 0 {
                break;
            }
        }

        println!("{} vs {}", data.len(), file_data.len());
        assert_eq!(data.as_ref(), file_data);
    }

    #[tokio::test]
    async fn local_full_volume() {
        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            1 << 4,
            Default::default(),
            Arc::new(|_| Arc::new(InMemory::new())),
        ));

        let mut tmpfile = NamedTempFile::new().unwrap();
        let data = random_bytes(1 << 10); // 64 KiB
        tmpfile.write_all(&data).unwrap();

        let location = Location::Local {
            path: tmpfile.path().to_path_buf(),
        };

        let mut file = File::new(
            volume_chunk_store.clone(),
            location,
            ByteRange {
                offset: 0,
                len: 1 << 10,
            },
        );

        let mut file_data = Vec::with_capacity(data.len());
        let bytes_read = file
            .read_to_end(&mut file_data)
            .await
            .expect("read should be ok");
        assert_eq!(bytes_read, 1 << 10);

        assert_eq!(data.as_ref(), file_data);
    }

    #[tokio::test]
    async fn seek_partial_volume() {
        let data = random_bytes(1 << 14); // 16 KiB
        let client_builder = object_store_with_data("volume".to_string(), data.clone()).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
        };

        // volume store fetches/caches 1 KiB chunks
        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            1 << 10,
            Default::default(),
            client_builder,
        ));

        // 10 KiB file in the middle of the volume (file offset at 1KiB into the volume)
        let file_start = 1 << 10; // 1 KiB offset
        let file_range = ByteRange {
            offset: file_start,
            len: 10 * (1 << 10), // 10 KiB
        };
        let mut file = File::new(volume_chunk_store.clone(), location, file_range);

        // seek to the middle of the file (512 bytes into the 1 KiB file)
        let seek_offset = file.seek(SeekFrom::Start(512)).await.unwrap();
        assert_eq!(seek_offset, 512);

        // read 128 bytes from here, make sure it matches the raw data
        let mut buffer = vec![0u8; 128];
        let bytes_read = file.read_exact(&mut buffer).await.unwrap();
        assert_eq!(bytes_read, 128);
        assert_eq!(
            data.slice((file_start + 512) as usize..(file_start + 512 + 128) as usize)
                .as_ref(),
            buffer
        );

        // read the following 2345 bytes
        buffer.resize(2345, 0u8);
        let bytes_read = file.read_exact(&mut buffer).await.unwrap();
        assert_eq!(bytes_read, 2345);
        assert_eq!(
            data.slice((file_start + 512 + 128) as usize..(file_start + 512 + 128 + 2345) as usize)
                .as_ref(),
            buffer
        );
    }

    #[tokio::test]
    async fn bad_seeks() {
        let data = random_bytes(1024); // 1 KiB
        let client_builder = object_store_with_data("volume".to_string(), data.clone()).await;
        let location = Location::ObjectStorage {
            bucket: "".to_string(),
            key: "volume".to_string(),
        };

        let volume_chunk_store = Arc::new(ChunkCache::new_with(
            256,
            Default::default(),
            client_builder,
        ));

        // create a file with a volume range of [100, 600)
        let file_range = ByteRange {
            offset: 100,
            len: 500,
        };
        let mut file = File::new(volume_chunk_store.clone(), location, file_range);
        // seek should be at pos 0
        assert_eq!(file.stream_position().await.unwrap(), 0);

        // seek before the file starts
        let result = file.seek(SeekFrom::Current(-1)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek way before the start (underflow)
        let result = file.seek(SeekFrom::Current(-200)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek past file range
        let result = file.seek(SeekFrom::Current(500)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek past the end of the file range
        let result = file.seek(SeekFrom::Start(500)).await; // file is only 500 bytes
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek overflow
        let result = file.seek(SeekFrom::Start(u64::MAX)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek the very end
        let result = file.seek(SeekFrom::End(0)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek from end with positive offset (past end)
        let result = file.seek(SeekFrom::End(1)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek from end with large negative offset (before start)
        let result = file.seek(SeekFrom::End(-501)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

        // seek underflow
        let result = file.seek(SeekFrom::End(i64::MIN)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    }
}
