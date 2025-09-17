use bytes::Bytes;
use dashmap::DashMap;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use object_store::{ObjectStore, path::Path};
use std::{io::SeekFrom, pin::Pin, sync::Arc, task::Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// A boxed future that resolves to bytes.
pub(crate) type BytesFuture = Shared<BoxFuture<'static, Bytes>>;

/// Similar to std::ops::Range<u64>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ByteRange {
    /// Start of range, inclusive
    pub(crate) start: u64,
    /// End of range, exclusive
    pub(crate) end: u64,
}

impl ByteRange {
    pub(crate) fn contains(&self, idx: u64) -> bool {
        self.start <= idx && idx < self.end
    }

    pub(crate) fn len(&self) -> u64 {
        self.end - self.start
    }
}

impl From<ByteRange> for std::ops::Range<u64> {
    fn from(range: ByteRange) -> Self {
        std::ops::Range {
            start: range.start,
            end: range.end,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will pre-fetch the bytes
    /// within interval [i, i + size) in the background.
    size: u64,
    // Whether we should pre-fetch past the read-range boundary given to the VolumeFile. If
    // enabled, we will pre-fetch the bytes of the next subsequent file. compaction policy of the
    // files determines which file will get pre-fetched, but it will always be the data of the
    // file that is immediately after the file we just read.
    // TODO: prefetch_past_read_boundary: bool,
}

/// Handle for a file on a volume.
// TODO: this only handles reads for a file that lives completely on one volume. if it spans two
// volumes, then we need to keep a list of potential volumes and per-volume ranges that we'll
// iterate over.
// TODO: handle unexpected EOFs (e.g. end of volume when we requested a range that's too big)
pub(crate) struct VolumeFile {
    // TODO: smolstr? lots of cloning happening here.
    volume: String,

    /// Offset that represents what we've read so far in the volume, an absolute offset relative to
    /// the start of the volume.
    volume_read_cursor: u64,

    /// Overall byte range to read in the volume, the range is represented by absolute offsets
    /// relative to the start of the volume.
    volume_read_range: ByteRange,

    /// Byte range for reads in `chunk`, offsets are relative to `chunk`.
    chunk_read_range: ByteRange,

    /// Future that returns the current chunk (bytes) when we've finished reading from object
    /// storage. If the chunk is already cached, this future resolves immediately.
    chunk: BytesFuture,

    /// If provided, performs readaheads based on the given policy
    readahead: Option<ReadAheadPolicy>,

    /// Handle into our chunked object store cache.
    volume_chunk_store: Arc<ChunkedVolumeStore>,
}

impl VolumeFile {
    pub(crate) async fn new(
        mut volume_chunk_store: Arc<ChunkedVolumeStore>,
        volume: String,
        volume_read_range: ByteRange,
        readahead: Option<ReadAheadPolicy>,
    ) -> Self {
        // read the first chunk into memory
        let (absolute_chunk_range, chunk) =
            volume_chunk_store.get(volume.clone(), volume_read_range.start, &readahead);
        let relative_chunk_range = ByteRange {
            start: volume_read_range.start - absolute_chunk_range.start,
            end: std::cmp::min(
                volume_read_range.end - absolute_chunk_range.start,
                absolute_chunk_range.len(),
            ),
        };

        Self {
            volume,
            volume_read_cursor: volume_read_range.start,
            volume_read_range,
            chunk_read_range: relative_chunk_range,
            chunk,
            readahead,
            volume_chunk_store,
        }
    }

    /// Swap out the internal `chunk` for the subsequent chunk and perform read-ahead.
    /// no-op if VolumeFile is finished reading the entire volume range.
    fn load_next_chunk(&mut self) {
        if self.finished() {
            return;
        }

        // fetch the next chunk (and do readahead if a policy is given)
        let (absolute_chunk_range, chunk) = self.volume_chunk_store.get(
            self.volume.clone(),
            self.volume_read_cursor,
            &self.readahead,
        );

        // update the internal current chunk state
        self.chunk_read_range = ByteRange {
            start: self.volume_read_cursor - absolute_chunk_range.start,
            end: std::cmp::min(
                self.volume_read_range.end - absolute_chunk_range.start,
                absolute_chunk_range.len(),
            ),
        };
        self.chunk = chunk;
    }

    /// Returns the offset of what we've seeked to, relative to the start of the file.
    fn relative_read_cursor(&self) -> u64 {
        self.volume_read_cursor - self.volume_read_range.start
    }

    /// Return whether the VolumeFile has exhausted the reads within self.volume_read_range.
    fn finished(&self) -> bool {
        self.volume_read_cursor == self.volume_read_range.end
    }
}

impl AsyncRead for VolumeFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // read_range is exhausted, return Ready(Ok(())) to indicate EOF.
        if self.finished() {
            return Poll::Ready(Ok(()));
        }

        let mut_inner = self.get_mut();
        // chunk is a shared Future, it's cheap to clone. we need to clone since we may do
        // a partial read of the chunk.
        match mut_inner.chunk.clone().poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(bytes) => {
                let buf_capacity = buf.remaining();
                if buf_capacity as u64 >= mut_inner.chunk_read_range.len() {
                    // there's enough space in the buffer to read the whole chunk. push it into buf
                    // and load the next chunk.
                    buf.put_slice(&bytes.slice(std::ops::Range {
                        start: mut_inner.chunk_read_range.start as usize,
                        end: mut_inner.chunk_read_range.end as usize,
                    }));
                    mut_inner.volume_read_cursor += mut_inner.chunk_read_range.len();
                    mut_inner.load_next_chunk();

                    Poll::Ready(Ok(()))
                } else {
                    // partial read of the chunk since buf is too small for the chunk. push what we
                    // can and update the internal cursor + range.
                    buf.put_slice(&bytes.slice(std::ops::Range {
                        start: mut_inner.chunk_read_range.start as usize,
                        end: mut_inner.chunk_read_range.start as usize + buf_capacity,
                    }));
                    mut_inner.volume_read_cursor += buf_capacity as u64;
                    mut_inner.chunk_read_range.start += buf_capacity as u64;

                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

impl AsyncSeek for VolumeFile {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let new_read_cursor = match position {
            SeekFrom::Start(delta) => self.volume_read_range.start.checked_add(delta),
            SeekFrom::End(delta) => self.volume_read_range.end.checked_add_signed(delta),
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
        let (range, chunk) = mut_inner.volume_chunk_store.get(
            mut_inner.volume.clone(),
            new_read_cursor,
            &mut_inner.readahead,
        );

        // update the internal current chunk state
        mut_inner.volume_read_cursor = new_read_cursor;
        mut_inner.chunk_read_range = ByteRange {
            start: mut_inner.volume_read_cursor % mut_inner.volume_chunk_store.chunk_size,
            end: std::cmp::min(mut_inner.volume_read_range.end - range.start, range.len()),
        };
        mut_inner.chunk = chunk;

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
        // chunk is a shared Future, it's cheap to clone. we're cloning it since we still
        // want to be able to await it later on.
        match mut_inner.chunk.clone().poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Ok(mut_inner.relative_read_cursor())),
        }
    }
}

/// Describes a chunk within Volume. Does not map directly to a file, just an arbitrary chunk of
/// bytes within the volume.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CacheKey {
    volume: String,
    // TODO: maybe this only needs to be a single offset byte? unless we want dynamic chunk sizes
    // later on ...
    range: ByteRange,
}

/// A chunked object store cache.
///
/// Abstraction over Volumes in object storage. Volumes are partitioned up into fixed-size chunks.
/// Read requests (given a volume and byte offset) are served from cache, or fetched from object
/// storage if not already loaded.
pub struct ChunkedVolumeStore {
    /// Cache from the chunk described by CacheKey to the future that fetches the data from
    /// object storage. The future returns the underlying bytes for the chunk.
    // TODO: consider foyer's inmemory cache. it does eviction for us too.
    cache: DashMap<CacheKey, BytesFuture>,

    /// Size of each chunk in bytes
    chunk_size: u64,

    /// Object store to query chunks from volumes
    object_store: Arc<dyn ObjectStore>,
}

impl ChunkedVolumeStore {
    pub fn new(chunk_size: u64, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cache: DashMap::new(),
            chunk_size,
            object_store,
        }
    }

    /// Return a ByteRange and a BytesFuture that will resolve to a chunk that contains the byte
    /// specified by the given volume and offset byte. The ByteRange represents the absolute
    /// offsets relative to the start of the volume that the BytesFuture reads. If the chunk is
    /// not already cached, fetch it from object storage. If readahead is provided, we will
    /// pre-fetch chunks according to the readahead policy.
    ///
    /// ChunkedVolumeStore::get(...) is thread-safe as the underlying cache is DashMap.
    pub fn get(
        self: &mut Arc<Self>,
        volume: String,
        offset: u64,
        readahead: &Option<ReadAheadPolicy>,
    ) -> (ByteRange, BytesFuture) {
        let key = self.cache_key(volume.clone(), offset);

        let bytes_future = match self.cache.entry(key.clone()) {
            // if the chunk isn't cached, kick off a fetch from object storage and store a future
            // that will resolve when the fetch is done
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let chunk_future = chunk_from_object_store(
                    self.object_store.clone(),
                    entry.key().volume.clone(),
                    entry.key().range,
                )
                .boxed()
                .shared();

                // run this in the background now instead of waiting for the first awaiter
                tokio::spawn(chunk_future.clone());
                entry.insert(chunk_future.clone());

                chunk_future
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
        };

        if let Some(readahead) = readahead {
            self.readahead(volume.clone(), offset, readahead);
        }

        (key.range, bytes_future)
    }

    /// Perform read-aheads based on the given ReadAheadPolicy. Ensures that the chunks that cover
    /// the byte range starting at offset until offset+readahead are loaded into cache from object
    /// storage.
    ///
    /// ChunkedVolumeStore::readahead(...) is thread-safe as the underlying cache is DashMap.
    pub(crate) fn readahead(
        self: &mut Arc<Self>,
        volume: String,
        offset: u64,
        readahead: &ReadAheadPolicy,
    ) {
        // kick off a read from object store for chunks within the byte range (offset, offset + readahead]
        let last_readahead_byte = offset + readahead.size - 1;
        let start_chunk = (offset / self.chunk_size) * self.chunk_size;
        let end_chunk = (last_readahead_byte / self.chunk_size) * self.chunk_size;
        for chunk_start in (start_chunk..=end_chunk).step_by(self.chunk_size as usize) {
            let key = self.cache_key(volume.clone(), chunk_start);
            if let dashmap::mapref::entry::Entry::Vacant(entry) = self.cache.entry(key) {
                let chunk_future = chunk_from_object_store(
                    self.object_store.clone(),
                    entry.key().volume.clone(),
                    entry.key().range,
                )
                .boxed()
                .shared();

                // run this in the background now instead of waiting for the first awaiter
                tokio::spawn(chunk_future.clone());
                entry.insert(chunk_future.clone());
            }
        }
    }

    /// For a given volume and offset, construct a CacheKey to the chunk that holds the byte
    /// pointed to by the offset.
    fn cache_key(&self, volume: String, offset: u64) -> CacheKey {
        // the start of the chunk that contains offset
        let aligned = offset / self.chunk_size * self.chunk_size;
        CacheKey {
            volume: volume.clone(),
            range: ByteRange {
                start: aligned,
                end: aligned + self.chunk_size,
            },
        }
    }

    #[cfg(test)]
    fn cached(&self, volume: String, offset: u64) -> bool {
        let key = self.cache_key(volume, offset);
        self.cache.contains_key(&key)
    }
}

/// Read a chunk (as specified by the key and byte range) from object storage.
async fn chunk_from_object_store(
    store: Arc<dyn ObjectStore>,
    key: String,
    range: ByteRange,
) -> Bytes {
    let path = Path::from(key);
    // TODO: figure out what happens when the read fails ...
    store
        .get_range(&path, range.into())
        .await
        .expect("what error handling?")
}

#[cfg(test)]
mod test {
    use super::*;
    use object_store::{PutPayload, memory::InMemory};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn random_bytes(n: usize) -> Bytes {
        Bytes::from(rand::random_iter().take(n).collect::<Vec<_>>())
    }

    async fn object_store_with_data(key: String, bytes: Bytes) -> Arc<InMemory> {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&Path::from(key), PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");
        object_store
    }

    #[tokio::test]
    async fn test_read_full_volume_no_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 16); // 64 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 1 KiB chunks
        let volume_chunk_store = Arc::new(ChunkedVolumeStore::new(1 << 10, object_store));

        let mut file = VolumeFile::new(
            volume_chunk_store.clone(),
            volume,
            ByteRange {
                start: 0,
                end: data.len() as u64,
            },
            None,
        )
        .await;
        let mut file_data = Vec::with_capacity(data.len());
        let bytes_read = file
            .read_to_end(&mut file_data)
            .await
            .expect("read should be ok");
        assert_eq!(bytes_read, data.len());
        assert_eq!(data.as_ref(), file_data);
    }

    #[tokio::test]
    async fn test_read_partial_volume_no_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 16); // 64 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 1 KiB chunks
        let volume_chunk_store = Arc::new(ChunkedVolumeStore::new(1 << 10, object_store));

        // read the middle 1/3 of the volume, this is a weird number so partial chunks should be
        // read. this maps to reading bytes (21845..43690] which doesn't get chunked up cleanly!
        let read_range = ByteRange {
            start: (data.len() / 3) as u64,
            end: (2 * data.len() / 3) as u64,
        };
        let mut file = VolumeFile::new(volume_chunk_store.clone(), volume, read_range, None).await;
        let mut file_data = Vec::with_capacity(data.len());
        let bytes_read = file
            .read_to_end(&mut file_data)
            .await
            .expect("read should be ok");
        assert_eq!(bytes_read, read_range.len() as usize);

        let slice = data.slice(read_range.start as usize..read_range.end as usize);
        assert_eq!(slice.as_ref(), file_data);
    }

    #[tokio::test]
    async fn test_read_middle_file_partial() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 14); // 16 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 1 KiB chunks
        let volume_chunk_store = Arc::new(ChunkedVolumeStore::new(1 << 10, object_store));

        // 10 KiB file in the middle of the volume (file offset at 1KiB into the volume)
        let file_start = 1 << 10; // 1 KiB offset
        let file_size = 10 * (1 << 10); // 10 KiB
        let file_range = ByteRange {
            start: file_start,
            end: file_start + file_size,
        };
        let mut file = VolumeFile::new(volume_chunk_store.clone(), volume, file_range, None).await;

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
    async fn test_seek_readahead() {
        let volume = "volume".to_string();
        let data = random_bytes(1 << 10); // 1 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        // volume store fetches/caches 10 byte chunks
        let volume_chunk_store = Arc::new(ChunkedVolumeStore::new(10, object_store));

        // readahead size of 40 bytes (4 chunks)
        let readahead = ReadAheadPolicy { size: 40 };

        let mut file = VolumeFile::new(
            volume_chunk_store.clone(),
            volume.clone(),
            ByteRange {
                start: 0,
                end: data.len() as u64,
            },
            Some(readahead.clone()),
        )
        .await;

        // for a new file, we load the bytes in the readahead
        for offset in 0..readahead.size {
            assert!(volume_chunk_store.cached(volume.clone(), offset));
        }
        // 1 byte past the readahead, not cached
        assert!(!volume_chunk_store.cached(volume.clone(), readahead.size));

        // seek should also trigger readahead
        let offset = file
            .seek(SeekFrom::Start(234))
            .await
            .expect("seek should be ok");

        // every byte from the range is cached
        for offset in offset..(offset + readahead.size) {
            assert!(volume_chunk_store.cached(volume.clone(), offset));
        }

        // 230 is cached because it's part of the same chunk as 234
        assert!(volume_chunk_store.cached(volume.clone(), 230));
        // 229 is the last byte in the previous chunk, not cached
        assert!(!volume_chunk_store.cached(volume.clone(), 229));
        // 279 is cached because it's part of the same chunk as 234 + 40
        assert!(volume_chunk_store.cached(volume.clone(), 279));
        // 280 is the first byte in the chunk after the read-ahead, not cached
        assert!(!volume_chunk_store.cached(volume.clone(), 280));
    }

    #[tokio::test]
    async fn test_bad_seeks() {
        let volume = "volume".to_string();
        let data = random_bytes(1024); // 1 KiB
        let object_store = object_store_with_data(volume.clone(), data.clone()).await;

        let volume_chunk_store = Arc::new(ChunkedVolumeStore::new(256, object_store));

        // create a file with a volume range of [100, 600) (size: 500 bytes)
        let file_range = ByteRange {
            start: 100,
            end: 600,
        };
        let mut file = VolumeFile::new(volume_chunk_store.clone(), volume, file_range, None).await;
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
