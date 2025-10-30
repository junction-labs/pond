use bytes::Bytes;
use std::{ops::Range, sync::Arc};

use crate::{Error, error::ErrorKind, metrics::RecordLatencyGuard, scoped_timer};

#[derive(Debug, Clone, Default)]
pub(crate) struct ReadAheadPolicy {
    /// Size of read-ahead in bytes. if you read a byte at index i, we will
    /// pre-fetch the bytes within interval [i, i + size) in the background.
    pub(crate) size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Chunk {
    path: Arc<object_store::path::Path>,
    offset: u64,
}

pub(crate) struct ChunkCache {
    inner: Arc<ChunkCacheInner>,
}

struct ChunkCacheInner {
    store: crate::storage::Storage,
    cache: foyer::Cache<Chunk, Bytes>,
    chunk_size: u64,
    readahead_policy: ReadAheadPolicy,
}

impl ChunkCache {
    pub(crate) fn new(
        max_cache_size: u64,
        chunk_size: u64,
        store: crate::storage::Storage,
        readahead_policy: ReadAheadPolicy,
    ) -> Self {
        let max_cache_size = (max_cache_size / chunk_size) as usize;
        let cache = foyer::Cache::<Chunk, Bytes>::builder(max_cache_size)
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();

        let inner = ChunkCacheInner {
            cache,
            chunk_size,
            readahead_policy,
            store,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn clear(&self) {
        self.inner.cache.clear()
    }

    /// Record the current number of entries within the cache and the size of the cache into their
    /// respective gauge metrics.
    pub(crate) fn record_cache_size(&self) {
        let num_entries = self.inner.cache.usage();
        metrics::gauge!("pond_cache_entries_count").set(num_entries as f64);
        metrics::gauge!("pond_cache_size_bytes")
            .set(num_entries as f64 * self.inner.chunk_size as f64);
    }

    pub(crate) async fn get_at(
        &self,
        path: Arc<object_store::path::Path>,
        offset: u64,
        len: u64,
    ) -> crate::Result<Vec<Bytes>> {
        let mut chunks = vec![];
        let read_offsets: Vec<_> = read_offsets(offset, len, self.inner.chunk_size)?.collect();

        for offset in &read_offsets {
            let cache = self.inner.clone();
            let path = path.clone();
            let offset = *offset;
            chunks.push(tokio::spawn(async move {
                cache.fetch_aligned(path, offset).await
            }));
        }

        // touch any readahead chunks. this gets done in background tasks
        // without waiting for them here.
        let readahead_offsets = readahead_offsets(
            offset,
            len,
            self.inner.readahead_policy.size,
            self.inner.chunk_size,
        )?;
        for offset in readahead_offsets {
            let cache = self.inner.clone();
            let path = path.clone();
            tokio::spawn(async move { cache.fetch_aligned(path, offset).await });
        }

        // actually fetch the chunks in parallel here
        //
        // TODO: should we spawn these tasks as well to make sure they're all
        // being fetched on separate threads if possible?
        // TODO: we should figure out what order errors come back in here or if
        // we want all of them. any error can (and should!) kill the whole fetch
        // though.
        let mut chunks: Vec<_> = match futures::future::try_join_all(chunks).await {
            Ok(chunks_res) => chunks_res.into_iter().collect::<Result<_, _>>()?,
            Err(join_err) => panic!("tasks failed: {join_err}"),
        };

        // trim the first chunk so that it starts at the start of the requested
        // range, instead of at the start of an aligned chunk. using Bytes::slice
        // here means we're just taking a ref-counted view into the cache and not
        // actually copying the range.
        if let Some(first_chunk) = chunks.first_mut() {
            let chunk_offset = read_offsets[0];
            let slice_start = (offset - chunk_offset) as usize;
            *first_chunk = first_chunk.slice(slice_start..);
        }
        // trim the last chunk so that it ends at the end of the requested range,
        // or the end of the object - whichever is shorter - instead of ending
        // at a chunk alignment boundary.
        //
        // slicing Bytes means that we're taking a ref-counted view into the cache
        // instead of making a copy of the range.
        if let Some(last_chunk) = chunks.last_mut() {
            let chunk_offset = read_offsets[read_offsets.len() - 1];
            let object_end = offset + len;
            let slice_end = std::cmp::min((object_end - chunk_offset) as usize, last_chunk.len());
            *last_chunk = last_chunk.slice(..slice_end);
        }

        Ok(chunks)
    }
}

impl ChunkCacheInner {
    /// fetch `self.chunk_size` bytes starting at the given offset. assumes
    /// offsets have already been aligned to `self.chunk_size` and panics
    /// if they have not.
    async fn fetch_aligned(
        &self,
        path: Arc<object_store::path::Path>,
        offset: u64,
    ) -> crate::Result<Bytes> {
        assert!(offset.is_multiple_of(self.chunk_size));

        let range = offset..(offset + self.chunk_size);
        let chunk = Chunk {
            path: path.clone(),
            offset,
        };

        metrics::counter!("pond_cache_requests_total").increment(1);
        let entry = self
            .cache
            .fetch(chunk, || {
                metrics::counter!("pond_cache_misses_total").increment(1);
                scoped_timer!("pond_cache_fetch_latency_secs");
                get_range(self.store.clone(), path, range)
            })
            .await?;
        Ok(entry.value().clone())
    }

    #[cfg(test)]
    fn is_cached(&self, path: Arc<object_store::path::Path>, offset: u64) -> bool {
        let offset = (offset / self.chunk_size) * self.chunk_size;
        let chunk = Chunk { path, offset };
        self.cache.contains(&chunk)
    }
}

impl From<foyer_memory::Error> for crate::Error {
    fn from(value: foyer_memory::Error) -> Self {
        Self::new(ErrorKind::Other, format!("cache error: {value}"))
    }
}

// NOTE: keeping this outlined makes rustc happy about move and local references
// where inlining it makes helllllllllla problems.
async fn get_range(
    store: crate::storage::Storage,
    path: Arc<object_store::path::Path>,
    range: Range<u64>,
) -> crate::Result<Bytes> {
    let bs = store
        .remote
        .get_range(&path, range)
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { path, source } => {
                Error::with_source(ErrorKind::NotFound, path, source)
            }
            object_store::Error::InvalidPath { source } => {
                Error::with_source(ErrorKind::InvalidData, "invalid path", source)
            }
            object_store::Error::PermissionDenied { path, source }
            | object_store::Error::Unauthenticated { path, source } => {
                Error::with_source(ErrorKind::PermissionDenied, path, source)
            }
            err => Error::with_source(ErrorKind::Other, "get_range failed", err),
        })?;
    Ok(bs)
}

#[inline]
fn next_chunk(offset: u64, chunk_size: u64) -> crate::Result<u64> {
    ((offset / chunk_size) * chunk_size)
        .checked_add(chunk_size)
        .ok_or(Error::new(
            ErrorKind::InvalidData,
            "u64 overflow when computing next chunk offset",
        ))
}

fn read_offsets(
    offset: u64,
    len: u64,
    chunk_size: u64,
) -> crate::Result<impl Iterator<Item = u64>> {
    // safety: if offset + len doesn't overflow, subtracting 1 is safe.
    let last_byte = offset.checked_add(len).ok_or(Error::new(
        ErrorKind::InvalidData,
        "u64 overflow when computing read offsets",
    ))? - 1;

    // safety: the divide-then-multiply is safe here since the divided values
    // are always less than last_byte, and by definiton the start of the chunk
    // containing last_byte is always less than or equal to last_byte
    let start_chunk = (offset / chunk_size) * chunk_size;
    let end_chunk = (last_byte / chunk_size) * chunk_size;

    // refuse to cause overflow later by checking to see if end_chunk +
    // chunk_size would overflow
    end_chunk.checked_add(chunk_size).ok_or(Error::new(
        ErrorKind::InvalidData,
        "u64 overflow when computing end of chunk range",
    ))?;

    Ok((start_chunk..=end_chunk).step_by(chunk_size as usize))
}

fn readahead_offsets(
    offset: u64,
    len: u64,
    readahead: u64,
    chunk_size: u64,
) -> crate::Result<impl Iterator<Item = u64>> {
    let adjust_offset = offset.checked_add(len).ok_or(Error::new(
        ErrorKind::InvalidData,
        "u64 overflow when computing readahead offset",
    ))?;
    let readahead_start = next_chunk(adjust_offset, chunk_size)?;
    read_offsets(readahead_start, readahead, chunk_size)
}

#[cfg(test)]
mod test {
    use std::num::NonZero;

    use super::*;
    use arbtest::arbtest;
    use bytes::Bytes;
    use object_store::{ObjectStore, PutPayload};

    async fn storage_with_data(
        key: &object_store::path::Path,
        bytes: Bytes,
    ) -> crate::storage::Storage {
        let storage = crate::storage::Storage::new_in_memory().unwrap();
        storage
            .remote
            .put(key, PutPayload::from_bytes(bytes))
            .await
            .expect("put into inmemory store should be ok");
        storage
    }

    #[test]
    fn fuzz_read_offsets() {
        arbtest(|u| {
            let chunk_size: u64 = u.arbitrary::<NonZero<_>>()?.get();
            let offset: u64 = u.arbitrary()?;
            let len: u64 = u.arbitrary::<NonZero<_>>()?.get();

            match read_offsets(offset, len, chunk_size) {
                // no overflow
                //
                // we can play fast-and-loose with checked math here without
                // spurious test failures since read_chunks returning Some(_)
                // guarantees us that computing the end of the last chunk does
                // not overflow.
                Ok(offsets) => {
                    let offsets: Vec<_> = offsets.collect();
                    assert!(!offsets.is_empty());

                    let first_chunk = *offsets.first().unwrap();
                    let last_chunk = *offsets.last().unwrap();
                    let read_len = (last_chunk + chunk_size) - first_chunk;
                    // total read length should be at least as large as the requested length
                    assert!(read_len >= len, "read length is too short");
                    // the maximum read length should allow for extending the start of the read
                    // to the beginning of a chunk, a section of chunks that fully overlap the range,
                    // and then touching the very beginning of a final chunk.
                    //
                    // have to check for overflow here since the max allowable value is larger
                    if let Some(max) =
                        (chunk_size + (len / chunk_size) * chunk_size).checked_add(chunk_size)
                    {
                        assert!(read_len <= max, "read_length is too long");
                    }
                }
                // we overflowed! make sure it's reasonable
                Err(_) => assert!(
                    offset
                        .checked_add(len)
                        .and_then(|n| n.checked_add(chunk_size))
                        .and_then(|n| n.checked_add(chunk_size))
                        .is_none(),
                    "should only return None on offset + len + 2 * chunk_size overflow"
                ),
            };

            Ok(())
        });
    }

    #[test]
    fn fuzz_readahead_offsets() {
        arbtest(|u| {
            let chunk_size: u64 = u.arbitrary::<NonZero<_>>()?.get();
            let offset: u64 = u.arbitrary()?;
            let readahead: u64 = u.arbitrary()?;
            let len: u64 = u.arbitrary::<NonZero<_>>()?.get();

            // compute both read and readahead offsets so we can compare them
            let read_offsets = read_offsets(offset, len, chunk_size);
            let readahead_offsets = readahead_offsets(offset, len, readahead, chunk_size);

            match (read_offsets, readahead_offsets) {
                // we can actually validate things
                (Ok(read_offsets), Ok(readahead_offsets)) => {
                    let read_offsets: Vec<_> = read_offsets.collect();
                    let readahead_offsets: Vec<_> = readahead_offsets.collect();

                    assert!(!read_offsets.is_empty());
                    assert!(!readahead_offsets.is_empty());

                    assert!(
                        read_offsets.last().unwrap() < readahead_offsets.first().unwrap(),
                        "readahead offsets should all be larger than read offsets"
                    );

                    let readahead_len = (*readahead_offsets.last().unwrap() + chunk_size)
                        - *readahead_offsets.first().unwrap();
                    // total readahead length should be at least as much as requested
                    assert!(readahead_len >= readahead);
                    // total readahead length should be no more than to the end of the next chunk
                    assert!(readahead_len <= (readahead / chunk_size) * chunk_size + chunk_size);
                }
                // validate that overflow should be happening
                (Err(_), Err(_)) | (Ok(_), Err(_)) => assert!(
                    offset
                        .checked_add(len)
                        .and_then(|n| n.checked_add(readahead))
                        .and_then(|n| n.checked_add(chunk_size))
                        .and_then(|n| n.checked_add(chunk_size))
                        .is_none(),
                    "should only overflow if offset + len + readahead + chunk_size overflows",
                ),
                // uhhhhhhhhhhh
                (Err(_), Ok(_)) => panic!("read overflowed but readahead did not. this is a bug"),
            }

            Ok(())
        });
    }

    #[tokio::test]
    async fn test_readahead() {
        let key = Arc::new(object_store::path::Path::from("some-key"));
        let storage = storage_with_data(&key, Bytes::from(vec![0u8; 1 << 10])).await;

        // volume store fetches/caches 10 byte chunks with a readahead size of
        // 40 bytes (4 chunks)
        let chunk_size = 10;
        let cache = ChunkCache::new(1024, 10, storage, ReadAheadPolicy { size: 40 });
        let read_offset = 123;
        let read_len = 234;
        let last_cached_byte =
            // align to the start of the last chunk
            (read_offset + read_len + cache.inner.readahead_policy.size) / chunk_size * chunk_size
            // and find the last byte
                + (chunk_size - 1);

        let _ = cache.get_at(key.clone(), 123, 234).await.unwrap();

        // every byte in the readahead-window is cached
        for offset in read_offset..=last_cached_byte {
            assert!(
                cache.inner.is_cached(key.clone(), offset),
                "offset should be cached: {offset}"
            );
        }
        // bytes beyond are not cached
        for offset in (last_cached_byte + 1)..(last_cached_byte + chunk_size) {
            assert!(
                !cache.inner.is_cached(key.clone(), offset,),
                "offset should not be cached: {offset}"
            );
        }
    }

    #[tokio::test]
    async fn test_bad_get_removes_entry() {
        let key: Arc<object_store::path::Path> = Arc::new("some-key".into());
        let storage = crate::storage::Storage::new_in_memory().unwrap();
        let cache = ChunkCache::new(10, 10, storage, ReadAheadPolicy { size: 123 });
        let res = cache.get_at(key.clone(), 10, 37).await;
        assert!(res.is_err());
        assert!(!cache.inner.is_cached(key, 10));
    }
}
