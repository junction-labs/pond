use anyhow::{Context, Result};
use bytesize::ByteSize;
use cfs_core::{
    ByteRange, File, Location,
    read::{ChunkCache, ReadAheadPolicy},
};
use clap::{Parser, value_parser};
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser, Debug)]
struct Args {
    /// S3 bucket that holds the object
    #[arg(long)]
    bucket: String,

    /// Object key within the bucket
    #[arg(long)]
    key: String,

    /// Maximum size of the chunk cache.
    #[clap(long, default_value = "2GiB", value_parser = value_parser!(ByteSize))]
    max_cache_size: ByteSize,

    /// The size of the chunk we fetch from object storage in a single request. It's also the size
    /// of the buffers we store within a single cache entry.
    #[clap(long, default_value = "8MiB", value_parser = value_parser!(ByteSize))]
    chunk_size: ByteSize,

    /// Size of readahead. If you're reading a file at byte 0, we will pre-fetch the bytes up to `readahead_size` in parallel.
    #[clap(long, default_value = "32MiB", value_parser = value_parser!(ByteSize))]
    readahead_size: ByteSize,

    /// Size of the local buffer to pull data into.
    #[arg(long, default_value = "1mb", value_parser = value_parser!(ByteSize))]
    local_buffer_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let chunk_store = Arc::new(ChunkCache::new(
        args.max_cache_size.as_u64(),
        args.chunk_size.as_u64(),
        ReadAheadPolicy {
            size: args.readahead_size.as_u64(),
        },
    ));

    let object_store = build_object_store(&args).context("failed to build S3 object store")?;
    let path = Path::from(args.key.as_str());
    let meta = object_store
        .head(&path)
        .await
        .with_context(|| format!("failed to fetch object metadata for {}", path))?;
    let filesize = meta.size;
    let location = Location::ObjectStorage {
        bucket: args.bucket.clone(),
        key: args.key.clone(),
    };
    let byte_range = ByteRange {
        offset: 0,
        len: filesize,
    };

    read(&args, chunk_store, location, byte_range).await?;

    Ok(())
}

fn build_object_store(args: &Args) -> Result<Arc<dyn ObjectStore>> {
    let builder = AmazonS3Builder::from_env()
        .with_bucket_name(&args.bucket)
        .with_region("us-east-2");

    let store = builder.build().context("failed to finalize S3 builder")?;
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    Ok(store)
}

async fn read(
    args: &Args,
    chunk_store: Arc<ChunkCache>,
    location: Location,
    byte_range: ByteRange,
) -> Result<()> {
    let mut file = File::new(chunk_store, location, byte_range);
    let mut buffer = vec![0u8; args.local_buffer_size];
    let mut stdout = tokio::io::stdout();

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        stdout.write_all(&buffer[..bytes_read]).await?;
    }
    stdout.flush().await?;

    Ok(())
}
