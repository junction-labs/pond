use bytesize::ByteSize;
use cfs_core::{
    File, Ino, VolumeMetadata,
    read::{ChunkCache, ReadAheadPolicy},
};
use clap::{Parser, value_parser};
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser, Debug)]
struct Args {
    #[command(flatten)]
    volume: VolumeArgs,

    #[command(flatten)]
    read_behavior: ReadBehaviorArgs,

    #[clap(long, default_value_t = false)]
    stdout: bool,

    #[clap(long, default_value_t = false)]
    cpu_profile: bool,
}

#[derive(clap::Args, Debug)]
struct VolumeArgs {
    /// The path to the volume. Can be a local path or an S3 path.
    path: String,

    /// S3 region (only applicable when the volume path is an S3 path).
    #[arg(long, default_value = "us-east-2")]
    region: String,
}

#[derive(clap::Args, Debug)]
struct ReadBehaviorArgs {
    /// Maximum size of the chunk cache.
    #[clap(long, default_value = "10GiB", value_parser = value_parser!(ByteSize))]
    max_cache_size: ByteSize,

    /// The size of the chunk we fetch from object storage in a single request. It's also the size
    /// of the buffers we store within a single cache entry.
    #[clap(long, default_value = "8MiB", value_parser = value_parser!(ByteSize))]
    chunk_size: ByteSize,

    /// Size of readahead. If you're reading a file at byte 0, we will pre-fetch the bytes up to `readahead_size` in parallel.
    #[clap(long, default_value = "32MiB", value_parser = value_parser!(ByteSize))]
    readahead_size: ByteSize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let volume = {
        let url: url::Url = args.volume.path.parse()?;
        let (object_store, path) =
            object_store::parse_url_opts(&url, [("region", args.volume.region)])?;
        let path = path.child("version");
        let path = {
            // list all versions, and use the lexographically maximum version
            let result = object_store.list_with_delimiter(Some(&path)).await?;
            let path = result
                .common_prefixes
                .into_iter()
                .max()
                .ok_or(anyhow::anyhow!("could not find a volume version to mount. consider explicitly specifying --version"))?;
            path.child("vol.cfs.bin")
        };

        let bytes = object_store.get(&path).await?.bytes().await?;
        VolumeMetadata::from_bytes(&bytes)?
    };

    let chunk_store = Arc::new(ChunkCache::new(
        args.read_behavior.max_cache_size.as_u64(),
        args.read_behavior.chunk_size.as_u64(),
        ReadAheadPolicy {
            size: args.read_behavior.readahead_size.as_u64(),
        },
    ));

    let guard = if args.cpu_profile {
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(10000)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()?,
        )
    } else {
        None
    };

    let mut stdout = tokio::io::stdout();
    let mut buf = Vec::new();
    for (_, _, attr) in volume.walk(Ino::Root)? {
        if attr.is_file() && attr.size > 0 {
            let (location, byte_range) = volume.location(attr.ino).unwrap();
            let mut file = File::new(chunk_store.clone(), location.clone(), *byte_range);
            let n = file.read_to_end(&mut buf).await?;
            assert_eq!(n, byte_range.len as usize);
            if args.stdout {
                stdout.write_all(&buf[..n]).await?;
            }
        }
    }
    if args.stdout {
        stdout.flush().await?;
    }

    if let Some(guard) = guard
        && let Ok(report) = guard.report().build()
    {
        let file = std::fs::File::create("flamegraph.svg")?;
        report.flamegraph(file)?;
    };

    Ok(())
}
