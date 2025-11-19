use std::time::Instant;

use futures::StreamExt;
use pond::{CacheConfig, Volume};
use tokio::io::AsyncReadExt;

/// Walk the entire volume and read all files.
#[tokio::main]
async fn main() -> pond::Result<()> {
    let volume = Volume::load(
        "s3://junctionlabs/pond/volume/berkeley_gnm_recon",
        None,
        CacheConfig {
            max_cache_size_bytes: 10 * (1 << 30),
            chunk_size_bytes: 8 * (1 << 20),
            readahead_size_bytes: 32 * (1 << 20),
        },
    )
    .await?;

    let mut stack = vec![String::from("/")];
    let mut bytes_read = 0;
    let now = Instant::now();
    while let Some(dir) = stack.pop() {
        let mut stream = volume.read_dir(dir)?;
        while let Some(Ok(entry)) = stream.next().await {
            match entry.file_type() {
                pond::FileType::Regular => {
                    let mut file = volume.open(entry.path(), Default::default()).await?;
                    let mut buf = Vec::new();
                    let _ = file.read_to_end(&mut buf).await;
                    assert_eq!(buf.len() as u64, entry.attr().size);
                    bytes_read += entry.attr().size;
                }
                pond::FileType::Directory => {
                    stack.push(entry.path().to_string());
                }
            }
        }
    }

    println!(
        "read {bytes_read} bytes in {} seconds",
        now.elapsed().as_secs_f64()
    );

    Ok(())
}
