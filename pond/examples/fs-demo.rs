use std::str::FromStr;

use pond::{OpenOptions, Version, Volume};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

static CONTENT: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris in risus lacus. Vivamus ac accumsan.";

/// Creates Volume that lives on our local filesystem and writes + reads a string from it.
#[tokio::main]
async fn main() -> pond::Result<()> {
    // Creating a brand new Volume that lives locally on disk. If you wanted to persist it on S3, you
    // could use something like s3://my-bucket/path/to/my/new/volume. If you wanted to persist it
    // in-memory (making it ephemeral), you could do something like memory://.
    let volume = Volume::new("my-local-volume", Default::default()).await?;

    // Let's try to read a file
    let file = volume
        .open("/path/to/something", OpenOptions::default())
        .await;

    // But it's a new volume so the file doesn't exist yet. the directory structure also doesn't
    // exist yet!
    assert_eq!(file.err().unwrap().kind(), pond::ErrorKind::NotFound);

    // Create the directory up to where we want to create the file
    let _ = volume.create_dir_all("/path/to")?.await?;

    // Now let's open it and write to it.
    let mut file = volume
        .open(
            "/path/to/something",
            OpenOptions::default().write(true).create(true),
        )
        .await?;
    let _ = file.write_all(CONTENT.as_bytes()).await;

    // Cool, it's there
    let _ = file.seek(std::io::SeekFrom::Start(0)).await;
    let mut s = String::new();
    let _ = file.read_to_string(&mut s).await;
    assert_eq!(s, CONTENT);
    std::mem::drop(file);

    // Let's open it again and double check
    let mut file = volume
        .open("/path/to/something", OpenOptions::default())
        .await?;
    let mut s = String::new();
    let _ = file.read_to_string(&mut s).await;
    assert_eq!(s, CONTENT);
    std::mem::drop(file);

    // Now let's commit it to make it durable then drop it.
    volume.commit("my-first-volume")?.await?;
    std::mem::drop(volume);

    // let's load back up that volume we just created and committed!
    let volume = Volume::load(
        "my-local-volume",
        Some(Version::from_str("my-first-volume").unwrap()),
        Default::default(),
    )
    .await?;

    // Let's open up the old file again and triple check
    let mut file = volume
        .open("/path/to/something", OpenOptions::default())
        .await?;
    let mut s = String::new();
    let _ = file.read_to_string(&mut s).await;
    assert_eq!(s, CONTENT);
    std::mem::drop(file);
    std::mem::drop(volume);

    // if you wanted to keep that volume around to look at it, comment the line below out.
    let _ = std::fs::remove_file("my-local-volume");

    Ok(())
}
