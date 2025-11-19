use pond::{OpenOptions, Volume};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

static CONTENT: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris in risus lacus. Vivamus ac accumsan.";

/// Creates an in-memory (ephemeral) Volume and writes + reads a string from it.
#[tokio::main]
async fn main() -> pond::Result<()> {
    // Creating a brand new Volume that lives in-memory. If you wanted to persist it on S3, you
    // could use something like s3://my-bucket/path/to/my/new/volume. If you wanted to persist it
    // locally on disk, you could do something like file://path/to/my/new/volume (or even shorter,
    // /path/to/my/new/volume).
    let volume = Volume::new("memory://hello-world", Default::default()).await?;

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

    Ok(())
}
