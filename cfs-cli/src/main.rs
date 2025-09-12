use std::str::FromStr;

use clap::{Parser, Subcommand};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    List { url: Option<String> },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = cfs_client::Client::new();

    match args.cmd {
        Cmd::List { url } => list(&client, url.as_ref()).await,
    };
}

async fn list(client: &cfs_client::Client, url: Option<impl AsRef<str>>) {
    match url {
        // list all volumes
        None => {
            let volumes = client.volumes().await.unwrap();
            for volume in volumes {
                println!("{volume}");
            }
        }
        // list versions of a volume
        Some(url) => {
            let url: http::Uri = url.as_ref().parse().unwrap();
            if url.scheme() != Some(&http::uri::Scheme::from_str("cfs").unwrap()) {
                panic!("not a cool url");
            }

            if let Some(volume) = url.authority() {
                let volume = volume.host();

                let versions = client.versions(volume).await.unwrap();
                for version in versions {
                    println!("{version}");
                }
            }
        }
    }
}
