#![deny(clippy::unwrap_used)]

use clap::Parser;

use cfs_fuse::*;

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let res = match args.cmd {
        Cmd::Version => version(),
        Cmd::Dump { volume, version } => dump(runtime, volume, version),
        Cmd::Pack { dir, to, version } => pack(runtime, dir, to, version),
        Cmd::List { volume } => list(runtime, volume),
        Cmd::Mount(mount_args) => mount(runtime, mount_args),
    };

    if let Err(e) = res {
        eprintln!("{e:?}");
        std::process::exit(-1);
    }

    Ok(())
}

fn version() -> anyhow::Result<()> {
    let pkg_version = option_env!("CARGO_PKG_VERSION").unwrap_or("dev");
    let git_sha = option_env!("CFS_GIT_SHA");

    match git_sha {
        Some(sha) => println!("cfs {pkg_version} ({sha})"),
        None => println!("cfs {pkg_version}"),
    }

    Ok(())
}
