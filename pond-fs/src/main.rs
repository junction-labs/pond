#![deny(clippy::unwrap_used)]

use clap::Parser;

use pond_fs::*;

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
        if args.backtrace {
            eprintln!("{e:?}");
        } else {
            eprintln!("{e}");
        }
        std::process::exit(-1);
    }

    Ok(())
}

fn version() -> anyhow::Result<()> {
    let pkg_version = option_env!("CARGO_PKG_VERSION").unwrap_or("dev");
    let git_sha = option_env!("POND_GIT_SHA");

    match git_sha {
        Some(sha) => println!("pond {pkg_version} ({sha})"),
        None => println!("pond {pkg_version}"),
    }

    Ok(())
}
