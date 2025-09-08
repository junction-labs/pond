use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use xshell::{Shell, cmd};

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Generate,
}

fn main() -> anyhow::Result<()> {
    let project_root = project_root();
    std::env::set_current_dir(&project_root)?;

    let sh = Shell::new()?;
    let args = Args::parse();

    match args.cmd {
        Cmd::Generate => generate_srcs(&sh)?,
    }

    Ok(())
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}

fn generate_srcs(sh: &Shell) -> anyhow::Result<()> {
    let fbs: Result<Vec<_>, _> = glob::glob("cfs-core/fbs/*.fbs")
        .expect("invalid glob pattern")
        .collect();
    let fbs = fbs?;

    cmd!(
        sh,
        "flatc -o cfs-core/src/ --rust --gen-all --filename-suffix '.fbs' {fbs...}"
    )
    .run()
    .map_err(|e| e.into())
}
