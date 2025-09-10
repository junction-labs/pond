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
        Cmd::Generate => {
            generate_srcs(&sh, "cfs-core")?;
            generate_srcs(&sh, "cfs-md")?;
        }
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

fn generate_srcs(sh: &Shell, crate_name: &str) -> anyhow::Result<()> {
    let fbs = glob::glob(&format!("{crate_name}/fbs/*.fbs"))
        .expect("invalid glob pattern")
        .collect::<Result<Vec<_>, _>>()?;

    cmd!(
        sh,
        "flatc -o {crate_name}/src/ --rust --gen-all --filename-suffix '.fbs' {fbs...}"
    )
    .run()
    .map_err(|e| e.into())
}
