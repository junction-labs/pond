use std::process::Command;

fn main() {
    git_rev_hash();
}

fn git_rev_hash() {
    let mut cmd = Command::new("git");
    let output = cmd.args(["rev-parse", "--short=10", "HEAD"]).output();

    match output {
        Ok(output) => {
            let git_sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if git_sha.is_empty() {
                println!("cargo:warning=empty output from `git rev-parse`");
                return;
            }
            println!("cargo:rustc-env=POND_GIT_SHA={git_sha}");
        }
        Err(e) => println!("cargo:warning=failed to run `git rev-parse`: {e}"),
    }
}
