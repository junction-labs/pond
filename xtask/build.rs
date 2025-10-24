fn main() {
    // get the host platform's target triple and make it available to rustc.
    // for whatever reason, cargo does not already export this and we use it
    // to pick which flatc to install.
    if let Ok(triple) = std::env::var("HOST") {
        println!("cargo:rustc-env=HOST_PLATFORM={triple}");
    }
}
