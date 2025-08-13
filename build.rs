fn main() {
    println!("cargo::rerun-if-changed=src/");
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "macos" {
        println!("cargo::rustc-link-arg-cdylib=-Wl,-undefined,dynamic_lookup");
    }
}
