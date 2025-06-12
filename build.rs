fn main() {
    println!("cargo::rerun-if-changed=src/");
    println!("cargo::rerun-if-changed=pg_duckdb/libpg_duckdb.a");
    println!(
        "cargo::rerun-if-changed=pg_duckdb/third_party/duckdb/build/release/libduckdb_bundle.a"
    );

    println!("cargo::rustc-link-search=native=pg_duckdb");
    println!("cargo::rustc-link-lib=static=pg_duckdb");

    println!("cargo::rustc-link-search=native=pg_duckdb/third_party/duckdb/build/release");
    println!("cargo::rustc-link-lib=static=duckdb_bundle");

    println!("cargo::rustc-link-lib=dylib=crypto");
    println!("cargo::rustc-link-lib=dylib=ssl");
    println!("cargo::rustc-link-lib=dylib=stdc++");

    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "macos" {
        println!("cargo::rustc-link-arg-cdylib=-Wl,-undefined,dynamic_lookup");
    }
}
