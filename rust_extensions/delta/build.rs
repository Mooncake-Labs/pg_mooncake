fn main() {
    cxx_build::bridge("src/lib.rs").compile("cxxbridge");

    println!("cargo:rerun-if-changed=src/lib.rs");
}
