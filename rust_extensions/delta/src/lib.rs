use cxx::CxxString;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn delta(s: &CxxString) -> usize;
    }
}

pub fn delta(s: &CxxString) -> usize {
    s.len() + 1
}
