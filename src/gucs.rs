use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pub(crate) static MOONLINK_URI: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) fn init() {
    GucRegistry::define_string_guc(
        c"mooncake.moonlink_uri",
        c"moonlink URI",
        c"moonlink URI",
        &MOONLINK_URI,
        GucContext::Userset,
        GucFlags::default(),
    );
}
