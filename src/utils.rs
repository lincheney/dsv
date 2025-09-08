use anyhow::{Result, Context};
use std::default::Default;
use std::borrow::Cow;
use bstr::{BStr, ByteVec};

pub fn chain_errors<T: Default, I: IntoIterator<Item=Result<T>>>(results: I) -> Result<T> {
    let mut result = Ok(Default::default());
    for err in results {
        if result.is_ok() {
            result = result.and(err);
        } else if let Err(e) = err {
            result = result.context(e);
        }
    }
    result
}

pub fn unescape_str<'a>(val: &'a str) -> Cow<'a, BStr> {
    if val.contains('\\') {
        Cow::Owned(Vec::unescape_bytes(val).into())
    } else {
        Cow::Borrowed(val.as_bytes().into())
    }
}
