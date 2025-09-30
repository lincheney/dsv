use anyhow::{Result, Context};
use std::default::Default;
use std::borrow::Cow;
use bstr::{BStr, ByteVec};

pub fn chain_errors<T: Default, I: IntoIterator<Item=Result<T>>>(results: I) -> Result<T> {
    let mut results = results.into_iter();
    let mut result = results.next().unwrap_or_else(|| Ok(Default::default()));
    for err in results {
        // skip breaks
        if let Err(e) = Break::is_break(err) {
            if result.is_ok() {
                result = Err(e);
            } else {
                result = result.context(e);
            }
        }
    }
    result
}

pub fn unescape_str(val: &'_ str) -> Cow<'_, BStr> {
    if val.contains('\\') {
        Cow::Owned(Vec::unescape_bytes(val).into())
    } else {
        Cow::Borrowed(val.as_bytes().into())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Break;

impl Break {
    pub fn when<E: From<Self>>(item: bool) -> Result<(), E> {
        if item {
            Self.to_err()?;
        }
        Ok(())
    }

    pub fn is_break<T>(result: Result<T>) -> Result<bool> {
        match result {
            Ok(_) => Ok(false),
            Err(e) if e.is::<Self>() => Ok(true),
            Err(e) => Err(e),
        }
    }

    pub fn to_err<T, E: From<Self>>(self) -> Result<T, E> {
        Err(self.into())
    }
}

impl std::error::Error for Break {}
impl std::fmt::Display for Break {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "Break")
    }
}

impl<T> From<Break> for Result<T, Break> {
    fn from(item: Break) -> Self {
        Err(item)
    }
}

pub type MaybeBreak = Result<(), Break>;
