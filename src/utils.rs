use anyhow::{Result, Context};
use std::default::Default;

pub fn chain_errors<T: Default, I: Iterator<Item=Result<T>>>(results: I) -> Result<T> {
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
