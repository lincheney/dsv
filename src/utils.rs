use anyhow::{Result, Context};
use std::default::Default;

pub fn chain_errors<T: Default, I: Iterator<Item=Result<T>>>(results: I) -> Result<T> {
    let mut result = Ok(Default::default());
    for err in results {
        if let Err(e) = err {
            if result.is_ok() {
                result = Err(e);
            } else {
                result = result.context(e);
            }
        }
    }
    result
}
