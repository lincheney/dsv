use anyhow::Result;
use crate::base;
use bstr::{BString, ByteVec};
use clap::{Parser};
use super::py;
use crate::python;

#[derive(Parser, Default)]
#[command(about = "filter rows using python")]
pub struct Opts {
    #[command(flatten)]
    common: py::CommonOpts,
    #[arg(long, help = "print both matching and non-matching lines")]
    passthru: bool,
}

pub struct Handler {
    passthru: bool,
    colour: bool,
    inner: py::Handler,
    all: python::Object,
}

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base) -> Result<Self> {
        let mut py_opts = py::Opts::default();
        py_opts.common = opts.common;
        if py_opts.common.ignore_errors {
            py_opts.common.remove_errors = true;
        }
        let inner = py::Handler::new(py_opts, base)?;
        let all = {
            let py = inner.py.acquire_gil();
            py.get_builtin(py.to_str("all").unwrap())
                .ok_or_else(|| anyhow::anyhow!("could not get builtin `all`"))?
        };

        Ok(Self{
            passthru: opts.passthru,
            colour: base.opts.colour == base::AutoChoices::Always,
            inner,
            all,
        })
    }
}

unsafe impl Send for Handler {}

impl base::Processor for Handler {
    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.inner.process_header(&header);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<bool> {
        self.inner.count += 1;
        let py = self.inner.py.acquire_gil();
        let rows = py.list_from_iter([self.inner.row_to_py(&py, &row)]).unwrap();
        let result = self.inner.run_python(&py, rows, [], self.inner.code, self.inner.prelude)?;
        let result = if let Some(mut result) = result {
            if py.isinstance(result, self.inner.vec_cls) {
                result = py.call_func(self.all, &[result])?;
            }
            py.is_truthy(result)
        } else {
            false
        };

        // colour rows
        if self.passthru && self.colour {
            if let Some(first) = row.first_mut() {
                first.insert_str(0, if result { b"\x1b[1m" } else { b"\x1b[2m" });
            }
            if let Some(last) = row.last_mut() {
                last.push_str(base::RESET_COLOUR);
            }
        }

        if result || self.passthru {
            base.on_row(row)?;
        }
        Ok(false)
    }

}
