use anyhow::Result;
use crate::base;
use bstr::{BString, ByteVec};
use clap::{Parser};
use super::exec;
use crate::python;

#[derive(Parser, Default)]
#[command(about = "filter rows using python")]
pub struct Opts {
    #[command(flatten)]
    common: exec::CommonOpts,
    #[arg(long, help = "print both matching and non-matching lines")]
    passthru: bool,
}

pub struct Handler {
    passthru: bool,
    colour: bool,
    inner: exec::Handler,
    all: Option<python::Object>,
}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        let mut exec_opts = exec::Opts::default();
        exec_opts.common = opts.common;
        let inner = exec::Handler::new(exec_opts);

        Self{
            passthru: opts.passthru,
            colour: false,
            inner,
            all: None,
        }
    }
}

unsafe impl Send for Handler {}

impl base::Processor for Handler {

    fn process_opts(&mut self, opts: &mut base::BaseOptions, is_tty: bool) {
        self._process_opts(opts, is_tty);
        self.colour = opts.colour == base::AutoChoices::Always;
    }

    fn on_header(&mut self, base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.inner.process_header(&header);
        base.on_header(header)
    }

    fn on_row(&mut self, base: &mut base::Base, mut row: Vec<BString>) -> Result<bool> {
        self.inner.count += 1;
        let result = self.inner.run_python([&row].iter(), &[]);
        let result = if let Some(mut result) = result {
            let py = self.inner.py.acquire_gil();
            if py.isinstance(result, self.inner.vec_cls) {
                let all = self.all.get_or_insert_with(|| {
                    py.get_builtin(py.to_str("all").unwrap()).unwrap()
                });
                result = py.call_func(*all, &[result]).unwrap();
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
