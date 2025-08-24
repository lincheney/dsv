use anyhow::Result;
use crate::base;
use bstr::{BString};
use std::collections::{HashMap, hash_map::Entry};
use clap::{Parser, ArgAction};
use super::exec;
use std::ffi::{CStr};
use crate::column_slicer::ColumnSlicer;
use crate::python;

#[derive(Parser, Default)]
struct OtherOpts {
    #[arg(short = 'k', long, help = "sort based only on these fields")]
    fields: Vec<String>,
    #[arg(short = 'x', long, action = ArgAction::SetTrue, help = "exclude, rather than include, field names")]
    complement: bool,
    #[arg(long, action = ArgAction::SetTrue, help = "treat fields as regexes")]
    regex: bool,
}

#[derive(Parser, Default)]
#[command(about = "aggregate rows using python")]
pub struct Opts {
    #[command(flatten)]
    common: exec::CommonOpts,
    #[command(flatten)]
    other: OtherOpts,
}

pub struct Handler {
    inner: exec::Handler,
    opts: OtherOpts,
    postprocess: python::Object,
    default_key: python::Object,
    column_slicer: ColumnSlicer,
    groups: HashMap<Vec<BString>, Vec<Vec<BString>>>,
    header: Option<Vec<BString>>,
}

unsafe impl Send for Handler {}

const POSTPROCESS: &CStr = c"
if not isinstance(result, BaseTable) or (isinstance(result, Proxy) and (result.__is_column__() or result.__is_row__())):
    if not isinstance(result, dict):
        result = {default_key: result}
    result = {**current_key, **result}
";

impl Handler {
    pub fn new(opts: Opts) -> Result<Self> {
        let mut exec_opts = exec::Opts::default();
        exec_opts.common = opts.common;
        let inner = exec::Handler::new(exec_opts)?;
        let column_slicer = ColumnSlicer::new(&opts.other.fields, opts.other.regex);

        let py = inner.py.acquire_gil();
        let postprocess = py.compile_code_cstr(POSTPROCESS, python::StartToken::File).unwrap();
        let default_key = py.to_str(inner.opts.common.script.last().unwrap()).unwrap();

        drop(py);

        Ok(Self{
            inner,
            opts: opts.other,
            postprocess,
            column_slicer,
            default_key,
            header: None,
            groups: HashMap::new(),
        })
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.column_slicer.make_header_map(&header);
        self.inner.process_header(&header);
        self.header = Some(header);
        Ok(false)
    }

    fn on_row(&mut self, _base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        let key = self.column_slicer.slice(&row, self.opts.complement, true);
        match self.groups.entry(key) {
            Entry::Occupied(mut entry) => { entry.get_mut().push(row); },
            Entry::Vacant(entry) => { entry.insert(vec![row]); },
        }
        Ok(false)
    }

    fn on_eof(&mut self, base: &mut base::Base) -> Result<bool> {
        let mut header: Option<Vec<_>> = None;
        for (key, group) in self.groups.iter() {
            let py = self.inner.py.acquire_gil();
            let header = header.get_or_insert_with(|| {
                let header = self.column_slicer.slice_with(
                    self.header.as_ref().unwrap_or(&vec![]),
                    self.opts.complement,
                    Some(|i| format!("{i}").into()),
                );
                header.iter().map(|col| self.inner.bytes_to_py(&py, col.as_ref())).collect()
            });
            let current_key = py.empty_dict().unwrap();
            for (k, v) in header.iter().zip(key) {
                py.dict_set(current_key, *k, self.inner.bytes_to_py(&py, v.as_ref()));
            }
            drop(py);

            let result = self.inner.run_python(group.iter(), &[(c"current_key", current_key)])?;
            let py = self.inner.py.acquire_gil();
            let result = if self.inner.expr && let Some(result) = result {
                py.dict_clear(self.inner.locals);
                py.dict_set_string(self.inner.locals, c"result", result);
                py.dict_set_string(self.inner.locals, c"default_key", self.default_key);
                py.dict_set_string(self.inner.locals, c"current_key", current_key);

                py.exec_code(self.postprocess, self.inner.globals.as_ptr(), self.inner.locals.as_ptr())?;
                py.dict_get_string(self.inner.locals, c"result")
            } else {
                result
            };
            drop(py);

            if let Some(result) = result {
                if self.inner.handle_result(base, result)? {
                    return Ok(true)
                }
            }
        }
        base.on_eof()
    }

}
