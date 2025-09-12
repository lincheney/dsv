use anyhow::Result;
use crate::base;
use bstr::{BString};
use clap::{Parser, ArgAction};
use super::py;
use std::ffi::{CString, CStr};
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
    #[arg(short = 'p', long, conflicts_with_all = ["regex", "complement"], help = "grouping fields are python scripts")]
    python_fields: bool,
}

#[derive(Parser, Default)]
#[command(about = "aggregate rows using python")]
pub struct Opts {
    #[command(flatten)]
    common: py::CommonOpts,
    #[command(flatten)]
    other: OtherOpts,
}

pub struct Handler {
    inner: py::Handler,
    opts: OtherOpts,
    postprocess: python::Object,
    default_key: python::Object,
    column_slicer: ColumnSlicer,
    header: Option<Vec<BString>>,
    rows: Vec<Vec<BString>>,
    field_scripts: Vec<python::Object>,
}

unsafe impl Send for Handler {}

const POSTPROCESS: &CStr = /*python*/ c"
if not isinstance(result, BaseTable) or (isinstance(result, Proxy) and (result.__is_column__() or result.__is_row__())):
    if not isinstance(result, dict):
        result = {default_key: result}
    result = {**current_key, **result}
";

impl Handler {
    pub fn new(opts: Opts, base: &mut base::Base) -> Result<Self> {
        let mut py_opts = py::Opts::default();
        py_opts.common = opts.common;
        let inner = py::Handler::new(py_opts, base)?;
        let column_slicer = ColumnSlicer::new(&opts.other.fields, opts.other.regex);

        let py = inner.py.acquire_gil();
        let postprocess = py.compile_code_cstr(POSTPROCESS, None, python::StartToken::File)?;
        let default_key = py.to_str(inner.opts.common.script.last().unwrap()).unwrap();

        let mut field_scripts = vec![];
        if opts.other.python_fields {
            for f in &opts.other.fields {
                field_scripts.push(py.compile_code(f, None, python::StartToken::Eval)?);
            }
        }

        drop(py);

        Ok(Self{
            inner,
            opts: opts.other,
            postprocess,
            column_slicer,
            default_key,
            field_scripts,
            header: None,
            rows: vec![],
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
        self.rows.push(row);
        Ok(false)
    }

    fn on_eof(mut self, base: &mut base::Base) -> Result<bool> {

        let py = self.inner.py.acquire_gil();
        let missing = py.to_bytes(b"".into()).unwrap();

        let (header, keys): (_, Vec<_>) = if self.opts.python_fields {
            let mut keys = vec![vec![]; self.rows.len()];

            self.inner.count = self.rows.len();
            for (i, script) in self.field_scripts.iter().enumerate() {
                let rows = py.list_from_iter(self.rows.iter().map(|row| self.inner.row_to_py(&py, row))).unwrap();
                let result = self.inner.run_python(&py, base, rows, [], *script, None)?;

                let result = if let Some(result) = result {
                    Some(py.try_iter(result)?)
                } else {
                    None
                };
                let result = result.into_iter().flatten().chain(std::iter::repeat(missing));

                for (k, val) in keys.iter_mut().zip(result) {
                    k.resize(i, py.get_none());
                    k.push(val);
                }
            }

            let keys = keys.into_iter()
                .map(|key| py.tuple_from_iter(key).unwrap())
                .collect();

            let header = self.opts.fields;
            (header.into_iter().map(|h| h.into()).collect(), keys)

        } else {
            let header = self.column_slicer.slice_with(
                self.header.as_ref().unwrap_or(&vec![]),
                self.opts.complement,
                Some(|i| format!("{i}").into()),
            );

            let keys = self.rows.iter()
                .map(|row| self.column_slicer.slice(row, self.opts.complement, true))
                .map(|key| py.tuple_from_iter(key.iter().map(|col| self.inner.bytes_to_py(&py, col.as_ref())) ).unwrap())
                .collect();

            (header, keys)
        };

        let groups = py.empty_dict().unwrap();
        for (key, row) in keys.into_iter().zip(self.rows.iter()) {
            let group = py.dict_get(groups, key).unwrap_or_else(|| {
                let group = py.empty_list(0).unwrap();
                py.dict_set(groups, key, group);
                group
            });
            let row = self.inner.row_to_py(&py, row);
            py.list_append(group, row);
        }

        let header: Vec<_> = header.into_iter().map(|h| CString::new(h).unwrap()).collect();
        for (key, group) in py.dict_iter(groups) {
            let current_key = py.empty_dict().unwrap();
            py.dict_extend(current_key, header.iter().map(|h| h.as_ref()).zip(py.try_iter(key)?));

            self.inner.count = py.list_len(group);
            let result = self.inner.run_python(&py, base, group, [(c"K", current_key)], self.inner.code, self.inner.prelude)?;

            let result = if self.inner.inner.expr && let Some(result) = result {
                py.dict_clear(self.inner.inner.locals);
                py.dict_extend(self.inner.inner.locals, [
                    (c"result", result),
                    (c"default_key", self.default_key),
                    (c"current_key", current_key),
                ]);

                py.exec_code(self.postprocess, self.inner.globals.as_ptr(), self.inner.inner.locals.as_ptr())?;
                py.dict_get_string(self.inner.inner.locals, c"result")
            } else {
                result
            };

            if let Some(result) = result && self.inner.inner.handle_result(&py, base, result)? {
                return Ok(true)
            }
        }
        base.on_eof()
    }

}
