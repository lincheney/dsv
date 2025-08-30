use anyhow::Result;
use crate::base;
use bstr::{BString, BStr};
use clap::{Parser};
use std::ffi::{CStr};
use crate::python;

const TABLE_SCRIPT: &CStr = unsafe{ CStr::from_bytes_with_nul_unchecked(concat!(include_str!("../../_dsv/_table.py"), "\0").as_bytes()) };

#[derive(Parser, Default)]
pub struct CommonOpts {
    #[arg(required = true, help = "python statements to run")]
    pub script: Vec<String>,
    #[arg(long, default_value = "X", help = "python variable to use to refer to the data")]
    var: String,
    #[arg(short = 'b', long, help = "do not auto convert data to int, str etc, treat everything as bytes")]
    bytes: bool,
    #[arg(short = 'I', long, overrides_with = "remove_errors", help = "do not abort on python errors")]
    pub ignore_errors: bool,
    #[arg(short = 'E', long, help = "remove rows on python errors")]
    pub remove_errors: bool,
    #[arg(short = 'q', long, help = "do not print errors")]
    quiet: bool,
    #[arg(long, help = "do not auto generate NA for invalid operations")]
    no_na: bool,
    #[arg(long, long, help = "path to libpython3.so")]
    libpython: Option<String>,
}

#[derive(Parser, Default)]
#[command(about = "run python on each row")]
pub struct Opts {
    #[arg(short = 'S', long, help = "run python on one row at a time")]
    no_slurp: bool,
    #[command(flatten)]
    pub common: CommonOpts,
}

pub struct InnerHandler {
    pub expr: bool,
    got_header: bool,
    convert_to_table_fn: python::Object,
    pub locals: python::Object,
}

pub struct Handler {
    pub opts: Opts,
    rows: Vec<Vec<BString>>,
    pub count: usize,
    pub inner: InnerHandler,

    pub var_name: python::Object,
    pub globals: python::Object,
    pub py: python::Python,
    pub prelude: Option<python::Object>,
    pub code: python::Object,
    table_cls: python::Object,
    pub vec_cls: python::Object,
    header: python::Object,
    header_numbers: python::Object,
}

impl Handler {
    pub fn new(opts: Opts, _base: &mut base::Base, _is_tty: bool) -> Result<Self> {
        let python = python::Python::new(opts.common.libpython.as_ref().map(|x| x.as_ref()))?;

        let py = python.acquire_gil();

        let globals = py.empty_dict().unwrap();
        let locals = py.empty_dict().unwrap();
        py.dict_set_string(globals, c"__builtins__", py.get_builtin_dict().unwrap());
        py.exec(TABLE_SCRIPT, Some(c"_table.py"), globals.as_ptr(), globals.as_ptr()).unwrap();
        if opts.common.no_na {
            py.dict_set_string(globals, c"NA", py.get_none());
        }

        let table_cls = py.dict_get_string(globals, c"Table").unwrap();
        let vec_cls = py.dict_get_string(globals, if opts.common.no_na { c"NoNaVec" } else { c"Vec" }).unwrap();
        let convert_to_table_fn = py.dict_get_string(globals, c"convert_to_table").unwrap();

        let (last, rest) = opts.common.script.split_last().unwrap();
        let prelude = if rest.is_empty() {
            None
        } else {
            Some(py.compile_code(&rest.join("\n"), None, python::StartToken::File)?)
        };

        let code = py.compile_code(last, None, python::StartToken::Eval);
        let expr = code.is_ok();
        let code = code.or_else(|_| py.compile_code(last, None, python::StartToken::File))?;
        let var_name = py.to_str(&opts.common.var).unwrap();
        let header = py.empty_list(0).unwrap();
        let header_numbers = py.empty_dict().unwrap();

        drop(py);

        Ok(Self {
            opts,
            rows: vec![],
            count: 0,
            inner: InnerHandler{
                got_header: false,
                convert_to_table_fn,
                expr,
                locals,
            },

            var_name,
            globals,
            prelude,
            code,
            table_cls,
            vec_cls,
            header,
            header_numbers,
            py: python,
        })
    }
}

impl Handler {
    pub fn bytes_to_py(&self, py: &python::GilHandle, bytes: &BStr) -> python::Object {
        if !self.opts.common.bytes && let Ok(string) = std::str::from_utf8(bytes) {
            if let Ok(val) = string.parse::<isize>() {
                py.to_int(val).unwrap()
            } else if let Ok(val) = string.parse::<f64>() {
                py.to_float(val).unwrap()
            } else {
                py.to_str(string).unwrap()
            }
        } else {
            py.to_bytes(bytes).unwrap()
        }
    }

    pub fn row_to_py(&self, py: &python::GilHandle, row: &[BString]) -> python::Object {
        py.list_from_iter(row.iter().map(|col| self.bytes_to_py(py, col.as_ref())) ).unwrap()
    }

    pub fn run_python<'a, I: IntoIterator<Item=(&'a CStr, python::Object)>>(
        &self,
        py: &python::GilHandle,
        rows: python::Object,
        vars: I,
        code: python::Object,
        prelude: Option<python::Object>,
    ) -> Result<Option<python::Object>> {

        let table = py.call_func(self.table_cls, &[rows, self.header_numbers, py.to_bool(!self.opts.common.no_na).unwrap()])?;

        py.dict_clear(self.inner.locals);
        py.dict_extend(self.inner.locals, vars);
        py.dict_extend(self.inner.locals, [
            (c"Vec", self.vec_cls),
            (c"H", self.header),
            (c"N", py.to_uint(self.count).unwrap()),
        ]);
        py.dict_set(self.inner.locals, self.var_name, table);

        if let Some(prelude) = prelude {
            py.exec_code(prelude, self.inner.locals.as_ptr(), self.inner.locals.as_ptr())?;
        }
        let result = py.exec_code(code, self.inner.locals.as_ptr(), self.inner.locals.as_ptr());

        match result {
            Ok(result) => {
                if self.inner.expr {
                    Ok(Some(result))
                } else {
                    Ok(py.dict_get(self.inner.locals, self.var_name))
                }
            },
            Err(e) if !self.opts.common.ignore_errors && !self.opts.common.quiet => {
                Err(e)
            }
            Err(e) => {
                if ! self.opts.common.quiet {
                    eprintln!("{e}");
                }
                if self.opts.common.remove_errors || (self.opts.common.ignore_errors && self.inner.expr) {
                    Ok(None)
                } else {
                    Ok(py.dict_get(self.inner.locals, self.var_name))
                }
            },
        }
    }

    pub fn process_header(&mut self, header: &[BString]) {
        let py = self.py.acquire_gil();
        py.dict_clear(self.header_numbers);
        py.list_clear(self.header);

        for (i, k) in header.iter().enumerate() {
            let k = py.to_bytes(k.as_ref()).unwrap();
            py.dict_set(self.header_numbers, k, py.to_uint(i).unwrap());
            py.list_append(self.header, k);
        }
    }
}

impl base::Processor for Handler {

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> Result<bool> {
        self.process_header(&header);
        Ok(false)
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> Result<bool> {
        if self.opts.no_slurp {
            self.count += 1;
            let py = self.py.acquire_gil();
            let rows = py.list_from_iter([self.row_to_py(&py, &row)]).unwrap();
            let result = self.run_python(&py, rows, [], self.code, self.prelude)?;
            if let Some(result) = result {
                self.inner.handle_result(&py, base, result)
            } else {
                Ok(false)
            }
        } else {
            self.rows.push(row);
            Ok(false)
        }
    }

    fn on_eof(mut self, base: &mut base::Base) -> Result<bool> {
        if !self.opts.no_slurp {
            let py = self.py.acquire_gil();
            let rows = py.list_from_iter(self.rows.iter().map(|row| self.row_to_py(&py, row))).unwrap();
            let result = self.run_python(&py, rows, [], self.code, self.prelude)?;
            if let Some(result) = result && self.inner.handle_result(&py, base, result)? {
                return Ok(true)
            }
        }
        base.on_eof()
    }

}

impl InnerHandler {
    pub fn handle_result(
        &mut self,
        py: &python::GilHandle,
        base: &mut base::Base,
        result: python::Object,
    ) -> Result<bool> {

        if !py.is_none(result) {
            let table = py.call_func(self.convert_to_table_fn, &[result, py.get_none()])?;
            if !py.is_none(table) {
                let header = py.getattr_string(table, c"__headers__");

                if !self.got_header && let Some(header) = header && !py.is_none(header) {
                    self.got_header = true;
                    let mut new_header = vec![];
                    for x in py.try_iter(header)? {
                        new_header.push(py.convert_py_to_bytes(x)?.to_owned());
                    }
                    if base.on_header(new_header)? {
                        return Ok(true)
                    }
                }

                let rows = py.getattr_string(table, c"__data__");
                if let Some(rows) = rows && !py.is_none(rows) {
                    for row in py.try_iter(rows)? {
                        let mut new_row = vec![];
                        for x in py.try_iter(row)? {
                            new_row.push(py.convert_py_to_bytes(x)?.to_owned());
                        }
                        if base.on_row(new_row)? {
                            return Ok(true)
                        }
                    }
                }
            } else if self.expr {
                let bytes = py.convert_py_to_bytes(result)?;
                if base.write_raw(bytes.to_owned()) {
                    return Ok(true)
                }
            } else {
                py.dict_set(self.locals, py.to_str("X").unwrap(), result);
                py.exec(c"raise ValueError(X)", None, self.locals.as_ptr(), self.locals.as_ptr()).unwrap();
            }
        }
        Ok(false)
    }

}
