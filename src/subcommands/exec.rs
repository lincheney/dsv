use crate::base;
use bstr::{BString, BStr};
use clap::{Parser};
use std::ptr::{NonNull, null_mut};
use std::ffi::{c_void, CString, CStr};
use libloading::os::unix::{Library, Symbol, RTLD_GLOBAL, RTLD_LAZY};
use anyhow::Result;
use once_cell::sync::Lazy;
use std::os::raw::c_char;

type Pointer = *mut c_void;
pub type PyObject = NonNull<c_void>;

macro_rules! define_python_lib {
    ($($name:ident: $fn:ty,)*) => {
        #[allow(non_snake_case)]
        struct PythonLib {
            $(
                $name: Symbol<$fn>,
            )*
        }

        impl PythonLib {
            fn new(lib: &Library) -> Result<Self> {
                let lib = Self {
                    $(
                        $name: unsafe { lib.get(concat!(stringify!($name), "\0").as_bytes()) }?,
                    )*
                };
                unsafe{
                    (lib.Py_InitializeEx)(0);
                    // release gil in main thread
                    (lib.PyEval_SaveThread)();
                }
                Ok(lib)
            }
        }
    };
}

define_python_lib!(
    Py_InitializeEx: unsafe extern "C" fn(i32),
    // Py_Finalize: unsafe extern "C" fn(),
    PyDict_GetItemString: unsafe extern "C" fn(Pointer, *const c_char) -> Pointer,
    PyDict_GetItem: unsafe extern "C" fn(Pointer, Pointer) -> Pointer,
    PyObject_GetAttr: unsafe extern "C" fn(Pointer, Pointer) -> Pointer,
    PyDict_New: unsafe extern "C" fn() -> Pointer,
    PyList_New: unsafe extern "C" fn(isize) -> Pointer,
    PyList_SetItem: unsafe extern "C" fn(Pointer, isize, Pointer) -> i32,
    PyList_Append: unsafe extern "C" fn(Pointer, Pointer) -> i32,
    PyList_Clear: unsafe extern "C" fn(Pointer) -> i32,
    PyLong_FromSize_t: unsafe extern "C" fn(usize) -> Pointer,
    PyLong_FromSsize_t: unsafe extern "C" fn(isize) -> Pointer,
    PyFloat_FromDouble: unsafe extern "C" fn(f64) -> Pointer,
    // PyRun_SimpleString: unsafe extern "C" fn(*const c_char) -> i32,
    // PyRun_String: unsafe extern "C" fn(*const c_char, i32, Pointer, Pointer) -> Pointer,
    PyBytes_FromStringAndSize: unsafe extern "C" fn(*const c_char, isize) -> Pointer,
    PyUnicode_FromStringAndSize: unsafe extern "C" fn(*const c_char, isize) -> Pointer,
    // Py_IncRef: unsafe extern "C" fn(Pointer),
    Py_DecRef: unsafe extern "C" fn(Pointer),
    PyObject_IsTrue: unsafe extern "C" fn(Pointer) -> i32,
    // PyImport_AddModule: unsafe extern "C" fn(*const c_char) -> Pointer,
    PyDict_SetItem: unsafe extern "C" fn(Pointer, Pointer, Pointer) -> i32,
    PyDict_SetItemString: unsafe extern "C" fn(Pointer, *const c_char, Pointer) -> i32,
    PyDict_Clear: unsafe extern "C" fn(Pointer),
    // PyModule_GetDict: unsafe extern "C" fn(Pointer) -> Pointer,
    PyObject_Vectorcall: unsafe extern "C" fn(Pointer, *const Pointer, usize, Pointer) -> Pointer,
    Py_CompileString: unsafe extern "C" fn(*const c_char, *const c_char, i32) -> Pointer,
    PyEval_EvalCode: unsafe extern "C" fn(Pointer, Pointer, Pointer) -> Pointer,
    PyObject_Str: unsafe extern "C" fn(Pointer) -> Pointer,
    PyUnicode_AsUTF8AndSize: unsafe extern "C" fn(Pointer, *mut isize) -> *const c_char,
    PyObject_GetIter: unsafe extern "C" fn(Pointer) -> Pointer,
    PyIter_Next: unsafe extern "C" fn(Pointer) -> Pointer,
    PyBytes_AsString: unsafe extern "C" fn(Pointer) -> *const c_char,
    PyBytes_Size: unsafe extern "C" fn(Pointer) -> isize,
    PyEval_GetBuiltins: unsafe extern "C" fn() -> Pointer,
    PyObject_IsInstance: unsafe extern "C" fn(Pointer, Pointer) -> i32,
    PyErr_Clear: unsafe extern "C" fn(),
    // PyErr_Print: unsafe extern "C" fn(),
    PyGILState_Ensure: unsafe extern "C" fn() -> i32,
    PyGILState_Release: unsafe extern "C" fn(i32),
    PyEval_SaveThread: unsafe extern "C" fn() -> Pointer,
);

static LIBPYTHON: Lazy<Library> = Lazy::new(|| {
    unsafe{ Library::open(Some("libpython3.so"), RTLD_GLOBAL | RTLD_LAZY) }.unwrap()
} );
static PYTHON: Lazy<PythonLib> = Lazy::new(|| {
    PythonLib::new(&LIBPYTHON).unwrap()
});

#[allow(dead_code)]
pub enum PyStartToken {
    Single = 256,
    File = 257,
    Eval = 258,
    FuncType = 345,
}

pub struct Python {
    none: Pointer,
    bytes: PyObject,
}

impl Python {
    fn new() -> Self {
        unsafe{
            let none = *LIBPYTHON.get(c"_Py_NoneStruct".to_bytes()).unwrap();
            let bytes = NonNull::new(*LIBPYTHON.get(c"PyBytes_Type".to_bytes()).unwrap()).unwrap();
            Self{
                none,
                bytes,
            }
        }
    }

    pub fn acquire_gil(&self) -> PythonHandle {
        let state = unsafe{ (PYTHON.PyGILState_Ensure)() };
        PythonHandle{ inner: self, state }
    }
}

pub struct PythonHandle<'a> {
    inner: &'a Python,
    state: i32,
}

impl<'a> Drop for PythonHandle<'a> {
    fn drop(&mut self) {
        unsafe{
            (PYTHON.PyGILState_Release)(self.state);
        }
    }
}

impl<'a> PythonHandle<'a> {
    fn is_none(&self, obj: PyObject) -> bool {
        obj.as_ptr() == self.inner.none
    }

    pub fn is_truthy(&self, obj: PyObject) -> bool {
        unsafe{
            (PYTHON.PyObject_IsTrue)(obj.as_ptr()) != 0
        }
    }

    pub fn isinstance(&self, obj: PyObject, typ: PyObject) -> bool {
        unsafe{
            (PYTHON.PyObject_IsInstance)(obj.as_ptr(), typ.as_ptr()) != 0
        }
    }

    pub fn get_builtin(&self, key: PyObject) -> Option<PyObject> {
        unsafe{
            let builtins = NonNull::new((PYTHON.PyEval_GetBuiltins)()).unwrap();
            self.dict_get(builtins, key)
        }
    }

    fn convert_py_to_bytes(&self, obj: PyObject) -> &BStr {
        unsafe{
            let mut size = 0isize;

            let bytes = if self.isinstance(obj, self.inner.bytes) {
                size = (PYTHON.PyBytes_Size)(obj.as_ptr());
                (PYTHON.PyBytes_AsString)(obj.as_ptr())
            } else {
                let obj = (PYTHON.PyObject_Str)(obj.as_ptr());
                debug_assert!(!obj.is_null());
                let bytes = (PYTHON.PyUnicode_AsUTF8AndSize)(obj, &mut size as _);
                debug_assert!(!bytes.is_null());
                bytes
            };

            std::slice::from_raw_parts(bytes as *const u8, size as _).into()
        }
    }

    // fn incref(&self, value: PyObject) {
        // unsafe{
            // (PYTHON.Py_IncRef)(value.as_ptr());
        // }
    // }

    fn to_float(&self, value: f64) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyFloat_FromDouble)(value))
        }
    }

    fn to_int(&self, value: isize) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyLong_FromSsize_t)(value))
        }
    }

    fn to_uint(&self, value: usize) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyLong_FromSize_t)(value))
        }
    }

    pub fn to_str(&self, string: &str) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyUnicode_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    fn to_bytes(&self, string: &BStr) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyBytes_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    fn empty_list(&self, size: usize) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyList_New)(size as _))
        }
    }

    fn list_append(&self, list: PyObject, item: PyObject) {
        unsafe{
            let result = (PYTHON.PyList_Append)(list.as_ptr(), item.as_ptr());
            debug_assert!(result == 0);
        }
    }

    fn list_clear(&self, list: PyObject) {
        unsafe{
            (PYTHON.PyList_Clear)(list.as_ptr());
        }
    }

    fn list_from_iter<I: Iterator<Item=PyObject> + ExactSizeIterator>(&self, iter: I) -> Option<PyObject> {
        unsafe{
            let list = self.empty_list(iter.len())?;
            for (i, value) in iter.enumerate() {
                let result = (PYTHON.PyList_SetItem)(list.as_ptr(), i as _, value.as_ptr());
                debug_assert!(result == 0);
            }
            Some(list)
        }
    }

    pub fn empty_dict(&self) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyDict_New)())
        }
    }

    pub fn dict_clear(&self, dict: PyObject) {
        unsafe{
            (PYTHON.PyDict_Clear)(dict.as_ptr());
        }
    }

    pub fn dict_get(&self, dict: PyObject, key: PyObject) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyDict_GetItem)(dict.as_ptr(), key.as_ptr()))
        }
    }

    pub fn dict_get_string(&self, dict: PyObject, key: &CStr) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyDict_GetItemString)(dict.as_ptr(), key.as_ptr() as _))
        }
    }

    pub fn dict_set(&self, dict: PyObject, key: PyObject, value: PyObject) {
        unsafe{
            let result = (PYTHON.PyDict_SetItem)(dict.as_ptr(), key.as_ptr(), value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn dict_set_string(&self, dict: PyObject, key: &CStr, value: PyObject) {
        unsafe{
            let result = (PYTHON.PyDict_SetItemString)(dict.as_ptr(), key.as_ptr() as _, value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    fn getattr(&self, obj: PyObject, key: PyObject) -> Option<PyObject> {
        unsafe{
            NonNull::new((PYTHON.PyObject_GetAttr)(obj.as_ptr(), key.as_ptr()))
        }
    }

    pub fn call_func(&self, func: PyObject, args: &[PyObject]) -> Option<PyObject> {
        unsafe{
            (PYTHON.PyErr_Clear)();
            let args: &[Pointer] = std::mem::transmute(args);
            NonNull::new((PYTHON.PyObject_Vectorcall)(func.as_ptr(), args.as_ptr(), args.len(), null_mut()))
        }
    }

    pub fn compile_code(&self, code: &str, start: PyStartToken) -> Option<PyObject> {
        self.compile_code_cstr(&CString::new(code).unwrap(), start)
    }

    pub fn compile_code_cstr(&self, code: &CStr, start: PyStartToken) -> Option<PyObject> {
        unsafe{
            (PYTHON.PyErr_Clear)();
            let code = (PYTHON.Py_CompileString)(
                code.as_ptr(),
                CString::new("<string>").unwrap().as_ptr(),
                start as _,
            );
            NonNull::new(code)
        }
    }

    pub fn exec_code(&self, code: PyObject, globals: Pointer, locals: Pointer) -> Option<PyObject> {
        unsafe{
            (PYTHON.PyErr_Clear)();
            NonNull::new((PYTHON.PyEval_EvalCode)(code.as_ptr(), globals, locals))
        }
    }

    fn exec_cstr(&self, code: &CStr, globals: Pointer, locals: Pointer) {
        let code = self.compile_code_cstr(code, PyStartToken::File).unwrap();
        self.exec_code(code, globals, locals);
    }

    fn exec(&self, code: &str, globals: Pointer, locals: Pointer) {
        self.exec_cstr(&CString::new(code).unwrap(), globals, locals)
    }

    fn iter(&self, obj: PyObject) -> impl Iterator<Item=PyObject> {
        let iter = unsafe{ (PYTHON.PyObject_GetIter)(obj.as_ptr()) };
        debug_assert!(!iter.is_null());

        let mut item: Option<PyObject> = None;
        std::iter::from_fn(move || unsafe {
            if let Some(item) = item {
                (PYTHON.Py_DecRef)(item.as_ptr());
            }
            item = NonNull::new((PYTHON.PyIter_Next)(iter));
            item
        })
    }
}

const TABLE_SCRIPT: &CStr = unsafe{ CStr::from_bytes_with_nul_unchecked(concat!(include_str!("../../_dsv/_table.py"), "\0").as_bytes()) };

#[derive(Parser, Default)]
pub struct CommonOpts {
    #[arg(required = true, help = "python statements to run")]
    pub script: Vec<String>,
    #[arg(long, default_value = "X", help = "python variable to use to refer to the data")]
    var: String,
    #[arg(short = 'b', long, help = "do not auto convert data to int, str etc, treat everything as bytes")]
    bytes: bool,
}

#[derive(Parser, Default)]
#[command(about = "run python on each row")]
pub struct Opts {
    #[arg(short = 'S', long, help = "run python on one row at a time")]
    no_slurp: bool,
    #[command(flatten)]
    pub common: CommonOpts,
}

pub struct Handler {
    pub opts: Opts,
    rows: Vec<Vec<BString>>,
    pub expr: bool,
    pub count: usize,
    got_header: bool,

    pub var_name: PyObject,
    pub locals: PyObject,
    pub globals: PyObject,
    pub py: Python,
    prelude: Option<PyObject>,
    code: PyObject,
    table_cls: PyObject,
    pub vec_cls: PyObject,
    convert_to_table_fn: PyObject,
    header: PyObject,
    header_numbers: PyObject,
}

unsafe impl Send for Handler {}

impl Handler {
    pub fn new(opts: Opts) -> Self {
        let python = Python::new();

        let py = python.acquire_gil();
        // let main = py.add_module("__main__").unwrap();

        let globals = py.empty_dict().unwrap();
        let locals = py.empty_dict().unwrap();
        py.exec_cstr(TABLE_SCRIPT, globals.as_ptr(), globals.as_ptr());

        let table_cls = py.dict_get_string(globals, c"Table").unwrap();
        let vec_cls = py.dict_get_string(globals, c"Vec").unwrap();
        let convert_to_table_fn = py.dict_get_string(globals, c"convert_to_table").unwrap();

        let (last, rest) = opts.common.script.split_last().unwrap();
        let prelude = if rest.is_empty() {
            None
        } else {
            Some(py.compile_code(&rest.join("\n"), PyStartToken::File).unwrap())
        };

        let code = py.compile_code(last, PyStartToken::Eval);
        let expr = code.is_some();
        let code = code.or_else(|| py.compile_code(last, PyStartToken::File)).unwrap();
        let var_name = py.to_str(&opts.common.var).unwrap();
        let header = py.empty_list(0).unwrap();
        let header_numbers = py.empty_dict().unwrap();

        drop(py);

        Self {
            opts,
            rows: vec![],
            count: 0,
            got_header: false,

            var_name,
            locals,
            globals,
            expr,
            prelude,
            code,
            table_cls,
            vec_cls,
            convert_to_table_fn,
            header,
            header_numbers,
            py: python,
        }
    }
}

impl Handler {
    pub fn bytes_to_py(&self, py: &PythonHandle, bytes: &BStr) -> PyObject {
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

    fn row_to_py(&self, py: &PythonHandle, row: &[BString]) -> PyObject {
        py.list_from_iter(row.iter().map(|col| self.bytes_to_py(py, col.as_ref())) ).unwrap()
    }

    pub fn run_python<T, I>(
        &self,
        rows: I,
        vars: &[(&CStr, PyObject)],
    ) -> Option<PyObject>
    where
        T: AsRef<[BString]>,
        I: ExactSizeIterator + Iterator<Item=T>,
    {

        let py = self.py.acquire_gil();
        let rows = py.list_from_iter(rows.map(|row| self.row_to_py(&py, row.as_ref()))).unwrap();
        let table = py.call_func(self.table_cls, &[rows, self.header_numbers]).unwrap();

        py.dict_clear(self.locals);
        for (k, v) in vars {
            py.dict_set_string(self.locals, k, *v);
        }
        py.dict_set_string(self.locals, c"Vec", self.vec_cls);
        py.dict_set_string(self.locals, c"H", self.header);
        py.dict_set_string(self.locals, c"N", py.to_uint(self.count).unwrap());
        py.dict_set(self.locals, self.var_name, table);

        if let Some(prelude) = self.prelude {
            py.exec_code(prelude, self.locals.as_ptr(), self.locals.as_ptr());
        }
        let result = py.exec_code(self.code, self.locals.as_ptr(), self.locals.as_ptr());
        if self.expr {
            result
        } else {
            py.dict_get(self.locals, self.var_name)
        }
    }

    pub fn handle_result(&mut self, base: &mut base::Base, result: PyObject) -> bool {
        let py = self.py.acquire_gil();

        if !py.is_none(result) {
            let table = py.call_func(self.convert_to_table_fn, &[result]).unwrap();
            if !py.is_none(table) {
                let header = py.getattr(table, py.to_str("__headers__").unwrap()).unwrap();

                if !self.got_header && !py.is_none(header) {
                    self.got_header = true;
                    let header = py.iter(header).map(|x| py.convert_py_to_bytes(x).to_owned()).collect();
                    if base.on_header(header) {
                        return true
                    }
                }

                let rows = py.getattr(table, py.to_str("__data__").unwrap()).unwrap();
                if !py.is_none(rows) {
                    for row in py.iter(rows) {
                        let row = py.iter(row).map(|x| py.convert_py_to_bytes(x).to_owned()).collect();
                        if base.on_row(row) {
                            return true
                        }
                    }
                }
            } else if self.expr {
                let bytes = py.convert_py_to_bytes(result);
                if base.write_raw(bytes.to_owned()) {
                    return true
                }
            } else {
                py.dict_set(self.locals, py.to_str("X").unwrap(), result);
                py.exec("raise ValueError(X)", self.locals.as_ptr(), self.locals.as_ptr());
            }
        }
        false
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

    fn on_header(&mut self, _base: &mut base::Base, header: Vec<BString>) -> bool {
        self.process_header(&header);
        false
    }

    fn on_row(&mut self, base: &mut base::Base, row: Vec<BString>) -> bool {
        if self.opts.no_slurp {
            self.count += 1;
            let result = self.run_python([row].iter(), &[]);
            if let Some(result) = result {
                self.handle_result(base, result)
            } else {
                false
            }
        } else {
            self.rows.push(row);
            false
        }
    }

    fn on_eof(&mut self, base: &mut base::Base) -> bool {
        if !self.opts.no_slurp {
            let result = self.run_python(self.rows.iter(), &[]);
            if let Some(result) = result && self.handle_result(base, result) {
                return true
            }
        }
        base.on_eof()
    }

}
