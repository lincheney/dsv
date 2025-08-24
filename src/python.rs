use anyhow::{Result, anyhow};
use bstr::{BStr};
use std::ptr::{NonNull, null_mut};
use std::ffi::{c_void, CString, CStr};
use libloading::os::unix::{Library, Symbol, RTLD_GLOBAL, RTLD_LAZY};
use once_cell::sync::{OnceCell};
use std::os::raw::c_char;

type Pointer = *mut c_void;
struct SendPointer(Object);
unsafe impl Send for SendPointer {}
unsafe impl Sync for SendPointer {}
pub type Object = NonNull<c_void>;

static PYTHON: OnceCell<Result<PythonLib, libloading::Error>> = OnceCell::new();

macro_rules! define_python_lib {
    ($($name:ident: $fn:ty,)*) => {
        #[allow(non_snake_case)]
        struct PythonLib {
            #[allow(dead_code)]
            lib: Library,
            $(
                $name: Symbol<$fn>,
            )*
        }

        impl PythonLib {
            fn get(name: Option<&str>) -> Result<&'static Self> {
                let py = PYTHON.get_or_init(|| {
                    #[allow(non_snake_case)]
                    unsafe {
                        let lib = Library::open(Some(name.unwrap_or("libpython3.so")), RTLD_GLOBAL | RTLD_LAZY)?;
                        $(
                        let $name = lib.get(concat!(stringify!($name), "\0").as_bytes())?;
                        )*
                        let py = Self { lib, $($name,)* };
                        (py.Py_InitializeEx)(0);
                        // release gil in main thread
                        (py.PyEval_SaveThread)();
                        Ok(py)
                    }
                });

                match &*py {
                    Ok(py) => Ok(py),
                    Err(e) => return Err(anyhow!("{e}")),
                }
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
    PyErr_Fetch: unsafe extern "C" fn(*mut Pointer, *mut Pointer, *mut Pointer),
    PyErr_SetExcInfo: unsafe extern "C" fn(Pointer, Pointer, Pointer),
    _Py_NoneStruct: SendPointer,
    PyBytes_Type: SendPointer,
);

#[allow(dead_code)]
pub enum StartToken {
    Single = 256,
    File = 257,
    Eval = 258,
    FuncType = 345,
}

pub struct Python {
    py: &'static PythonLib,
    get_exception: Object,
}

fn _compile_code_cstr(py: &PythonLib, code: &CStr, start: StartToken) -> Option<Object> {
    unsafe{
        (py.PyErr_Clear)();
        let code = (py.Py_CompileString)(
            code.as_ptr(),
            CString::new("<string>").unwrap().as_ptr(),
            start as _,
        );
        NonNull::new(code)
    }
}

impl Python {
    pub fn new(name: Option<&str>) -> Result<Self> {
        let py = PythonLib::get(name)?;
        unsafe{
            let state = (py.PyGILState_Ensure)();
            let get_exception = _compile_code_cstr(py, c"__import__('traceback').format_exc()", StartToken::Eval).unwrap();
            (py.PyGILState_Release)(state);

            Ok(Self{
                py,
                get_exception,
            })
        }
    }

    pub fn acquire_gil(&self) -> GilHandle {
        let state = unsafe{ (self.py.PyGILState_Ensure)() };
        GilHandle{ inner: self, state, py: self.py }
    }
}

pub struct GilHandle<'a> {
    inner: &'a Python,
    py: &'a PythonLib,
    state: i32,
}

impl<'a> Drop for GilHandle<'a> {
    fn drop(&mut self) {
        unsafe{
            (self.py.PyGILState_Release)(self.state);
        }
    }
}

impl<'a> GilHandle<'a> {
    pub fn is_none(&self, obj: Object) -> bool {
        obj == self.py._Py_NoneStruct.0
    }

    pub fn is_truthy(&self, obj: Object) -> bool {
        unsafe{
            (self.py.PyObject_IsTrue)(obj.as_ptr()) != 0
        }
    }

    pub fn isinstance(&self, obj: Object, typ: Object) -> bool {
        unsafe{
            (self.py.PyObject_IsInstance)(obj.as_ptr(), typ.as_ptr()) != 0
        }
    }

    pub fn get_builtin(&self, key: Object) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyEval_GetBuiltins)()).and_then(|builtins| self.dict_get(builtins, key))
        }
    }

    pub fn convert_py_to_bytes(&self, obj: Object) -> &BStr {
        unsafe{
            let mut size = 0isize;

            let bytes = if self.isinstance(obj, self.py.PyBytes_Type.0) {
                size = (self.py.PyBytes_Size)(obj.as_ptr());
                (self.py.PyBytes_AsString)(obj.as_ptr())
            } else {
                let obj = (self.py.PyObject_Str)(obj.as_ptr());
                debug_assert!(!obj.is_null());
                let bytes = (self.py.PyUnicode_AsUTF8AndSize)(obj, &mut size as _);
                debug_assert!(!bytes.is_null());
                bytes
            };

            std::slice::from_raw_parts(bytes as *const u8, size as _).into()
        }
    }

    // fn incref(&self, value: Object) {
        // unsafe{
            // (self.py.Py_IncRef)(value.as_ptr());
        // }
    // }

    pub fn to_float(&self, value: f64) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyFloat_FromDouble)(value))
        }
    }

    pub fn to_int(&self, value: isize) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyLong_FromSsize_t)(value))
        }
    }

    pub fn to_uint(&self, value: usize) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyLong_FromSize_t)(value))
        }
    }

    pub fn to_str(&self, string: &str) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyUnicode_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    pub fn to_bytes(&self, string: &BStr) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyBytes_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    pub fn empty_list(&self, size: usize) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyList_New)(size as _))
        }
    }

    pub fn list_append(&self, list: Object, item: Object) {
        unsafe{
            let result = (self.py.PyList_Append)(list.as_ptr(), item.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn list_clear(&self, list: Object) {
        unsafe{
            (self.py.PyList_Clear)(list.as_ptr());
        }
    }

    pub fn list_from_iter<I: Iterator<Item=Object> + ExactSizeIterator>(&self, iter: I) -> Option<Object> {
        unsafe{
            let list = self.empty_list(iter.len())?;
            for (i, value) in iter.enumerate() {
                let result = (self.py.PyList_SetItem)(list.as_ptr(), i as _, value.as_ptr());
                debug_assert!(result == 0);
            }
            Some(list)
        }
    }

    pub fn empty_dict(&self) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyDict_New)())
        }
    }

    pub fn dict_clear(&self, dict: Object) {
        unsafe{
            (self.py.PyDict_Clear)(dict.as_ptr());
        }
    }

    pub fn dict_get(&self, dict: Object, key: Object) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyDict_GetItem)(dict.as_ptr(), key.as_ptr()))
        }
    }

    pub fn dict_get_string(&self, dict: Object, key: &CStr) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyDict_GetItemString)(dict.as_ptr(), key.as_ptr() as _))
        }
    }

    pub fn dict_set(&self, dict: Object, key: Object, value: Object) {
        unsafe{
            let result = (self.py.PyDict_SetItem)(dict.as_ptr(), key.as_ptr(), value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn dict_set_string(&self, dict: Object, key: &CStr, value: Object) {
        unsafe{
            let result = (self.py.PyDict_SetItemString)(dict.as_ptr(), key.as_ptr() as _, value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn getattr(&self, obj: Object, key: Object) -> Option<Object> {
        unsafe{
            NonNull::new((self.py.PyObject_GetAttr)(obj.as_ptr(), key.as_ptr()))
        }
    }

    pub fn call_func(&self, func: Object, args: &[Object]) -> Result<Object> {
        unsafe{
            (self.py.PyErr_Clear)();
            let args: &[Pointer] = std::mem::transmute(args);
            NonNull::new((self.py.PyObject_Vectorcall)(func.as_ptr(), args.as_ptr(), args.len(), null_mut()))
                .ok_or_else(|| self.get_exception())
        }
    }

    pub fn compile_code(&self, code: &str, start: StartToken) -> Result<Object> {
        self.compile_code_cstr(&CString::new(code).unwrap(), start)
    }

    fn get_exception(&self) -> anyhow::Error {
        let dict = self.empty_dict().unwrap();

        let mut typ: Pointer = null_mut();
        let mut value: Pointer = null_mut();
        let mut tb: Pointer = null_mut();
        unsafe {
            (self.py.PyErr_Fetch)(&mut typ as _, &mut value as _, &mut tb as _);
            (self.py.PyErr_SetExcInfo)(typ, value, tb);
            let exc = self._exec_code(self.inner.get_exception, dict.as_ptr(), dict.as_ptr()).unwrap();
            // then clear it
            (self.py.PyErr_SetExcInfo)(null_mut(), null_mut(), null_mut());
            anyhow!(self.convert_py_to_bytes(exc).to_owned())
        }
    }


    fn _exec_code(&self, code: Object, globals: Pointer, locals: Pointer) -> Option<Object> {
        unsafe{
            (self.py.PyErr_Clear)();
            NonNull::new((self.py.PyEval_EvalCode)(code.as_ptr(), globals, locals))
        }
    }

    pub fn compile_code_cstr(&self, code: &CStr, start: StartToken) -> Result<Object> {
        _compile_code_cstr(self.py, code, start).ok_or_else(|| self.get_exception())
    }

    pub fn exec_code(&self, code: Object, globals: Pointer, locals: Pointer) -> Result<Object> {
        self._exec_code(code, globals, locals).ok_or_else(|| self.get_exception())
    }

    pub fn exec(&self, code: &CStr, globals: Pointer, locals: Pointer) -> Result<()> {
        let code = self.compile_code_cstr(code, StartToken::File)?;
        self.exec_code(code, globals, locals)?;
        Ok(())
    }

    pub fn iter(&self, obj: Object) -> impl Iterator<Item=Object> {
        let iter = unsafe{ (self.py.PyObject_GetIter)(obj.as_ptr()) };
        debug_assert!(!iter.is_null());

        let mut item: Option<Object> = None;
        std::iter::from_fn(move || unsafe {
            if let Some(item) = item {
                (self.py.Py_DecRef)(item.as_ptr());
            }
            item = NonNull::new((self.py.PyIter_Next)(iter));
            item
        })
    }
}
