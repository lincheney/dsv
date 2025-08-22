use anyhow::Result;
use bstr::{BStr};
use std::ptr::{NonNull, null_mut};
use std::ffi::{c_void, CString, CStr};
use libloading::os::unix::{Library, Symbol, RTLD_GLOBAL, RTLD_LAZY};
use once_cell::sync::Lazy;
use std::os::raw::c_char;

type Pointer = *mut c_void;
pub type Object = NonNull<c_void>;

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
pub enum StartToken {
    Single = 256,
    File = 257,
    Eval = 258,
    FuncType = 345,
}

pub struct Python {
    none: Pointer,
    bytes: Object,
}

impl Python {
    pub fn new() -> Self {
        unsafe{
            let none = *LIBPYTHON.get(c"_Py_NoneStruct".to_bytes()).unwrap();
            let bytes = NonNull::new(*LIBPYTHON.get(c"PyBytes_Type".to_bytes()).unwrap()).unwrap();
            Self{
                none,
                bytes,
            }
        }
    }

    pub fn acquire_gil(&self) -> GilHandle {
        let state = unsafe{ (PYTHON.PyGILState_Ensure)() };
        GilHandle{ inner: self, state }
    }
}

pub struct GilHandle<'a> {
    inner: &'a Python,
    state: i32,
}

impl<'a> Drop for GilHandle<'a> {
    fn drop(&mut self) {
        unsafe{
            (PYTHON.PyGILState_Release)(self.state);
        }
    }
}

impl<'a> GilHandle<'a> {
    pub fn is_none(&self, obj: Object) -> bool {
        obj.as_ptr() == self.inner.none
    }

    pub fn is_truthy(&self, obj: Object) -> bool {
        unsafe{
            (PYTHON.PyObject_IsTrue)(obj.as_ptr()) != 0
        }
    }

    pub fn isinstance(&self, obj: Object, typ: Object) -> bool {
        unsafe{
            (PYTHON.PyObject_IsInstance)(obj.as_ptr(), typ.as_ptr()) != 0
        }
    }

    pub fn get_builtin(&self, key: Object) -> Option<Object> {
        unsafe{
            let builtins = NonNull::new((PYTHON.PyEval_GetBuiltins)()).unwrap();
            self.dict_get(builtins, key)
        }
    }

    pub fn convert_py_to_bytes(&self, obj: Object) -> &BStr {
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

    // fn incref(&self, value: Object) {
        // unsafe{
            // (PYTHON.Py_IncRef)(value.as_ptr());
        // }
    // }

    pub fn to_float(&self, value: f64) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyFloat_FromDouble)(value))
        }
    }

    pub fn to_int(&self, value: isize) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyLong_FromSsize_t)(value))
        }
    }

    pub fn to_uint(&self, value: usize) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyLong_FromSize_t)(value))
        }
    }

    pub fn to_str(&self, string: &str) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyUnicode_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    pub fn to_bytes(&self, string: &BStr) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyBytes_FromStringAndSize)(string.as_ptr() as _, string.len() as _))
        }
    }

    pub fn empty_list(&self, size: usize) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyList_New)(size as _))
        }
    }

    pub fn list_append(&self, list: Object, item: Object) {
        unsafe{
            let result = (PYTHON.PyList_Append)(list.as_ptr(), item.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn list_clear(&self, list: Object) {
        unsafe{
            (PYTHON.PyList_Clear)(list.as_ptr());
        }
    }

    pub fn list_from_iter<I: Iterator<Item=Object> + ExactSizeIterator>(&self, iter: I) -> Option<Object> {
        unsafe{
            let list = self.empty_list(iter.len())?;
            for (i, value) in iter.enumerate() {
                let result = (PYTHON.PyList_SetItem)(list.as_ptr(), i as _, value.as_ptr());
                debug_assert!(result == 0);
            }
            Some(list)
        }
    }

    pub fn empty_dict(&self) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyDict_New)())
        }
    }

    pub fn dict_clear(&self, dict: Object) {
        unsafe{
            (PYTHON.PyDict_Clear)(dict.as_ptr());
        }
    }

    pub fn dict_get(&self, dict: Object, key: Object) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyDict_GetItem)(dict.as_ptr(), key.as_ptr()))
        }
    }

    pub fn dict_get_string(&self, dict: Object, key: &CStr) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyDict_GetItemString)(dict.as_ptr(), key.as_ptr() as _))
        }
    }

    pub fn dict_set(&self, dict: Object, key: Object, value: Object) {
        unsafe{
            let result = (PYTHON.PyDict_SetItem)(dict.as_ptr(), key.as_ptr(), value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn dict_set_string(&self, dict: Object, key: &CStr, value: Object) {
        unsafe{
            let result = (PYTHON.PyDict_SetItemString)(dict.as_ptr(), key.as_ptr() as _, value.as_ptr());
            debug_assert!(result == 0);
        }
    }

    pub fn getattr(&self, obj: Object, key: Object) -> Option<Object> {
        unsafe{
            NonNull::new((PYTHON.PyObject_GetAttr)(obj.as_ptr(), key.as_ptr()))
        }
    }

    pub fn call_func(&self, func: Object, args: &[Object]) -> Option<Object> {
        unsafe{
            (PYTHON.PyErr_Clear)();
            let args: &[Pointer] = std::mem::transmute(args);
            NonNull::new((PYTHON.PyObject_Vectorcall)(func.as_ptr(), args.as_ptr(), args.len(), null_mut()))
        }
    }

    pub fn compile_code(&self, code: &str, start: StartToken) -> Option<Object> {
        self.compile_code_cstr(&CString::new(code).unwrap(), start)
    }

    pub fn compile_code_cstr(&self, code: &CStr, start: StartToken) -> Option<Object> {
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

    pub fn exec_code(&self, code: Object, globals: Pointer, locals: Pointer) -> Option<Object> {
        unsafe{
            (PYTHON.PyErr_Clear)();
            NonNull::new((PYTHON.PyEval_EvalCode)(code.as_ptr(), globals, locals))
        }
    }

    pub fn exec_cstr(&self, code: &CStr, globals: Pointer, locals: Pointer) {
        let code = self.compile_code_cstr(code, StartToken::File).unwrap();
        self.exec_code(code, globals, locals);
    }

    pub fn exec(&self, code: &str, globals: Pointer, locals: Pointer) {
        self.exec_cstr(&CString::new(code).unwrap(), globals, locals)
    }

    pub fn iter(&self, obj: Object) -> impl Iterator<Item=Object> {
        let iter = unsafe{ (PYTHON.PyObject_GetIter)(obj.as_ptr()) };
        debug_assert!(!iter.is_null());

        let mut item: Option<Object> = None;
        std::iter::from_fn(move || unsafe {
            if let Some(item) = item {
                (PYTHON.Py_DecRef)(item.as_ptr());
            }
            item = NonNull::new((PYTHON.PyIter_Next)(iter));
            item
        })
    }
}
