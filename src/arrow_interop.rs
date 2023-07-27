use std::any::{type_name, Any};

use h3arrow::array::{
    CellIndexArray, DirectedEdgeIndexArray, H3Array, H3IndexArrayValue, VertexIndexArray,
};
use h3arrow::export::arrow2::array::{Array, PrimitiveArray, UInt64Array};
use h3arrow::export::arrow2::datatypes::Field;
use h3arrow::export::arrow2::ffi;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::Python;

use crate::error::{IntoPyErr, IntoPyResult};

pub(crate) fn with_pyarrow<F, O>(f: F) -> PyResult<O>
where
    F: FnOnce(Python, &PyModule) -> PyResult<O>,
{
    Python::with_gil(|py| {
        let pyarrow = py.import("pyarrow")?;
        f(py, pyarrow)
    })
}

/// Arrow array to Python.
/// from https://github.com/pola-rs/polars/blob/d1e5b1062c6872cd030b04b96505d2fac36b5376/py-polars/src/arrow_interop/to_py.rs
pub(crate) fn native_to_pyarray(
    array: Box<dyn Array>,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&Field::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

#[inline]
pub fn h3array_to_pyarray<IX>(
    h3array: H3Array<IX>,
    py: Python,
    pyarrow: &PyModule,
) -> PyResult<PyObject>
where
    IX: H3IndexArrayValue,
{
    native_to_pyarray(PrimitiveArray::from(h3array).boxed(), py, pyarrow)
}

pub(crate) fn pyarray_to_boxed(obj: &PyAny) -> PyResult<Box<dyn Array>> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    obj.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    let array = unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).into_pyresult()?;
        ffi::import_array_from_c(*array, field.data_type).into_pyresult()?
    };
    Ok(array)
}

pub(crate) fn pyarray_to_native<T: Any + Array + Clone>(obj: &PyAny) -> PyResult<T> {
    let array = pyarray_to_boxed(obj)?;

    // downcast to the concrete type
    Ok(array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "Expected {}, found arrow array of type {:?}",
                type_name::<T>(),
                array.data_type(),
            ))
        })?
        .clone())
}

pub(crate) fn pyarray_to_cellindexarray(obj: &PyAny) -> PyResult<CellIndexArray> {
    pyarray_to_h3array::<CellIndexArray>(obj)
}

pub(crate) fn pyarray_to_vertexindexarray(obj: &PyAny) -> PyResult<VertexIndexArray> {
    pyarray_to_h3array::<VertexIndexArray>(obj)
}

pub(crate) fn pyarray_to_directededgeindexarray(obj: &PyAny) -> PyResult<DirectedEdgeIndexArray> {
    pyarray_to_h3array::<DirectedEdgeIndexArray>(obj)
}

pub(crate) fn pyarray_to_uint64array(obj: &PyAny) -> PyResult<UInt64Array> {
    pyarray_to_native::<UInt64Array>(obj)
}

#[inline]
fn pyarray_to_h3array<T>(obj: &PyAny) -> PyResult<T>
where
    T: TryFrom<UInt64Array>,
    <T as TryFrom<UInt64Array>>::Error: IntoPyErr,
{
    T::try_from(pyarray_to_uint64array(obj)?).into_pyresult()
}
