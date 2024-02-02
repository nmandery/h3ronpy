use arrow::array::{make_array, Array, ArrayData, UInt64Array};
use arrow::pyarrow::{FromPyArrow, IntoPyArrow};
use std::any::{type_name, Any};

use h3arrow::array::{
    CellIndexArray, DirectedEdgeIndexArray, H3Array, H3IndexArrayValue, VertexIndexArray,
};
use pyo3::exceptions::PyValueError;
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

#[inline]
pub fn h3array_to_pyarray<IX>(h3array: H3Array<IX>, py: Python) -> PyResult<PyObject>
where
    IX: H3IndexArrayValue,
{
    let pa: UInt64Array = h3array.into();
    pa.into_data().into_pyarrow(py)
}

pub(crate) fn pyarray_to_native<T: Any + Array + Clone>(obj: &PyAny) -> PyResult<T> {
    let array = make_array(ArrayData::from_pyarrow(obj)?);

    let array = array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "Expected {}, found arrow array of type {:?}",
                type_name::<T>(),
                array.data_type(),
            ))
        })?
        .clone();

    // downcast to the concrete type
    Ok(array)
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
