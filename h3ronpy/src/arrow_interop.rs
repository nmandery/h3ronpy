use arrow::array::{Array, UInt64Array};
use pyo3_arrow::PyArray;
use std::any::{type_name, Any};
use std::sync::Arc;

use h3arrow::array::{
    CellIndexArray, DirectedEdgeIndexArray, H3Array, H3IndexArrayValue, VertexIndexArray,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::Python;

use crate::error::{IntoPyErr, IntoPyResult};

#[inline]
pub fn h3array_to_pyarray<IX>(h3array: H3Array<IX>, py: Python) -> PyResult<PyObject>
where
    IX: H3IndexArrayValue,
{
    let pa: UInt64Array = h3array.into();
    PyArray::from_array_ref(Arc::new(pa)).to_arro3(py)
}

pub(crate) fn pyarray_to_native<T: Any + Array + Clone>(obj: &Bound<PyAny>) -> PyResult<T> {
    let array = obj.extract::<PyArray>()?;
    let (array, _field) = array.into_inner();

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

pub(crate) fn pyarray_to_cellindexarray(obj: &Bound<PyAny>) -> PyResult<CellIndexArray> {
    pyarray_to_h3array::<CellIndexArray>(obj)
}

pub(crate) fn pyarray_to_vertexindexarray(obj: &Bound<PyAny>) -> PyResult<VertexIndexArray> {
    pyarray_to_h3array::<VertexIndexArray>(obj)
}

pub(crate) fn pyarray_to_directededgeindexarray(
    obj: &Bound<PyAny>,
) -> PyResult<DirectedEdgeIndexArray> {
    pyarray_to_h3array::<DirectedEdgeIndexArray>(obj)
}

pub(crate) fn pyarray_to_uint64array(obj: &Bound<PyAny>) -> PyResult<UInt64Array> {
    pyarray_to_native::<UInt64Array>(obj)
}

#[inline]
fn pyarray_to_h3array<T>(obj: &Bound<PyAny>) -> PyResult<T>
where
    T: TryFrom<UInt64Array>,
    <T as TryFrom<UInt64Array>>::Error: IntoPyErr,
{
    T::try_from(pyarray_to_uint64array(obj)?).into_pyresult()
}
