use arrow::array::{Array, UInt64Array};
use arrow::datatypes::Field;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;
use std::any::{type_name, Any};
use std::sync::Arc;

use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::Python;

use crate::error::{IntoPyErr, IntoPyResult};

pub(crate) fn pyarray_to_native<T: Any + Array + Clone>(array: PyArray) -> PyResult<T> {
    let array = array.array();
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

pub(crate) fn pyarray_to_cellindexarray(obj: PyArray) -> PyResult<CellIndexArray> {
    pyarray_to_h3array::<CellIndexArray>(obj)
}

pub(crate) fn pyarray_to_vertexindexarray(obj: PyArray) -> PyResult<VertexIndexArray> {
    pyarray_to_h3array::<VertexIndexArray>(obj)
}

pub(crate) fn pyarray_to_directededgeindexarray(obj: PyArray) -> PyResult<DirectedEdgeIndexArray> {
    pyarray_to_h3array::<DirectedEdgeIndexArray>(obj)
}

pub(crate) fn pyarray_to_uint64array(array: PyArray) -> PyResult<UInt64Array> {
    pyarray_to_native::<UInt64Array>(array)
}

#[inline]
fn pyarray_to_h3array<T>(obj: PyArray) -> PyResult<T>
where
    T: TryFrom<UInt64Array>,
    <T as TryFrom<UInt64Array>>::Error: IntoPyErr,
{
    T::try_from(pyarray_to_uint64array(obj)?).into_pyresult()
}

pub(crate) fn array_to_arro3<A: Array + 'static, S: Into<String>>(
    py: Python<'_>,
    array: A,
    name: S,
    nullable: bool,
) -> PyArrowResult<PyObject> {
    let data_type = array.data_type().clone();
    Ok(PyArray::new(
        Arc::new(array),
        Field::new(name, data_type, nullable).into(),
    )
    .to_arro3(py)?)
}
