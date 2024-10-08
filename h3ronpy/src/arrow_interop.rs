use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, UInt64Array, UInt8Array};
use arrow::compute::concat;
use arrow::datatypes::{Field, FieldRef};
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyChunkedArray};
use std::any::{type_name, Any};
use std::sync::Arc;

use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, ResolutionArray, VertexIndexArray};
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

/*
pub(crate) fn pyarray_to_cellindexarray(obj: PyArray) -> PyResult<CellIndexArray> {
    pyarray_to_h3array::<CellIndexArray>(obj)
}

pub(crate) fn pyarray_to_vertexindexarray(obj: PyArray) -> PyResult<VertexIndexArray> {
    pyarray_to_h3array::<VertexIndexArray>(obj)
}

pub(crate) fn pyarray_to_directededgeindexarray(obj: PyArray) -> PyResult<DirectedEdgeIndexArray> {
    pyarray_to_h3array::<DirectedEdgeIndexArray>(obj)
}
    */

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

/// Workaround to avoid implementing all algorithms for &[ArrayRef] / chunked arrays.
///
/// This comes at the cost of having to copy data arround and should be improved.
pub struct PyConcatedArray(PyArray);

impl PyConcatedArray {
    #[allow(unused)]
    pub fn array(&self) -> &ArrayRef {
        self.0.array()
    }

    pub fn field(&self) -> &FieldRef {
        self.0.field()
    }

    pub fn into_cellindexarray(self) -> PyResult<CellIndexArray> {
        pyarray_to_h3array::<CellIndexArray>(self.0)
    }

    pub fn into_vertexindexarray(self) -> PyResult<VertexIndexArray> {
        pyarray_to_h3array::<VertexIndexArray>(self.0)
    }

    pub fn into_directededgeindexarray(self) -> PyResult<DirectedEdgeIndexArray> {
        pyarray_to_h3array::<DirectedEdgeIndexArray>(self.0)
    }

    pub fn into_int32array(self) -> PyResult<Int32Array> {
        pyarray_to_native::<Int32Array>(self.0)
    }

    pub fn into_uint64array(self) -> PyResult<UInt64Array> {
        pyarray_to_native::<UInt64Array>(self.0)
    }

    pub fn into_float64array(self) -> PyResult<Float64Array> {
        pyarray_to_native::<Float64Array>(self.0)
    }

    pub fn into_resolutionarray(self) -> PyResult<ResolutionArray> {
        ResolutionArray::try_from(pyarray_to_native::<UInt8Array>(self.0)?).into_pyresult()
    }
}

impl From<PyConcatedArray> for PyArray {
    fn from(value: PyConcatedArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PyConcatedArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(array) = ob.extract::<PyArray>() {
            Ok(Self(array))
        } else if let Ok(chunked_array) = ob.extract::<PyChunkedArray>() {
            let (arrays, field) = chunked_array.into_inner();
            let array_refs: Vec<_> = arrays.iter().map(|a| a.as_ref()).collect();
            Ok(Self(
                PyArray::try_new(concat(array_refs.as_ref()).into_pyresult()?, field)
                    .into_pyresult()?,
            ))
        // TODO: numpy support. is available in pyo3-arrow, but not available in the rust api
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ method, __arrow_c_stream__ method or implementing buffer protocol.",
            ))
        }
    }
}
