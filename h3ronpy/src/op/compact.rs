use arrow::array::PrimitiveArray;
use h3arrow::algorithm::CompactOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (cellarray, mixed_resolutions = false))]
pub(crate) fn compact(
    py: Python<'_>,
    cellarray: PyArray,
    mixed_resolutions: bool,
) -> PyArrowResult<PyObject> {
    let name = cellarray.field().name().to_string();
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let compacted = py
        .allow_threads(|| {
            if mixed_resolutions {
                cellindexarray.compact_mixed_resolutions()
            } else {
                cellindexarray.compact()
            }
        })
        .into_pyresult()?;

    array_to_arro3(py, PrimitiveArray::from(compacted), name, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray, target_resolution))]
pub(crate) fn uncompact(
    py: Python<'_>,
    cellarray: PyArray,
    target_resolution: u8,
) -> PyArrowResult<PyObject> {
    let name = cellarray.field().name().to_string();
    let target_resolution = Resolution::try_from(target_resolution).into_pyresult()?;
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let out = py.allow_threads(|| cellindexarray.uncompact(target_resolution));
    array_to_arro3(py, PrimitiveArray::from(out), name, true)
}
