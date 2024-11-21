use h3arrow::algorithm::CompactOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;

use crate::array::PyCellArray;
use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (cellarray, mixed_resolutions = false))]
pub(crate) fn compact(
    py: Python<'_>,
    cellarray: PyCellArray,
    mixed_resolutions: bool,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let compacted = py
        .allow_threads(|| {
            if mixed_resolutions {
                cellindexarray.compact_mixed_resolutions()
            } else {
                cellindexarray.compact()
            }
        })
        .into_pyresult()?;

    h3array_to_pyarray(compacted, py)
}

#[pyfunction]
#[pyo3(signature = (cellarray, target_resolution))]
pub(crate) fn uncompact(
    py: Python<'_>,
    cellarray: PyCellArray,
    target_resolution: u8,
) -> PyResult<PyObject> {
    let target_resolution = Resolution::try_from(target_resolution).into_pyresult()?;
    let cellarray = cellarray.into_inner();
    let out = py.allow_threads(|| cellarray.uncompact(target_resolution));
    h3array_to_pyarray(out, py)
}
