use h3arrow::algorithm::CompactOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;

use crate::array::PyCellArray;
use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (cellarray, mixed_resolutions = false))]
pub(crate) fn compact(cellarray: PyCellArray, mixed_resolutions: bool) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let compacted = if mixed_resolutions {
        cellindexarray.compact_mixed_resolutions()
    } else {
        cellindexarray.compact()
    }
    .into_pyresult()?;

    Python::with_gil(|py| h3array_to_pyarray(compacted, py))
}

#[pyfunction]
#[pyo3(signature = (cellarray, target_resolution))]
pub(crate) fn uncompact(cellarray: PyCellArray, target_resolution: u8) -> PyResult<PyObject> {
    let target_resolution = Resolution::try_from(target_resolution).into_pyresult()?;
    let out = cellarray.into_inner().uncompact(target_resolution);
    Python::with_gil(|py| h3array_to_pyarray(out, py))
}
