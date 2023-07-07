use h3arrow::algorithm::CompactOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (cellarray, mixed_resolutions = false))]
pub(crate) fn compact(cellarray: &PyAny, mixed_resolutions: bool) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let compacted = if mixed_resolutions {
        cellindexarray.compact_mixed_resolutions()
    } else {
        cellindexarray.compact()
    }
    .into_pyresult()?;

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(compacted, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (cellarray, target_resolution))]
pub(crate) fn uncompact(cellarray: &PyAny, target_resolution: u8) -> PyResult<PyObject> {
    let target_resolution = Resolution::try_from(target_resolution).into_pyresult()?;
    let out = pyarray_to_cellindexarray(cellarray)?.uncompact(target_resolution);
    with_pyarrow(|py, pyarrow| h3array_to_pyarray(out, py, pyarrow))
}
