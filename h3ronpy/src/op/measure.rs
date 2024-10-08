use crate::arrow_interop::{array_to_arro3, PyConcatedArray};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;

const AREA_NAME: &str = "area";

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_m2(py: Python<'_>, cellarray: PyConcatedArray) -> PyArrowResult<PyObject> {
    let out = cellarray.into_cellindexarray()?.area_m2();
    array_to_arro3(py, out, AREA_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_km2(
    py: Python<'_>,
    cellarray: PyConcatedArray,
) -> PyArrowResult<PyObject> {
    let out = cellarray.into_cellindexarray()?.area_km2();
    array_to_arro3(py, out, AREA_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_rads2(
    py: Python<'_>,
    cellarray: PyConcatedArray,
) -> PyArrowResult<PyObject> {
    let out = cellarray.into_cellindexarray()?.area_rads2();
    array_to_arro3(py, out, AREA_NAME, true)
}
