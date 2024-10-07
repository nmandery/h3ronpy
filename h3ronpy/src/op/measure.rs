use crate::arrow_interop::{array_to_arro3, pyarray_to_cellindexarray};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

const AREA_NAME: &str = "area";

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_m2(py: Python<'_>, cellarray: PyArray) -> PyArrowResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_m2();
    array_to_arro3(py, out, AREA_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_km2(py: Python<'_>, cellarray: PyArray) -> PyArrowResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_km2();
    array_to_arro3(py, out, AREA_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_rads2(py: Python<'_>, cellarray: PyArray) -> PyArrowResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_rads2();
    array_to_arro3(py, out, AREA_NAME, true)
}
