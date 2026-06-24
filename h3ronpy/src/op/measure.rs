use std::sync::Arc;

use crate::array::PyCellArray;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_m2(py: Python<'_>, cellarray: PyCellArray) -> PyResult<Bound<'_, PyAny>> {
    let out = cellarray.as_ref().area_m2();
    PyArray::from_array_ref(Arc::new(out)).into_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_km2(py: Python<'_>, cellarray: PyCellArray) -> PyResult<Bound<'_, PyAny>> {
    let out = cellarray.as_ref().area_km2();
    PyArray::from_array_ref(Arc::new(out)).into_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_rads2(py: Python<'_>, cellarray: PyCellArray) -> PyResult<Bound<'_, PyAny>> {
    let out = cellarray.as_ref().area_rads2();
    PyArray::from_array_ref(Arc::new(out)).into_arro3(py)
}
