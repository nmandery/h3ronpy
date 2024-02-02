use crate::arrow_interop::pyarray_to_cellindexarray;
use arrow::array::Array;
use arrow::pyarrow::IntoPyArrow;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_m2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_m2();
    Python::with_gil(|py| out.into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_km2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_km2();
    Python::with_gil(|py| out.into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_rads2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_rads2();
    Python::with_gil(|py| out.into_data().into_pyarrow(py))
}
