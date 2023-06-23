use crate::arrow_interop::{native_to_pyarray, pyarray_to_cellindexarray, with_pyarrow};
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_m2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_m2();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out.boxed(), py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_km2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_km2();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out.boxed(), py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_area_rads2(cellarray: &PyAny) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?.area_rads2();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out.boxed(), py, pyarrow))
}
