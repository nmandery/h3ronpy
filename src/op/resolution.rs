use h3arrow::algorithm::ChangeResolutionOp;
use h3arrow::export::arrow2::array::PrimitiveArray;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;

#[pyfunction]
pub(crate) fn change_resolution(cellarray: &PyAny, h3_resolution: u8) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let out = cellindexarray
        .change_resolution(h3_resolution)
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(out, py, pyarrow))
}

#[pyfunction]
pub(crate) fn change_resolution_paired(cellarray: &PyAny, h3_resolution: u8) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let pair = cellindexarray
        .change_resolution_paired(h3_resolution)
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            h3array_to_pyarray(pair.before, py, pyarrow)?,
            h3array_to_pyarray(pair.after, py, pyarrow)?,
        ];
        let table = pyarrow.getattr("Table")?.call_method1(
            "from_arrays",
            (
                arrays,
                [
                    format!("{}_before", DEFAULT_CELL_COLUMN_NAME),
                    format!("{}_after", DEFAULT_CELL_COLUMN_NAME),
                ],
            ),
        )?;
        Ok(table.to_object(py))
    })
}

#[pyfunction]
pub(crate) fn cells_resolution(cellarray: &PyAny) -> PyResult<PyObject> {
    let resarray = pyarray_to_cellindexarray(cellarray)?.resolution();
    with_pyarrow(|py, pyarrow| {
        native_to_pyarray(PrimitiveArray::from(resarray).boxed(), py, pyarrow)
    })
}
