use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeListArray, RecordBatch};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::ChangeResolutionOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyRecordBatch};

use crate::array::PyCellArray;
use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;

#[pyfunction]
pub(crate) fn change_resolution(
    py: Python<'_>,
    cellarray: PyCellArray,
    h3_resolution: u8,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let out = py.allow_threads(|| {
        cellindexarray
            .change_resolution(h3_resolution)
            .into_pyresult()
    })?;

    h3array_to_pyarray(out, py)
}

#[pyfunction]
pub(crate) fn change_resolution_list(
    py: Python,
    cellarray: PyCellArray,
    h3_resolution: u8,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let listarray = cellindexarray
        .change_resolution_list(h3_resolution)
        .into_pyresult()?;

    PyArray::from_array_ref(Arc::new(LargeListArray::from(listarray))).to_arro3(py)
}

#[pyfunction]
pub(crate) fn change_resolution_paired(
    py: Python,
    cellarray: PyCellArray,
    h3_resolution: u8,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let pair = cellindexarray
        .change_resolution_paired(h3_resolution)
        .into_pyresult()?;

    let before = pair.before;
    let after = pair.after;

    let schema = Schema::new(vec![
        Field::new(
            format!("{}_before", DEFAULT_CELL_COLUMN_NAME),
            before.primitive_array().data_type().clone(),
            true,
        ),
        Field::new(
            format!("{}_after", DEFAULT_CELL_COLUMN_NAME),
            after.primitive_array().data_type().clone(),
            true,
        ),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(before.primitive_array().clone()),
        Arc::new(after.primitive_array().clone()),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
}

#[pyfunction]
pub(crate) fn cells_resolution(py: Python, cellarray: PyCellArray) -> PyResult<PyObject> {
    let resarray = cellarray.as_ref().resolution();
    PyArray::from_array_ref(Arc::new(resarray.into_inner())).to_arro3(py)
}
