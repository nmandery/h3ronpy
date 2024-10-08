use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeListArray, PrimitiveArray, RecordBatch};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::ChangeResolutionOp;
use h3arrow::export::h3o::Resolution;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyTable;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;

const RESOLUTION_NAME: &str = "resolution";

#[pyfunction]
pub(crate) fn change_resolution(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    h3_resolution: u8,
) -> PyArrowResult<PyObject> {
    let field = cellarray.field().clone();
    let cellindexarray = cellarray.into_cellindexarray()?;
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let out = py
        .allow_threads(|| cellindexarray.change_resolution(h3_resolution))
        .into_pyresult()?;

    array_to_arro3(py, PrimitiveArray::from(out), field.name(), true)
}

#[pyfunction]
pub(crate) fn change_resolution_list(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    h3_resolution: u8,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let listarray = py.allow_threads(|| {
        cellindexarray
            .change_resolution_list(h3_resolution)
            .into_pyresult()
    })?;

    array_to_arro3(py, LargeListArray::from(listarray), RESOLUTION_NAME, true)
}

#[pyfunction]
pub(crate) fn change_resolution_paired(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    h3_resolution: u8,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;
    let pair = py.allow_threads(|| {
        cellindexarray
            .change_resolution_paired(h3_resolution)
            .into_pyresult()
    })?;

    let outarrays: Vec<ArrayRef> = vec![
        Arc::new(PrimitiveArray::from(pair.before)),
        Arc::new(PrimitiveArray::from(pair.after)),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            format!("{}_before", DEFAULT_CELL_COLUMN_NAME),
            outarrays[0].data_type().clone(),
            true,
        ),
        Field::new(
            format!("{}_after", DEFAULT_CELL_COLUMN_NAME),
            outarrays[1].data_type().clone(),
            true,
        ),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;

    Ok(PyTable::try_new(vec![rb], schema)?.to_arro3(py)?)
}

#[pyfunction]
pub(crate) fn cells_resolution(
    py: Python<'_>,
    cellarray: PyConcatedArray,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let resarray = PrimitiveArray::from(py.allow_threads(|| cellindexarray.resolution()));

    array_to_arro3(py, resarray, RESOLUTION_NAME, true)
}
