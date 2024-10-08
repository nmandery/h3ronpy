use crate::arrow_interop::{array_to_arro3, PyConcatedArray};
use crate::error::IntoPyResult;
use arrow::array::{Array, ArrayRef, PrimitiveArray, RecordBatch};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::localij::{LocalIJArrays, ToLocalIJOp};
use h3arrow::array::CellIndexArray;
use h3arrow::h3o::CellIndex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{pyfunction, Bound, PyAny, PyObject, PyResult, Python};
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyTable;
use std::iter::repeat;
use std::sync::Arc;

#[pyfunction]
#[pyo3(signature = (cellarray, anchor, set_failing_to_invalid = false))]
pub(crate) fn cells_to_localij(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    anchor: &Bound<PyAny>,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let anchorarray = get_anchor_array(anchor, cellindexarray.len())?;

    let localij_arrays = cellindexarray
        .to_local_ij_array(anchorarray, set_failing_to_invalid)
        .into_pyresult()?;

    let outarrays: Vec<ArrayRef> = vec![
        Arc::new(localij_arrays.i),
        Arc::new(localij_arrays.j),
        Arc::new(PrimitiveArray::from(localij_arrays.anchors)),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("i", outarrays[0].data_type().clone(), true),
        Field::new("j", outarrays[1].data_type().clone(), true),
        Field::new("anchor", outarrays[2].data_type().clone(), true),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;

    PyTable::try_new(vec![rb], schema)?.to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (anchor, i_array, j_array, set_failing_to_invalid = false))]
pub(crate) fn localij_to_cells(
    py: Python<'_>,
    anchor: &Bound<PyAny>,
    i_array: PyConcatedArray,
    j_array: PyConcatedArray,
    set_failing_to_invalid: bool,
) -> PyArrowResult<PyObject> {
    let i_array = i_array.into_int32array()?;
    let j_array = j_array.into_int32array()?;
    let anchorarray = get_anchor_array(anchor, i_array.len())?;

    let localij_arrays = LocalIJArrays::try_new(anchorarray, i_array, j_array).into_pyresult()?;

    let cellarray = py.allow_threads(|| {
        if set_failing_to_invalid {
            localij_arrays.to_cells_failing_to_invalid().into_pyresult()
        } else {
            localij_arrays.to_cells().into_pyresult()
        }
    })?;

    array_to_arro3(py, PrimitiveArray::from(cellarray), "cells", true)
}

fn get_anchor_array(anchor: &Bound<PyAny>, len: usize) -> PyResult<CellIndexArray> {
    if let Ok(anchor) = anchor.extract::<u64>() {
        let anchor_cell = CellIndex::try_from(anchor).into_pyresult()?;
        Ok(CellIndexArray::from_iter(repeat(anchor_cell).take(len)))
    } else if let Ok(anchorarray) = anchor
        .extract::<PyConcatedArray>()
        .and_then(|ca| ca.into_cellindexarray())
    {
        Ok(anchorarray)
    } else {
        return Err(PyValueError::new_err(format!(
            "Expected a single cell or an array of cells, found type {:?}",
            anchor.get_type(),
        )));
    }
}
