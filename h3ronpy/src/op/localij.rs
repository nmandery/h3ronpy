use crate::array::PyCellArray;
use crate::arrow_interop::{h3array_to_pyarray, pyarray_to_cellindexarray, pyarray_to_native};
use crate::error::IntoPyResult;
use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::localij::{LocalIJArrays, ToLocalIJOp};
use h3arrow::array::CellIndexArray;
use h3arrow::h3o::CellIndex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{pyfunction, Bound, PyAny, PyObject, PyResult, Python};
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyRecordBatch;
use std::iter::repeat;
use std::sync::Arc;

#[pyfunction]
#[pyo3(signature = (cellarray, anchor, set_failing_to_invalid = false))]
pub(crate) fn cells_to_localij(
    py: Python,
    cellarray: PyCellArray,
    anchor: &Bound<PyAny>,
    set_failing_to_invalid: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let anchorarray = get_anchor_array(anchor, cellindexarray.len())?;

    let localij_arrays = cellindexarray
        .to_local_ij_array(anchorarray, set_failing_to_invalid)
        .into_pyresult()?;

    let i = localij_arrays.i.clone();
    let j = localij_arrays.j.clone();
    let anchor = localij_arrays.anchors.primitive_array().clone();

    let schema = Schema::new(vec![
        Field::new("i", i.data_type().clone(), true),
        Field::new("j", j.data_type().clone(), true),
        Field::new("anchor", anchor.data_type().clone(), true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(localij_arrays.i),
        Arc::new(localij_arrays.j),
        Arc::new(anchor),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (anchor, i_array, j_array, set_failing_to_invalid = false))]
pub(crate) fn localij_to_cells(
    anchor: &Bound<PyAny>,
    i_array: &Bound<PyAny>,
    j_array: &Bound<PyAny>,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let i_array = pyarray_to_native::<Int32Array>(i_array)?;
    let j_array = pyarray_to_native::<Int32Array>(j_array)?;
    let anchorarray = get_anchor_array(anchor, i_array.len())?;

    let localij_arrays = LocalIJArrays::try_new(anchorarray, i_array, j_array).into_pyresult()?;

    let cellarray = if set_failing_to_invalid {
        localij_arrays
            .to_cells_failing_to_invalid()
            .into_pyresult()?
    } else {
        localij_arrays.to_cells().into_pyresult()?
    };

    Python::with_gil(|py| h3array_to_pyarray(cellarray, py))
}

fn get_anchor_array(anchor: &Bound<PyAny>, len: usize) -> PyResult<CellIndexArray> {
    if let Ok(anchor) = anchor.extract::<u64>() {
        let anchor_cell = CellIndex::try_from(anchor).into_pyresult()?;
        Ok(CellIndexArray::from_iter(repeat(anchor_cell).take(len)))
    } else if let Ok(anchorarray) = pyarray_to_cellindexarray(anchor) {
        Ok(anchorarray)
    } else {
        return Err(PyValueError::new_err(format!(
            "Expected a single cell or an array of cells, found type {:?}",
            anchor.get_type(),
        )));
    }
}
