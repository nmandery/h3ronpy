use crate::arrow_interop::{
    h3array_to_pyarray, pyarray_to_cellindexarray, pyarray_to_native, with_pyarrow,
};
use crate::error::IntoPyResult;
use arrow::array::{Array, Int32Array};
use arrow::pyarrow::ToPyArrow;
use h3arrow::algorithm::localij::{LocalIJArrays, ToLocalIJOp};
use h3arrow::array::CellIndexArray;
use h3arrow::h3o::CellIndex;
use pyo3::exceptions::PyValueError;
use pyo3::{pyfunction, PyAny, PyObject, PyResult, Python, ToPyObject};
use std::iter::repeat;

#[pyfunction]
#[pyo3(signature = (cellarray, anchor, set_failing_to_invalid = false))]
pub(crate) fn cells_to_localij(
    cellarray: &PyAny,
    anchor: &PyAny,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let anchorarray = get_anchor_array(anchor, cellindexarray.len())?;

    let localij_arrays = cellindexarray
        .to_local_ij_array(anchorarray, set_failing_to_invalid)
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            localij_arrays.i.into_data().to_pyarrow(py)?,
            localij_arrays.j.into_data().to_pyarrow(py)?,
            localij_arrays
                .anchors
                .primitive_array()
                .into_data()
                .to_pyarrow(py)?,
        ];
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_arrays", (arrays, ["i", "j", "anchor"]))?;
        Ok(table.to_object(py))
    })
}

#[pyfunction]
#[pyo3(signature = (anchor, i_array, j_array, set_failing_to_invalid = false))]
pub(crate) fn localij_to_cells(
    anchor: &PyAny,
    i_array: &PyAny,
    j_array: &PyAny,
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

fn get_anchor_array(anchor: &PyAny, len: usize) -> PyResult<CellIndexArray> {
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
