use h3arrow::algorithm::ParseCellsOp;
use h3arrow::export::arrow2::array::Utf8Array;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (utf8array, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(utf8array: &PyAny, set_failing_to_invalid: bool) -> PyResult<PyObject> {
    let boxed_array = pyarray_to_boxed(utf8array)?;
    let cells = if let Some(utf8array) = boxed_array.as_any().downcast_ref::<Utf8Array<i32>>() {
        utf8array
            .parse_cells(set_failing_to_invalid)
            .into_pyresult()?
    } else if let Some(utf8array) = boxed_array.as_any().downcast_ref::<Utf8Array<i64>>() {
        utf8array
            .parse_cells(set_failing_to_invalid)
            .into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse cells from",
        ));
    };

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(cells, py, pyarrow))
}
