use h3arrow::algorithm::ParseCellsOp;
use h3arrow::export::arrow2::array::Utf8Array;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (utf8array, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(utf8array: &PyAny, set_failing_to_invalid: bool) -> PyResult<PyObject> {
    let utf8array: Utf8Array<i32> = pyarray_to_native(utf8array)?;
    let cells = utf8array
        .parse_cells(set_failing_to_invalid)
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(cells, py, pyarrow))
}
