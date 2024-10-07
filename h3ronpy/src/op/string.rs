use arrow::array::{Array, LargeStringArray, PrimitiveArray, StringArray};
use h3arrow::algorithm::{ParseGenericStringArray, ToGenericStringArray};
use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

fn parse<O>(py: Python<'_>, stringarray: &PyArray, set_failing_to_invalid: bool) -> PyResult<O>
where
    O: ParseGenericStringArray + Send,
{
    let any_array = stringarray.array().as_any();
    let h3entities = if let Some(stringarray) = any_array.downcast_ref::<StringArray>() {
        py.allow_threads(|| O::parse_genericstringarray(stringarray, set_failing_to_invalid))
            .into_pyresult()?
    } else if let Some(stringarray) = any_array.downcast_ref::<LargeStringArray>() {
        py.allow_threads(|| O::parse_genericstringarray(stringarray, set_failing_to_invalid))
            .into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse h3 indexes from",
        ));
    };

    Ok(h3entities)
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(
    py: Python<'_>,
    stringarray: PyArray, //TODO
    set_failing_to_invalid: bool,
) -> PyArrowResult<PyObject> {
    let cells: CellIndexArray = parse(py, &stringarray, set_failing_to_invalid)?;
    array_to_arro3(
        py,
        PrimitiveArray::from(cells),
        stringarray.field().name(),
        true,
    )
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn vertexes_parse(
    py: Python<'_>,
    stringarray: PyArray,
    set_failing_to_invalid: bool,
) -> PyArrowResult<PyObject> {
    let vertexes: VertexIndexArray = parse(py, &stringarray, set_failing_to_invalid)?;
    array_to_arro3(
        py,
        PrimitiveArray::from(vertexes),
        stringarray.field().name(),
        true,
    )
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn directededges_parse(
    py: Python<'_>,
    stringarray: PyArray,
    set_failing_to_invalid: bool,
) -> PyArrowResult<PyObject> {
    let edges: DirectedEdgeIndexArray = parse(py, &stringarray, set_failing_to_invalid)?;
    array_to_arro3(
        py,
        PrimitiveArray::from(edges),
        stringarray.field().name(),
        true,
    )
}

fn to_string<A: ToGenericStringArray<i64> + Send + Sync, S: Into<String>>(
    py: Python<'_>,
    name: S,
    array: A,
) -> PyArrowResult<PyObject> {
    let stringarray = py.allow_threads(|| array.to_genericstringarray().into_pyresult())?;

    array_to_arro3(py, stringarray, name, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_to_string(py: Python<'_>, cellarray: PyArray) -> PyArrowResult<PyObject> {
    to_string(
        py,
        cellarray.field().name().clone(),
        pyarray_to_cellindexarray(cellarray)?,
    )
}

#[pyfunction]
#[pyo3(signature = (vertexarray))]
pub(crate) fn vertexes_to_string(py: Python<'_>, vertexarray: PyArray) -> PyArrowResult<PyObject> {
    to_string(
        py,
        vertexarray.field().name().clone(),
        pyarray_to_vertexindexarray(vertexarray)?,
    )
}

#[pyfunction]
#[pyo3(signature = (directededgearray))]
pub(crate) fn directededges_to_string(
    py: Python<'_>,
    directededgearray: PyArray,
) -> PyArrowResult<PyObject> {
    to_string(
        py,
        directededgearray.field().name().clone(),
        pyarray_to_directededgeindexarray(directededgearray)?,
    )
}
