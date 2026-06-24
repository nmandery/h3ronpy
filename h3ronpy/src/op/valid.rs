use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use arrow::buffer::NullBuffer;
use h3arrow::array::{FromIteratorWithValidity, H3Array, H3IndexArrayValue};
use h3arrow::h3o;
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::arrow_interop::*;

fn h3index_valid<'py, IX>(
    py: Python<'py>,
    arr: &Bound<'py, PyAny>,
    booleanarray: bool,
) -> PyResult<Bound<'py, PyAny>>
where
    IX: H3IndexArrayValue + Send,
{
    let u64array = pyarray_to_uint64array(arr)?;
    let validated = py.detach(|| H3Array::<IX>::from_iter_with_validity(u64array.iter()));

    if booleanarray {
        let nullbuffer = validated
            .primitive_array()
            .nulls()
            .cloned()
            .unwrap_or_else(|| NullBuffer::new_valid(validated.len()));
        let bools = BooleanArray::from(nullbuffer.into_inner());
        PyArray::from_array_ref(Arc::new(bools)).into_arro3(py)
    } else {
        h3array_to_pyarray(validated, py)
    }
}

macro_rules! impl_h3index_valid {
    ($name:ident, $arr_type:ty) => {
        #[pyfunction]
        #[pyo3(signature = (array, booleanarray = false))]
        pub(crate) fn $name<'py>(
            py: Python<'py>,
            array: &Bound<'py, PyAny>,
            booleanarray: bool,
        ) -> PyResult<Bound<'py, PyAny>> {
            h3index_valid::<$arr_type>(py, array, booleanarray)
        }
    };
}

impl_h3index_valid!(cells_valid, CellIndex);
impl_h3index_valid!(vertexes_valid, VertexIndex);
impl_h3index_valid!(directededges_valid, DirectedEdgeIndex);
