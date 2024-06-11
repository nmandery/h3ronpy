use arrow::array::{Array, BooleanArray};
use arrow::buffer::NullBuffer;
use arrow::pyarrow::IntoPyArrow;
use h3arrow::array::{FromIteratorWithValidity, H3Array, H3IndexArrayValue};
use h3arrow::h3o;
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};
use pyo3::prelude::*;

use crate::arrow_interop::*;

fn h3index_valid<IX>(arr: &PyAny, booleanarray: bool) -> PyResult<PyObject>
where
    IX: H3IndexArrayValue,
{
    let u64array = pyarray_to_uint64array(arr)?;
    let validated = H3Array::<IX>::from_iter_with_validity(u64array.iter());

    Python::with_gil(|py| {
        if booleanarray {
            let nullbuffer = validated
                .primitive_array()
                .nulls()
                .cloned()
                .unwrap_or_else(|| NullBuffer::new_valid(validated.len()));
            BooleanArray::from(nullbuffer.into_inner())
                .into_data()
                .into_pyarrow(py)
        } else {
            h3array_to_pyarray(validated, py)
        }
    })
}

macro_rules! impl_h3index_valid {
    ($name:ident, $arr_type:ty) => {
        #[pyfunction]
        #[pyo3(signature = (array, booleanarray = false))]
        pub(crate) fn $name(array: &PyAny, booleanarray: bool) -> PyResult<PyObject> {
            h3index_valid::<$arr_type>(array, booleanarray)
        }
    };
}

impl_h3index_valid!(cells_valid, CellIndex);
impl_h3index_valid!(vertexes_valid, VertexIndex);
impl_h3index_valid!(directededges_valid, DirectedEdgeIndex);
