use h3arrow::array::{FromIteratorWithValidity, H3Array, H3IndexArrayValue};
use h3arrow::export::arrow2::array::{BooleanArray, PrimitiveArray};
use h3arrow::export::arrow2::bitmap::{Bitmap, MutableBitmap};
use h3arrow::export::arrow2::datatypes::DataType;
use h3arrow::h3o;
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

fn h3index_valid<IX>(arr: &PyAny, booleanarray: bool) -> PyResult<PyObject>
where
    IX: H3IndexArrayValue,
{
    let u64array = pyarray_to_uint64array(arr)?;
    let validated = H3Array::<IX>::from_iter_with_validity(u64array.iter().map(|v| v.copied()));

    with_pyarrow(|py, pyarrow| {
        native_to_pyarray(
            if booleanarray {
                let pa: PrimitiveArray<_> = validated.into();
                let bm: Bitmap = pa
                    .validity()
                    .cloned()
                    .unwrap_or_else(|| MutableBitmap::from_len_set(pa.len()).into());
                BooleanArray::try_new(DataType::Boolean, bm, None)
                    .into_pyresult()?
                    .boxed()
            } else {
                PrimitiveArray::from(validated).boxed()
            },
            py,
            pyarrow,
        )
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
