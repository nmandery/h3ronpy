use h3arrow::array::{
    CellIndexArray, DirectedEdgeIndexArray, FromIteratorWithValidity, H3Array, VertexIndexArray,
};
use h3arrow::export::arrow2::array::{BooleanArray, PrimitiveArray};
use h3arrow::export::arrow2::bitmap::{Bitmap, MutableBitmap};
use h3arrow::export::arrow2::datatypes::DataType;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

fn h3index_valid<A>(arr: &PyAny, booleanarray: bool) -> PyResult<PyObject>
where
    A: FromIteratorWithValidity<Option<u64>> + Into<PrimitiveArray<u64>> + H3Array,
{
    let u64array = pyarray_to_uint64array(arr)?;
    let validated = A::from_iter_with_validity(u64array.iter().map(|v| v.copied()));

    with_pyarrow(|py, pyarrow| {
        native_to_pyarray(
            if booleanarray {
                let pa = validated.into();
                let bm: Bitmap = pa
                    .validity()
                    .cloned()
                    .unwrap_or_else(|| MutableBitmap::from_len_set(pa.len()).into());
                BooleanArray::try_new(DataType::Boolean, bm, None)
                    .into_pyresult()?
                    .boxed()
            } else {
                validated.into().boxed()
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

impl_h3index_valid!(cells_valid, CellIndexArray);
impl_h3index_valid!(vertexes_valid, VertexIndexArray);
impl_h3index_valid!(directededges_valid, DirectedEdgeIndexArray);
