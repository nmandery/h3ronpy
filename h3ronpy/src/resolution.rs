use h3arrow::export::h3o;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub struct Resolution(h3o::Resolution);

impl<'py> FromPyObject<'py> for Resolution {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let int = ob.extract::<u8>()?;
        int.try_into().map_err(|err| PyValueError::new_err(args))
    }
}
