use h3arrow::export::h3o::Resolution;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub struct PyResolution(Resolution);

impl<'py> FromPyObject<'py> for PyResolution {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let int = ob.extract::<u8>()?;
        let res =
            Resolution::try_from(int).map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(res))
    }
}

impl From<PyResolution> for Resolution {
    fn from(value: PyResolution) -> Self {
        value.0
    }
}
