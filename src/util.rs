use pyo3::prelude::*;
use h3ron::H3_MAX_RESOLUTION;
use pyo3::exceptions::PyValueError;

pub fn validate_h3_resolution(h3_res: u8) -> PyResult<()> {
    if h3_res > H3_MAX_RESOLUTION {
        Err(PyValueError::new_err(format!("h3 resolution out of range: {}", h3_res)))
    } else {
        Ok(())
    }
}

