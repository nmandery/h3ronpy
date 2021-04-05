use std::str::FromStr;

use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

use h3ron_ndarray as h3n;

pub struct AxisOrder {
    pub inner: h3n::AxisOrder
}

impl FromStr for AxisOrder {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "yx" | "YX" => Ok(Self { inner: h3n::AxisOrder::YX }),
            "xy" | "XY" => Ok(Self { inner: h3n::AxisOrder::XY }),
            _ => Err(PyValueError::new_err("unknown axis order"))
        }
    }
}

pub struct ResolutionSearchMode {
    pub inner: h3n::ResolutionSearchMode
}

impl FromStr for ResolutionSearchMode {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "min_diff" | "min-diff" => Ok(Self { inner: h3n::ResolutionSearchMode::MinDiff }),
            "smaller_than_pixel" | "smaller-than-pixel" => Ok(Self { inner: h3n::ResolutionSearchMode::SmallerThanPixel }),
            _ => Err(PyValueError::new_err("unknown resolution search mode"))
        }
    }
}
