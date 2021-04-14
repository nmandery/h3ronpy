use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyResult;

pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, h3ron::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => match err {
                h3ron::Error::InvalidInput
                | h3ron::Error::MixedResolutions
                | h3ron::Error::InvalidH3Resolution
                | h3ron::Error::InvalidH3Hexagon(_)
                | h3ron::Error::InvalidH3Edge(_) => Err(PyValueError::new_err(err.to_string())),

                h3ron::Error::PentagonalDistortion
                | h3ron::Error::NoLocalIJCoordinates
                | h3ron::Error::LineNotComputable
                | h3ron::Error::UnsupportedOperation => {
                    Err(PyRuntimeError::new_err(err.to_string()))
                }
            },
        }
    }
}

impl<T> IntoPyResult<T> for Result<T, h3ron_ndarray::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => match err {
                h3ron_ndarray::Error::EmptyArray | h3ron_ndarray::Error::UnsupportedArrayShape => {
                    Err(PyValueError::new_err(err.to_string()))
                }
                h3ron_ndarray::Error::TransformNotInvertible => {
                    Err(PyRuntimeError::new_err(err.to_string()))
                }
            },
        }
    }
}
