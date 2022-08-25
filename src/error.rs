use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{PyErr, PyResult};

pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

trait IntoPyErr {
    fn into_pyerr(self) -> PyErr;
}

impl IntoPyErr for h3ron::Error {
    fn into_pyerr(self) -> PyErr {
        match self {
            Self::Domain
            | Self::LatLonDomain
            | Self::ResDomain
            | Self::CellInvalid
            | Self::DirectedEdgeInvalid
            | Self::UndirectedEdgeInvalid
            | Self::VertexInvalid
            | Self::Pentagon
            | Self::DuplicateInput
            | Self::NotNeighbors
            | Self::ResMismatch
            | Self::MemoryBounds
            | Self::OptionInvalid
            | Self::DirectionInvalid(_) => PyValueError::new_err(self.to_string()),

            Self::Failed
            | Self::MemoryAlloc
            | Self::UnknownError(_)
            | Self::DecompressionError(_) => PyRuntimeError::new_err(self.to_string()),
        }
    }
}

impl<T> IntoPyResult<T> for Result<T, h3ron::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => Err(err.into_pyerr()),
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
                h3ron_ndarray::Error::H3ron(h3ron_e) => Err(h3ron_e.into_pyerr()),
            },
        }
    }
}
