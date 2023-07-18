use h3arrow::error::Error as A3Error;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::{PyErr, PyResult};
use rasterh3::Error;

pub trait IntoPyResult<T> {
    fn into_pyresult(self) -> PyResult<T>;
}

pub trait IntoPyErr {
    fn into_pyerr(self) -> PyErr;
}

impl IntoPyErr for h3arrow::export::arrow2::error::Error {
    fn into_pyerr(self) -> PyErr {
        PyRuntimeError::new_err(self.to_string())
    }
}

impl IntoPyErr for A3Error {
    fn into_pyerr(self) -> PyErr {
        match self {
            A3Error::InvalidCellIndex(e) => e.into_pyerr(),
            A3Error::InvalidVertexIndex(e) => e.into_pyerr(),
            A3Error::InvalidDirectedEdgeIndex(e) => e.into_pyerr(),
            A3Error::InvalidResolution(e) => e.into_pyerr(),
            A3Error::InvalidLatLng(e) => e.into_pyerr(),
            A3Error::InvalidGeometry(e) => e.into_pyerr(),
            A3Error::CompactionError(e) => e.into_pyerr(),
            A3Error::OutlinerError(e) => e.into_pyerr(),
            A3Error::Arrow2(e) => e.into_pyerr(),
            A3Error::NotAPrimitiveArrayU64
            | A3Error::NonParsableCellIndex
            | A3Error::NonParsableDirectedEdgeIndex
            | A3Error::NonParsableVertexIndex
            | A3Error::InvalidWKB => PyValueError::new_err(self.to_string()),
            A3Error::IO(e) => e.into_pyerr(),
        }
    }
}

macro_rules! impl_h3o_value_err {
    ($($err_type:ty,)*) => {
        $(
            impl IntoPyErr for $err_type {
                fn into_pyerr(self) -> PyErr {
                    PyValueError::new_err(
                        self.to_string()
                    )
                }
            }
        )*
    }
}

impl_h3o_value_err!(
    h3arrow::export::h3o::error::CompactionError,
    h3arrow::export::h3o::error::InvalidCellIndex,
    h3arrow::export::h3o::error::InvalidDirectedEdgeIndex,
    h3arrow::export::h3o::error::InvalidGeometry,
    h3arrow::export::h3o::error::InvalidLatLng,
    h3arrow::export::h3o::error::InvalidResolution,
    h3arrow::export::h3o::error::InvalidVertexIndex,
    h3arrow::export::h3o::error::OutlinerError,
);

impl IntoPyErr for rasterh3::Error {
    fn into_pyerr(self) -> PyErr {
        match self {
            Error::TransformNotInvertible | Error::EmptyArray => {
                PyValueError::new_err(self.to_string())
            }
            Error::InvalidLatLng(e) => e.into_pyerr(),
            Error::InvalidGeometry(e) => e.into_pyerr(),
            Error::InvalidResolution(e) => e.into_pyerr(),
            Error::CompactionError(e) => e.into_pyerr(),
        }
    }
}

impl IntoPyErr for std::io::Error {
    fn into_pyerr(self) -> PyErr {
        PyIOError::new_err(self.to_string())
    }
}

impl<T, E> IntoPyResult<T> for Result<T, E>
where
    E: IntoPyErr,
{
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(IntoPyErr::into_pyerr)
    }
}
