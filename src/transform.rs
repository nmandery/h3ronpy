use pyo3::basic::CompareOp;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

/// affine geotransform
#[pyclass]
#[derive(Clone)]
pub struct Transform {
    pub(crate) inner: rasterh3::Transform,
}

#[pymethods]
impl Transform {
    #[allow(clippy::many_single_char_names)] // using the same parameter names as the affine library
    #[new]
    pub fn new(a: f64, b: f64, c: f64, d: f64, e: f64, f: f64) -> Self {
        Self {
            inner: rasterh3::Transform::new(a, b, c, d, e, f),
        }
    }

    /// construct a Transform from a six-values array as used by GDAL
    #[staticmethod]
    pub fn from_gdal(gdal_transform: [f64; 6]) -> Self {
        Transform {
            inner: rasterh3::Transform::from_gdal(&gdal_transform),
        }
    }

    /// construct a Transform from a six-values array as used by rasterio
    #[staticmethod]
    pub fn from_rasterio(rio_transform: [f64; 6]) -> Self {
        Transform {
            inner: rasterh3::Transform::from_rasterio(&rio_transform),
        }
    }

    fn __richcmp__(&self, other: Transform, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner == other.inner),
            CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Err(PyNotImplementedError::new_err(format!(
                "{:?} is not implemented",
                op
            ))),
        }
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
}
