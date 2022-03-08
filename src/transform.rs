use pyo3::prelude::*;

use h3ron_ndarray as h3n;
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyNotImplementedError;

/// affine geotransform
#[pyclass]
#[derive(Clone)]
pub struct Transform {
    pub inner: h3n::Transform,
}

#[pymethods]
impl Transform {
    #[allow(clippy::many_single_char_names)] // using the same parameter names as the affine library
    #[new]
    pub fn new(a: f64, b: f64, c: f64, d: f64, e: f64, f: f64) -> Self {
        Self {
            inner: h3n::Transform::new(a, b, c, d, e, f),
        }
    }

    /// construct a Transform from a six-values array as used by GDAL
    #[staticmethod]
    pub fn from_gdal(gdal_transform: &PyAny) -> PyResult<Self> {
        let gt: [f64; 6] = gdal_transform.extract()?;
        Ok(Transform {
            inner: h3n::Transform::from_gdal(&gt),
        })
    }

    /// construct a Transform from a six-values array as used by rasterio
    #[staticmethod]
    pub fn from_rasterio(rio_transform: &PyAny) -> PyResult<Self> {
        let rt: [f64; 6] = rio_transform.extract()?;
        Ok(Transform {
            inner: h3n::Transform::from_rasterio(&rt),
        })
    }

    fn __richcmp__(&self, other: Transform, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner == other.inner),
            CompareOp::Ne => Ok(self.inner != other.inner),
            _ => Err(PyNotImplementedError::new_err("not implemented")),
        }
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.inner))
    }
}
