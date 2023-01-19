use crate::error::IntoPyResult;
use geo_types as gt;
use h3ron::{H3Cell, Index, ToAlignedLinkedPolygons, ToLinkedPolygons};
use numpy::PyReadonlyArray1;
use py_geo_interface::to_py::AsGeoInterface;
use pyo3::prelude::*;

#[pyclass]
pub struct Polygon {
    inner: gt::Polygon<f64>,
}

#[pymethods]
impl Polygon {
    #[staticmethod]
    #[pyo3(signature = (h3index_arr, smoothen=false))]
    fn from_h3indexes(
        h3index_arr: PyReadonlyArray1<u64>,
        smoothen: bool,
    ) -> PyResult<Vec<Polygon>> {
        let h3indexes: Vec<_> = h3index_arr
            .as_array()
            .iter()
            .map(|hi| H3Cell::new(*hi))
            .collect();

        Ok(h3indexes
            .to_linked_polygons(smoothen)
            .into_pyresult()?
            .into_iter()
            .map(|poly| Polygon { inner: poly })
            .collect())
    }

    #[staticmethod]
    #[pyo3(signature = (h3index_arr, align_to_h3_resolution, smoothen=false))]
    fn from_h3indexes_aligned(
        h3index_arr: PyReadonlyArray1<u64>,
        align_to_h3_resolution: u8,
        smoothen: bool,
    ) -> PyResult<Vec<Polygon>> {
        let h3indexes: Vec<_> = h3index_arr
            .as_array()
            .iter()
            .map(|hi| H3Cell::new(*hi))
            .collect();

        Ok(h3indexes
            .to_aligned_linked_polygons(align_to_h3_resolution, smoothen)
            .into_pyresult()?
            .into_iter()
            .map(|poly| Polygon { inner: poly })
            .collect())
    }

    // python __geo_interface__ spec: https://gist.github.com/sgillies/2217756
    #[getter]
    fn __geo_interface__(&self, py: Python) -> PyResult<PyObject> {
        self.inner.as_geointerface_pyobject(py)
    }
}
