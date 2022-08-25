use std::collections::HashMap;
use std::iter::once;

use crate::error::IntoPyResult;
use geo_types as gt;
use h3ron::{H3Cell, Index, ToAlignedLinkedPolygons, ToLinkedPolygons};
use numpy::PyReadonlyArray1;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[pyclass]
pub struct Polygon {
    inner: gt::Polygon<f64>,
}

#[pymethods]
impl Polygon {
    #[staticmethod]
    #[args(smoothen = "false")]
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
            .drain(..)
            .map(|poly| Polygon { inner: poly })
            .collect())
    }

    #[staticmethod]
    #[args(smoothen = "false")]
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
            .drain(..)
            .map(|poly| Polygon { inner: poly })
            .collect())
    }

    // python __geo_interface__ spec: https://gist.github.com/sgillies/2217756
    #[getter]
    fn __geo_interface__(&self, py: Python) -> PyObject {
        let mut main = HashMap::new();
        main.insert("type".to_string(), "Polygon".to_string().into_py(py));
        main.insert("coordinates".to_string(), {
            let rings: Vec<_> = once(self.inner.exterior())
                .chain(self.inner.interiors().iter())
                .map(|ring| {
                    let r: Vec<_> = ring
                        .0
                        .iter()
                        .map(|c| PyTuple::new(py, &[c.x, c.y]).to_object(py))
                        .collect();
                    PyTuple::new(py, &r).to_object(py)
                })
                .collect();
            PyTuple::new(py, &rings).to_object(py)
        });

        main.to_object(py)
    }
}
