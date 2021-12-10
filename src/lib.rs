#![warn(
    clippy::all,
    clippy::correctness,
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    nonstandard_style
)]

use pyo3::{prelude::*, wrap_pyfunction, PyNativeType, Python};

use crate::op::init_op_submodule;
use crate::raster::init_raster_submodule;
use crate::vector::init_vector_submodule;
use crate::{collections::H3CompactedVec, polygon::Polygon};
use h3ron::{H3Cell, Index};

mod collections;
mod error;
mod op;
mod polygon;
mod raster;
mod transform;
mod vector;

/// version of the module
#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// h3ron python bindings
#[pymodule]
fn h3ronpy(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    env_logger::init(); // run with the environment variable RUST_LOG set to "debug" for log output

    m.add("H3CompactedVec", m.py().get_type::<H3CompactedVec>())?;
    m.add("Polygon", m.py().get_type::<Polygon>())?;

    m.add_function(wrap_pyfunction!(version, m)?)?;

    let vector_submod = PyModule::new(py, "vector")?;
    init_vector_submodule(vector_submod)?;
    m.add_submodule(vector_submod)?;

    let raster_submod = PyModule::new(py, "raster")?;
    init_raster_submodule(raster_submod)?;
    m.add_submodule(raster_submod)?;

    let op_submod = PyModule::new(py, "op")?;
    init_op_submodule(op_submod)?;
    m.add_submodule(op_submod)?;

    Ok(())
}

pub fn cells_to_h3indexes(mut cells: Vec<H3Cell>) -> Vec<u64> {
    cells.drain(..).map(|cell| cell.h3index() as u64).collect()
}
