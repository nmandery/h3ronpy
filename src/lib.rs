#![warn(
    clippy::all,
    clippy::correctness,
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    nonstandard_style
)]

use pyo3::{prelude::*, wrap_pyfunction, Python};

use crate::op::init_op_submodule;
use crate::raster::init_raster_submodule;
use crate::vector::init_vector_submodule;

mod arrow_interop;
mod error;
mod op;
mod raster;
mod transform;
mod vector;

pub(crate) const DEFAULT_CELL_COLUMN_NAME: &str = "cell";

/// version of the module
#[pyfunction]
fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// indicates if this extension has been compiled in release-mode
#[pyfunction]
fn is_release_build() -> bool {
    #[cfg(debug_assertions)]
    return false;

    #[cfg(not(debug_assertions))]
    return true;
}

#[pymodule]
fn h3ronpy(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    env_logger::init(); // run with the environment variable RUST_LOG set to "debug" for log output

    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(is_release_build, m)?)?;

    let raster_submod = PyModule::new(py, "raster")?;
    init_raster_submodule(raster_submod)?;
    m.add_submodule(raster_submod)?;

    let op_submod = PyModule::new(py, "op")?;
    init_op_submodule(op_submod)?;
    m.add_submodule(op_submod)?;

    let vector_submod = PyModule::new(py, "vector")?;
    init_vector_submodule(vector_submod)?;
    m.add_submodule(vector_submod)?;

    m.add("DEFAULT_CELL_COLUMN_NAME", DEFAULT_CELL_COLUMN_NAME)?;

    Ok(())
}
