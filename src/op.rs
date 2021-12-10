use std::convert::TryFrom;
use std::str::FromStr;

use h3ron::collections::HashMap;
use h3ron::iter::KRingBuilder;
use h3ron::{H3Cell, Index};
use ndarray::ArrayView1;
use numpy::{PyArray1, PyReadonlyArray1, ToPyArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyModule;
use pyo3::{pyfunction, wrap_pyfunction, Py, PyErr, PyResult, Python};

use crate::error::IntoPyResult;

fn check_k_min_max(k_min: u32, k_max: u32) -> PyResult<()> {
    if k_max < k_min {
        return Err(PyValueError::new_err(format!(
            "k_min must be smaller or equal to k_max. (k_min={}, k_max={})",
            k_min, k_max
        )));
    }
    Ok(())
}

#[inline]
fn k_min_max_capacity(num_h3indexes: usize, k_min: u32, k_max: u32) -> usize {
    num_h3indexes
        * (h3ron::max_k_ring_size(k_max) - h3ron::max_k_ring_size(k_min.saturating_sub(1)))
}

#[allow(clippy::type_complexity)]
#[pyfunction]
fn kring_distances(
    py: Python,
    h3index_arr: PyReadonlyArray1<u64>,
    k_min: u32,
    k_max: u32,
) -> PyResult<(Py<PyArray1<u64>>, Py<PyArray1<u64>>, Py<PyArray1<u32>>)> {
    check_k_min_max(k_min, k_max)?;
    let h3indexes = h3index_arr.as_array();

    let capacity = k_min_max_capacity(h3indexes.len(), k_min, k_max);
    let mut center_h3indexes = Vec::with_capacity(capacity);
    let mut ring_h3indexes = Vec::with_capacity(capacity);
    let mut ks = Vec::with_capacity(capacity);

    let mut k_ring_builder = KRingBuilder::new(k_min, k_max);

    for h3index in h3indexes.iter() {
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        k_ring_builder.build_k_ring(&cell);
        for (ring_cell, ring_cell_k) in &mut k_ring_builder {
            center_h3indexes.push(*h3index);
            ring_h3indexes.push(ring_cell.h3index() as u64);
            ks.push(ring_cell_k);
        }
    }

    Ok((
        center_h3indexes.to_pyarray(py).to_owned(),
        ring_h3indexes.to_pyarray(py).to_owned(),
        ks.to_pyarray(py).to_owned(),
    ))
}

enum KAggregationMode {
    Min,
    Max,
}

impl FromStr for KAggregationMode {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            _ => Err(PyValueError::new_err("unknown way to aggregate k")),
        }
    }
}

#[allow(clippy::type_complexity)]
fn kring_distances_agg_internal<A: Fn(&mut u32, u32)>(
    py: Python,
    h3indexes: ArrayView1<u64>,
    k_min: u32,
    k_max: u32,
    agg_closure: A,
) -> PyResult<(Py<PyArray1<u64>>, Py<PyArray1<u32>>)> {
    check_k_min_max(k_min, k_max)?;

    let mut cellmap = HashMap::with_capacity(k_min_max_capacity(h3indexes.len(), k_min, k_max));
    let mut k_ring_builder = KRingBuilder::new(k_min, k_max);

    for h3index in h3indexes.iter() {
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        k_ring_builder.build_k_ring(&cell);
        for (ring_cell, ring_cell_k) in &mut k_ring_builder {
            cellmap
                .entry(ring_cell.h3index() as u64)
                .and_modify(|existing_ring_cell_k| agg_closure(existing_ring_cell_k, ring_cell_k))
                .or_insert(ring_cell_k);
        }
    }

    let capacity = cellmap.len();
    let (h3indexes_out, k_out) = cellmap.drain().fold(
        (Vec::with_capacity(capacity), Vec::with_capacity(capacity)),
        |mut vecs, (h3index, k)| {
            vecs.0.push(h3index);
            vecs.1.push(k);
            vecs
        },
    );
    Ok((
        h3indexes_out.to_pyarray(py).to_owned(),
        k_out.to_pyarray(py).to_owned(),
    ))
}

/// Vectorized k-ring building, with the k-values of the rings being aggregated to their `min` or
/// `max` value for each cell.
#[allow(clippy::type_complexity)]
#[pyfunction]
fn kring_distances_agg(
    py: Python,
    h3index_arr: PyReadonlyArray1<u64>,
    k_min: u32,
    k_max: u32,
    aggregation_mode_str: &str,
) -> PyResult<(Py<PyArray1<u64>>, Py<PyArray1<u32>>)> {
    let h3indexes = h3index_arr.as_array();
    let agg_closure = match KAggregationMode::from_str(aggregation_mode_str)? {
        KAggregationMode::Min => |existing_k: &mut u32, new_k: u32| {
            if *existing_k > new_k {
                *existing_k = new_k
            }
        },
        KAggregationMode::Max => |existing_k: &mut u32, new_k: u32| {
            if *existing_k < new_k {
                *existing_k = new_k
            }
        },
    };
    kring_distances_agg_internal(py, h3indexes, k_min, k_max, agg_closure)
}

pub fn init_op_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(kring_distances, m)?)?;
    m.add_function(wrap_pyfunction!(kring_distances_agg, m)?)?;

    Ok(())
}
