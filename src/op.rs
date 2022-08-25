use std::iter::once;
use std::str::FromStr;

use h3ron::collections::{HashMap, RandomState};
use h3ron::error::check_valid_h3_resolution;
use h3ron::iter::GridDiskBuilder;
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
fn k_min_max_capacity(num_h3indexes: usize, k_min: u32, k_max: u32) -> PyResult<usize> {
    Ok(num_h3indexes
        * (h3ron::max_grid_disk_size(k_max).into_pyresult()?
            - h3ron::max_grid_disk_size(k_min.saturating_sub(1)).into_pyresult()?))
}

#[allow(clippy::type_complexity)]
#[pyfunction]
fn grid_disk_distances(
    py: Python,
    h3index_arr: PyReadonlyArray1<u64>,
    k_min: u32,
    k_max: u32,
) -> PyResult<(Py<PyArray1<u64>>, Py<PyArray1<u64>>, Py<PyArray1<u32>>)> {
    check_k_min_max(k_min, k_max)?;
    let h3indexes = h3index_arr.as_array();

    let capacity = k_min_max_capacity(h3indexes.len(), k_min, k_max)?;
    let mut center_h3indexes = Vec::with_capacity(capacity);
    let mut ring_h3indexes = Vec::with_capacity(capacity);
    let mut ks = Vec::with_capacity(capacity);

    let mut builder = GridDiskBuilder::create(k_min, k_max).into_pyresult()?;

    for h3index in h3indexes.iter() {
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        builder.build_grid_disk(&cell).into_pyresult()?;
        for (ring_cell, ring_cell_k) in &mut builder {
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

    let mut cellmap = HashMap::with_capacity_and_hasher(
        k_min_max_capacity(h3indexes.len(), k_min, k_max)?,
        RandomState::default(),
    );
    let mut builder = GridDiskBuilder::create(k_min, k_max).into_pyresult()?;

    for h3index in h3indexes.iter() {
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        builder.build_grid_disk(&cell).into_pyresult()?;
        for (ring_cell, ring_cell_k) in &mut builder {
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

/// Vectorized grid-disk building, with the k-values of the rings being aggregated to their `min` or
/// `max` value for each cell.
#[allow(clippy::type_complexity)]
#[pyfunction]
fn grid_disk_distances_agg(
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

#[pyfunction]
fn change_resolution(
    py: Python,
    h3index_arr: PyReadonlyArray1<u64>,
    h3_resolution: u8,
) -> PyResult<Py<PyArray1<u64>>> {
    check_valid_h3_resolution(h3_resolution).into_pyresult()?;

    let mut out_vec = Vec::with_capacity(h3index_arr.len());
    for h3index in h3index_arr.as_array().iter() {
        // TODO: this will fail once the index-mode is checked
        //       and the function gets applied to edges or other types.
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        let change_iter = h3ron::iter::change_resolution(once(cell), h3_resolution);
        out_vec.reserve(change_iter.size_hint().0);
        for maybe_cell in change_iter {
            out_vec.push(maybe_cell.into_pyresult()?.h3index())
        }
    }
    Ok(out_vec.to_pyarray(py).to_owned())
}

#[allow(clippy::type_complexity)]
#[pyfunction]
fn change_resolution_paired(
    py: Python,
    h3index_arr: PyReadonlyArray1<u64>,
    h3_resolution: u8,
) -> PyResult<(Py<PyArray1<u64>>, Py<PyArray1<u64>>)> {
    check_valid_h3_resolution(h3_resolution).into_pyresult()?;

    let mut out_vec_after = Vec::with_capacity(h3index_arr.len());
    let mut out_vec_before = Vec::with_capacity(h3index_arr.len());
    for h3index in h3index_arr.as_array().iter() {
        // TODO: this will fail once the index-mode is checked
        //       and the function gets applied to edges or other types.
        let cell = H3Cell::try_from(*h3index).into_pyresult()?;
        let iter = h3ron::iter::change_resolution_tuple(once(cell), h3_resolution);
        out_vec_after.reserve(iter.size_hint().0);
        out_vec_before.reserve(iter.size_hint().0);
        for pair in iter {
            let (before_cell, after_cell) = pair.into_pyresult()?;
            out_vec_before.push(before_cell.h3index() as u64);
            out_vec_after.push(after_cell.h3index() as u64);
        }
    }
    Ok((
        out_vec_before.to_pyarray(py).to_owned(),
        out_vec_after.to_pyarray(py).to_owned(),
    ))
}

pub fn init_op_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(grid_disk_distances, m)?)?;
    m.add_function(wrap_pyfunction!(grid_disk_distances_agg, m)?)?;
    m.add_function(wrap_pyfunction!(change_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(change_resolution_paired, m)?)?;

    Ok(())
}
