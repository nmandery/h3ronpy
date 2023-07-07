use h3arrow::algorithm::{GridDiskDistances, GridOp, KAggregationMethod};
use h3arrow::export::arrow2::array::{Array, ListArray, PrimitiveArray};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{PyObject, PyResult};
use std::str::FromStr;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk(cellarray: &PyAny, k: u32, flatten: bool) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let listarray = cellindexarray.grid_disk(k).into_pyresult()?;
    if flatten {
        let cellindexarray = listarray.into_flattened().into_pyresult()?;
        with_pyarrow(|py, pyarrow| h3array_to_pyarray(cellindexarray, py, pyarrow))
    } else {
        with_pyarrow(|py, pyarrow| {
            native_to_pyarray(ListArray::from(listarray).boxed(), py, pyarrow)
        })
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk_distances(cellarray: &PyAny, k: u32, flatten: bool) -> PyResult<PyObject> {
    let griddiskdistances = pyarray_to_cellindexarray(cellarray)?
        .grid_disk_distances(k)
        .into_pyresult()?;

    return_griddiskdistances_table(griddiskdistances, flatten)
}

#[pyfunction]
#[pyo3(signature = (cellarray, k_min, k_max, flatten = false))]
pub(crate) fn grid_ring_distances(
    cellarray: &PyAny,
    k_min: u32,
    k_max: u32,
    flatten: bool,
) -> PyResult<PyObject> {
    if k_min >= k_max {
        return Err(PyValueError::new_err("k_min must be less than k_max"));
    }
    let griddiskdistances = pyarray_to_cellindexarray(cellarray)?
        .grid_ring_distances(k_min, k_max)
        .into_pyresult()?;

    return_griddiskdistances_table(griddiskdistances, flatten)
}

fn return_griddiskdistances_table(
    griddiskdistances: GridDiskDistances,
    flatten: bool,
) -> PyResult<PyObject> {
    let (cells, distances) = if flatten {
        (
            PrimitiveArray::from(griddiskdistances.cells.into_flattened().into_pyresult()?).boxed(),
            griddiskdistances
                .distances
                .values()
                .as_any()
                .downcast_ref::<PrimitiveArray<u32>>()
                .ok_or_else(|| PyRuntimeError::new_err("expected primitivearray<u32>"))
                .map(|pa| pa.clone().to_boxed())?,
        )
    } else {
        (
            ListArray::from(griddiskdistances.cells).boxed(),
            griddiskdistances.distances.boxed(),
        )
    };

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            native_to_pyarray(cells, py, pyarrow)?,
            native_to_pyarray(distances, py, pyarrow)?,
        ];
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_arrays", (arrays, [DEFAULT_CELL_COLUMN_NAME, "k"]))?;
        Ok(table.to_object(py))
    })
}

struct KAggregationMethodWrapper(KAggregationMethod);

impl FromStr for KAggregationMethodWrapper {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "min" => Ok(Self(KAggregationMethod::Min)),
            "max" => Ok(Self(KAggregationMethod::Max)),
            _ => Err(PyValueError::new_err("unknown way to aggregate k")),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray, k, aggregation_method))]
pub(crate) fn grid_disk_aggregate_k(
    cellarray: &PyAny,
    k: u32,
    aggregation_method: &str,
) -> PyResult<PyObject> {
    let aggregation_method = KAggregationMethodWrapper::from_str(aggregation_method)?;

    let griddiskaggk = pyarray_to_cellindexarray(cellarray)?
        .grid_disk_aggregate_k(k, aggregation_method.0)
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            h3array_to_pyarray(griddiskaggk.cells, py, pyarrow)?,
            native_to_pyarray(griddiskaggk.distances.to_boxed(), py, pyarrow)?,
        ];
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_arrays", (arrays, [DEFAULT_CELL_COLUMN_NAME, "k"]))?;
        Ok(table.to_object(py))
    })
}
