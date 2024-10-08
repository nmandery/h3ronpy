use arrow::array::{
    Array, ArrayRef, GenericListArray, LargeListArray, PrimitiveArray, RecordBatch, UInt32Array,
};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::{GridDiskDistances, GridOp, KAggregationMethod};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{PyObject, PyResult};
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyRecordBatch};
use std::str::FromStr;
use std::sync::Arc;

use crate::array::PyCellArray;
use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk(
    py: Python,
    cellarray: PyCellArray,
    k: u32,
    flatten: bool,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let listarray = cellindexarray.grid_disk(k).into_pyresult()?;
    if flatten {
        let cellindexarray = listarray.into_flattened().into_pyresult()?;
        h3array_to_pyarray(cellindexarray, py)
    } else {
        PyArray::from_array_ref(Arc::new(LargeListArray::from(listarray))).to_arro3(py)
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk_distances(
    py: Python,
    cellarray: PyCellArray,
    k: u32,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let griddiskdistances = cellarray
        .into_inner()
        .grid_disk_distances(k)
        .into_pyresult()?;

    return_griddiskdistances_table(py, griddiskdistances, flatten)
}

#[pyfunction]
#[pyo3(signature = (cellarray, k_min, k_max, flatten = false))]
pub(crate) fn grid_ring_distances(
    py: Python,
    cellarray: PyCellArray,
    k_min: u32,
    k_max: u32,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    if k_min >= k_max {
        return Err(PyValueError::new_err("k_min must be less than k_max").into());
    }
    let griddiskdistances = cellarray
        .into_inner()
        .grid_ring_distances(k_min, k_max)
        .into_pyresult()?;

    return_griddiskdistances_table(py, griddiskdistances, flatten)
}

fn return_griddiskdistances_table(
    py: Python,
    griddiskdistances: GridDiskDistances<i64>,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let (cells, distances): (ArrayRef, ArrayRef) = if flatten {
        (
            Arc::new(PrimitiveArray::from(
                griddiskdistances.cells.into_flattened().into_pyresult()?,
            )),
            Arc::new(
                griddiskdistances
                    .distances
                    .values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("expected primitivearray<u32>"))
                    .cloned()?,
            ),
        )
    } else {
        (
            Arc::new(GenericListArray::<i64>::from(griddiskdistances.cells)),
            Arc::new(griddiskdistances.distances),
        )
    };

    let schema = Schema::new(vec![
        Field::new(DEFAULT_CELL_COLUMN_NAME, cells.data_type().clone(), true),
        Field::new("k", distances.data_type().clone(), true),
    ]);
    let columns = vec![cells, distances];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
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
    py: Python,
    cellarray: PyCellArray,
    k: u32,
    aggregation_method: &str,
) -> PyArrowResult<PyObject> {
    let aggregation_method = KAggregationMethodWrapper::from_str(aggregation_method)?;

    let griddiskaggk = cellarray
        .as_ref()
        .grid_disk_aggregate_k(k, aggregation_method.0)
        .into_pyresult()?;

    let schema = Schema::new(vec![
        Field::new(
            DEFAULT_CELL_COLUMN_NAME,
            griddiskaggk.cells.primitive_array().data_type().clone(),
            true,
        ),
        Field::new("k", griddiskaggk.distances.data_type().clone(), true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(griddiskaggk.cells.primitive_array().clone()),
        Arc::new(griddiskaggk.distances),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
}
