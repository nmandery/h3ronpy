use arrow::array::{
    Array, ArrayRef, GenericListArray, LargeListArray, PrimitiveArray, RecordBatch, UInt32Array,
};
use arrow::datatypes::{Field, Schema};
use h3arrow::algorithm::{GridDiskDistances, GridOp, KAggregationMethod};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyObject;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyTable;
use std::str::FromStr;
use std::sync::Arc;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;
use crate::DEFAULT_CELL_COLUMN_NAME;
use pyo3::prelude::*;

const DISK_NAME: &str = "disk";

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    k: u32,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let listarray = py
        .allow_threads(|| cellindexarray.grid_disk(k))
        .into_pyresult()?;
    if flatten {
        let cellindexarray = py
            .allow_threads(|| listarray.into_flattened())
            .into_pyresult()?;
        array_to_arro3(py, PrimitiveArray::from(cellindexarray), DISK_NAME, true)
    } else {
        array_to_arro3(py, LargeListArray::from(listarray), DISK_NAME, true)
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray, k, flatten = false))]
pub(crate) fn grid_disk_distances(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    k: u32,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let griddiskdistances = py
        .allow_threads(|| cellindexarray.grid_disk_distances(k))
        .into_pyresult()?;

    return_griddiskdistances_table(py, griddiskdistances, flatten)
}

#[pyfunction]
#[pyo3(signature = (cellarray, k_min, k_max, flatten = false))]
pub(crate) fn grid_ring_distances(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    k_min: u32,
    k_max: u32,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    if k_min >= k_max {
        return Err(PyValueError::new_err("k_min must be less than k_max").into());
    }
    let cellindexarray = cellarray.into_cellindexarray()?;
    let griddiskdistances = py
        .allow_threads(|| cellindexarray.grid_ring_distances(k_min, k_max))
        .into_pyresult()?;

    return_griddiskdistances_table(py, griddiskdistances, flatten)
}

fn return_griddiskdistances_table(
    py: Python<'_>,
    griddiskdistances: GridDiskDistances<i64>,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let (cells, distances): (ArrayRef, ArrayRef) = if flatten {
        (
            Arc::new(PrimitiveArray::from(
                py.allow_threads(|| griddiskdistances.cells.into_flattened())
                    .into_pyresult()?,
            )),
            Arc::new(
                griddiskdistances
                    .distances
                    .values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| PyRuntimeError::new_err("expected primitivearray<u32>"))?
                    .clone(),
            ),
        )
    } else {
        (
            Arc::new(GenericListArray::<i64>::from(griddiskdistances.cells)),
            Arc::new(griddiskdistances.distances),
        )
    };

    let outarrays: Vec<ArrayRef> = vec![cells, distances];
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            DEFAULT_CELL_COLUMN_NAME,
            outarrays[0].data_type().clone(),
            true,
        ),
        Field::new("k", outarrays[1].data_type().clone(), true),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;
    Ok(PyTable::try_new(vec![rb], schema)?.to_arro3(py)?)
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
    py: Python<'_>,
    cellarray: PyConcatedArray,
    k: u32,
    aggregation_method: &str,
) -> PyArrowResult<PyObject> {
    let aggregation_method = KAggregationMethodWrapper::from_str(aggregation_method)?;
    let cellindexarray = cellarray.into_cellindexarray()?;

    let griddiskaggk = py
        .allow_threads(|| cellindexarray.grid_disk_aggregate_k(k, aggregation_method.0))
        .into_pyresult()?;

    let outarrays: Vec<ArrayRef> = vec![
        Arc::new(PrimitiveArray::from(griddiskaggk.cells)),
        Arc::new(griddiskaggk.distances),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            DEFAULT_CELL_COLUMN_NAME,
            outarrays[0].data_type().clone(),
            true,
        ),
        Field::new("k", outarrays[1].data_type().clone(), true),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;
    Ok(PyTable::try_new(vec![rb], schema)?.to_arro3(py)?)
}
