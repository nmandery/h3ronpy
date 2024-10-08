use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Float64Array, GenericBinaryArray, GenericListArray,
    LargeBinaryArray, OffsetSizeTrait, PrimitiveArray, RecordBatch,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Schema};
use geo::{BoundingRect, HasDimensions};
use h3arrow::algorithm::ToCoordinatesOp;
use h3arrow::array::from_geo::{ToCellIndexArray, ToCellListArray, ToCellsOptions};
use h3arrow::array::to_geoarrow::{ToWKBLineStrings, ToWKBPoints, ToWKBPolygons};
use h3arrow::array::CellIndexArray;
use h3arrow::export::geoarrow::array::{WKBArray, WKBBuilder, WKBCapacity};
use h3arrow::export::h3o::geom::{ContainmentMode, ToGeo};
use h3arrow::export::h3o::Resolution;
use h3arrow::h3o::geom::PolyfillConfig;
use h3arrow::h3o::LatLng;
use itertools::multizip;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyTable};

use crate::error::IntoPyResult;
use crate::{arrow_interop::*, DEFAULT_CELL_COLUMN_NAME};

const WKB_NAME: &str = "wkb";

/// Containment mode used to decide if a cell is contained in a polygon or not.
///
/// Modes:
///
/// * ContainsCentroid: This mode will select every cells whose centroid are contained inside the polygon.
///
///         This is the fasted option and ensures that every cell is uniquely
///         assigned (e.g. two adjacent polygon with zero overlap also have zero
///         overlapping cells).
///         
///         On the other hand, some cells may cover area outside of the polygon
///         (overshooting) and some parts of the polygon may be left uncovered.
///
/// * ContainsBoundary: This mode will select every cells whose boundaries are entirely within the polygon.
///
///         This ensures that every cell is uniquely assigned  (e.g. two adjacent
///         polygon with zero overlap also have zero overlapping cells) and avoids
///         any coverage overshooting.
///         
///         Some parts of the polygon may be left uncovered (more than with
///         `ContainsCentroid`).
///
/// * IntersectsBoundary: This mode will select every cells whose boundaries are within the polygon, even partially.
///
///         This guarantees a complete coverage of the polygon, but some cells may
///         belong to two different polygons if they are adjacent/close enough. Some
///         cells may cover area outside of the polygon.
///
/// * Covers: This mode behaves the same as IntersectsBoundary, but also handles the case where the geometry is
///         being covered by a cell without intersecting with its boundaries. In such cases, the covering cell is returned.
///
#[pyclass(name = "ContainmentMode")]
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum PyContainmentMode {
    ContainsCentroid,
    ContainsBoundary,
    IntersectsBoundary,
    Covers,
}

impl Default for PyContainmentMode {
    fn default() -> Self {
        Self::ContainsCentroid
    }
}

impl PyContainmentMode {
    fn containment_mode(&self) -> ContainmentMode {
        match self {
            PyContainmentMode::ContainsCentroid => ContainmentMode::ContainsCentroid,
            PyContainmentMode::ContainsBoundary => ContainmentMode::ContainsBoundary,
            PyContainmentMode::IntersectsBoundary => ContainmentMode::IntersectsBoundary,
            PyContainmentMode::Covers => ContainmentMode::Covers,
        }
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray,))]
pub(crate) fn cells_bounds(cellarray: PyConcatedArray) -> PyResult<Option<PyObject>> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    if let Some(rect) = cellindexarray.bounding_rect() {
        Python::with_gil(|py| {
            Ok(Some(
                PyTuple::new_bound(py, [rect.min().x, rect.min().y, rect.max().x, rect.max().y])
                    .to_object(py),
            ))
        })
    } else {
        Ok(None)
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray,))]
pub(crate) fn cells_bounds_arrays(
    py: Python<'_>,
    cellarray: PyConcatedArray,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;

    let outarrays = py.allow_threads(|| {
        let mut minx_vec = vec![0.0f64; cellindexarray.len()];
        let mut miny_vec = vec![0.0f64; cellindexarray.len()];
        let mut maxx_vec = vec![0.0f64; cellindexarray.len()];
        let mut maxy_vec = vec![0.0f64; cellindexarray.len()];
        let mut validity_vec = vec![false; cellindexarray.len()];

        for (cell, minx, miny, maxx, maxy, validity) in multizip((
            cellindexarray.iter(),
            minx_vec.iter_mut(),
            miny_vec.iter_mut(),
            maxx_vec.iter_mut(),
            maxy_vec.iter_mut(),
            validity_vec.iter_mut(),
        )) {
            if let Some(cell) = cell {
                if let Some(rect) = cell
                    .to_geom(true)
                    .ok()
                    .and_then(|poly| poly.bounding_rect())
                {
                    *validity = true;
                    *minx = rect.min().x;
                    *miny = rect.min().y;
                    *maxx = rect.max().x;
                    *maxy = rect.max().y;
                };
            }
        }

        let validity = NullBuffer::from(validity_vec);

        vec![
            Arc::new(Float64Array::new(minx_vec.into(), Some(validity.clone()))) as ArrayRef,
            Arc::new(Float64Array::new(miny_vec.into(), Some(validity.clone()))),
            Arc::new(Float64Array::new(maxx_vec.into(), Some(validity.clone()))),
            Arc::new(Float64Array::new(maxy_vec.into(), Some(validity))),
        ]
    });

    let schema = Arc::new(Schema::new(vec![
        Field::new("minx", outarrays[0].data_type().clone(), true),
        Field::new("miny", outarrays[1].data_type().clone(), true),
        Field::new("maxx", outarrays[2].data_type().clone(), true),
        Field::new("maxy", outarrays[3].data_type().clone(), true),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;

    Ok(PyTable::try_new(vec![rb], schema)?.to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_coordinates(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;

    let coordinate_arrays = py
        .allow_threads(|| {
            if radians {
                cellindexarray.to_coordinates_radians()
            } else {
                cellindexarray.to_coordinates()
            }
        })
        .into_pyresult()?;

    let outarrays: Vec<ArrayRef> = vec![
        Arc::new(coordinate_arrays.lat) as ArrayRef,
        Arc::new(coordinate_arrays.lng),
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("lat", outarrays[0].data_type().clone(), true),
        Field::new("lng", outarrays[1].data_type().clone(), true),
    ]));

    let rb = RecordBatch::try_new(schema.clone(), outarrays).into_pyresult()?;
    Ok(PyTable::try_new(vec![rb], schema)?.to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (latarray, lngarray, resolution, radians = false))]
pub(crate) fn coordinates_to_cells(
    py: Python<'_>,
    latarray: PyConcatedArray,
    lngarray: PyConcatedArray,
    resolution: &Bound<PyAny>,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let latarray: Float64Array = latarray.into_float64array()?;
    let lngarray: Float64Array = lngarray.into_float64array()?;
    if lngarray.len() != latarray.len() {
        return Err(
            PyValueError::new_err("latarray and lngarray must be of the same length").into(),
        );
    }

    let cells = if let Ok(resolution) = resolution.extract::<u8>() {
        let resolution = Resolution::try_from(resolution).into_pyresult()?;

        py.allow_threads(|| {
            latarray
                .iter()
                .zip(lngarray.iter())
                .map(|(lat, lng)| {
                    if let (Some(lat), Some(lng)) = (lat, lng) {
                        if radians {
                            LatLng::from_radians(lat, lng).into_pyresult()
                        } else {
                            LatLng::new(lat, lng).into_pyresult()
                        }
                        .map(|ll| Some(ll.to_cell(resolution)))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<PyResult<CellIndexArray>>()
        })?
    } else {
        let resarray = resolution
            .extract::<PyConcatedArray>()?
            .into_resolutionarray()?;

        if resarray.len() != latarray.len() {
            return Err(PyValueError::new_err(
                "resarray must be of the same length as the coordinate arrays",
            )
            .into());
        }

        py.allow_threads(|| {
            multizip((latarray.iter(), lngarray.iter(), resarray.iter()))
                .map(|(lat, lng, res)| {
                    if let (Some(lat), Some(lng), Some(res)) = (lat, lng, res) {
                        if radians {
                            LatLng::from_radians(lat, lng).into_pyresult()
                        } else {
                            LatLng::new(lat, lng).into_pyresult()
                        }
                        .map(|ll| Some(ll.to_cell(res)))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<PyResult<CellIndexArray>>()
        })?
    };

    array_to_arro3(
        py,
        PrimitiveArray::from(cells),
        DEFAULT_CELL_COLUMN_NAME,
        true,
    )
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false, link_cells = false))]
pub(crate) fn cells_to_wkb_polygons(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    radians: bool,
    link_cells: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let use_degrees = !radians;

    let out: WKBArray<i64> = if link_cells {
        let out: Result<WKBArray<i64>, PyErr> = py.allow_threads(|| {
            let geoms = cellindexarray
                .iter()
                .flatten()
                .to_geom(use_degrees)
                .into_pyresult()?
                .0
                .into_iter()
                .map(|poly| Some(geo_types::Geometry::from(poly)))
                .collect::<Vec<_>>();
            let mut builder = WKBBuilder::with_capacity(WKBCapacity::from_geometries(
                geoms.iter().map(|v| v.as_ref()),
            ));
            builder.extend_from_iter(geoms.iter().map(|v| v.as_ref()));
            Ok(builder.finish())
        });
        out?
    } else {
        py.allow_threads(|| {
            cellindexarray
                .to_wkb_polygons(use_degrees)
                .expect("wkbarray")
        })
    };

    array_to_arro3(py, out.into_inner(), WKB_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_wkb_points(
    py: Python<'_>,
    cellarray: PyConcatedArray,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_cellindexarray()?;
    let out = py
        .allow_threads(|| cellindexarray.to_wkb_points::<i64>(!radians))
        .expect("wkbarray");

    array_to_arro3(py, out.into_inner(), WKB_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (vertexarray, radians = false))]
pub(crate) fn vertexes_to_wkb_points(
    py: Python<'_>,
    vertexarray: PyConcatedArray,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let vertexindexarray = vertexarray.into_vertexindexarray()?;
    let out = py
        .allow_threads(|| vertexindexarray.to_wkb_points::<i64>(!radians))
        .expect("wkbarray");

    array_to_arro3(py, out.into_inner(), WKB_NAME, true)
}

#[pyfunction]
#[pyo3(signature = (array, radians = false))]
pub(crate) fn directededges_to_wkb_linestrings(
    py: Python<'_>,
    array: PyConcatedArray,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let directededgesindexarray = array.into_directededgeindexarray()?;
    let out = py
        .allow_threads(|| directededgesindexarray.to_wkb_linestrings::<i64>(!radians))
        .expect("wkbarray");

    array_to_arro3(py, out.into_inner(), WKB_NAME, true)
}

fn get_to_cells_options(
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
) -> PyResult<ToCellsOptions> {
    Ok(ToCellsOptions::new(
        PolyfillConfig::new(Resolution::try_from(resolution).into_pyresult()?)
            .containment_mode(containment_mode.unwrap_or_default().containment_mode()),
    )
    .compact(compact))
}

#[pyfunction]
#[pyo3(signature = (array, resolution, containment_mode = None, compact = false, flatten = false))]
pub(crate) fn wkb_to_cells(
    py: Python<'_>,
    array: PyConcatedArray,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
    flatten: bool,
) -> PyArrowResult<PyObject> {
    let options = get_to_cells_options(resolution, containment_mode, compact)?;

    let array: PyArray = array.into();
    if let Some(binarray) = array.array().as_any().downcast_ref::<LargeBinaryArray>() {
        generic_wkb_to_cells(py, binarray.clone(), flatten, &options)
    } else if let Some(binarray) = array.array().as_any().downcast_ref::<BinaryArray>() {
        generic_wkb_to_cells(py, binarray.clone(), flatten, &options)
    } else {
        Err(PyValueError::new_err("unsupported array type for WKB input").into())
    }
}

fn generic_wkb_to_cells<O: OffsetSizeTrait>(
    py: Python<'_>,
    binarray: GenericBinaryArray<O>,
    flatten: bool,
    options: &ToCellsOptions,
) -> PyArrowResult<PyObject> {
    let wkbarray = WKBArray::new(binarray, Default::default());

    if flatten {
        let cells = py
            .allow_threads(|| wkbarray.to_cellindexarray(options))
            .into_pyresult()?;

        array_to_arro3(
            py,
            PrimitiveArray::from(cells),
            DEFAULT_CELL_COLUMN_NAME,
            true,
        )
    } else {
        let listarray: GenericListArray<O> = py
            .allow_threads(|| wkbarray.to_celllistarray(options))
            .into_pyresult()?
            .into();
        array_to_arro3(py, listarray, DEFAULT_CELL_COLUMN_NAME, true)
    }
}

#[pyfunction]
#[pyo3(signature = (obj, resolution, containment_mode = None, compact = false))]
pub(crate) fn geometry_to_cells(
    py: Python<'_>,
    obj: py_geo_interface::Geometry,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
) -> PyArrowResult<PyObject> {
    let cellindexarray = if obj.0.is_empty() {
        CellIndexArray::new_null(0)
    } else {
        let options = get_to_cells_options(resolution, containment_mode, compact)?;
        CellIndexArray::from(
            py.allow_threads(|| h3arrow::array::from_geo::geometry_to_cells(&obj.0, &options))
                .into_pyresult()?,
        )
    };
    array_to_arro3(
        py,
        PrimitiveArray::from(cellindexarray),
        DEFAULT_CELL_COLUMN_NAME,
        true,
    )
}

pub fn init_vector_submodule(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cells_to_coordinates, m)?)?;
    m.add_function(wrap_pyfunction!(cells_bounds, m)?)?;
    m.add_function(wrap_pyfunction!(cells_bounds_arrays, m)?)?;
    m.add_function(wrap_pyfunction!(cells_to_wkb_polygons, m)?)?;
    m.add_function(wrap_pyfunction!(cells_to_wkb_points, m)?)?;
    m.add_function(wrap_pyfunction!(vertexes_to_wkb_points, m)?)?;
    m.add_function(wrap_pyfunction!(directededges_to_wkb_linestrings, m)?)?;
    m.add_function(wrap_pyfunction!(wkb_to_cells, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_to_cells, m)?)?;
    m.add_function(wrap_pyfunction!(coordinates_to_cells, m)?)?;
    Ok(())
}
