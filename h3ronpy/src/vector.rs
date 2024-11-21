use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, Float64Array, GenericBinaryArray, GenericListArray, OffsetSizeTrait,
    RecordBatch, UInt8Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use geo::{BoundingRect, HasDimensions, LineString, ToRadians};
use h3arrow::algorithm::ToCoordinatesOp;
use h3arrow::array::from_geo::{ToCellIndexArray, ToCellListArray, ToCellsOptions};
use h3arrow::array::to_geoarrow::{ToWKBLineStrings, ToWKBPoints, ToWKBPolygons};
use h3arrow::array::{CellIndexArray, ResolutionArray};
use h3arrow::export::geoarrow::array::{WKBArray, WKBBuilder, WKBCapacity};
use h3arrow::export::geoarrow::ArrayBase;
use h3arrow::export::h3o::geom::ContainmentMode;
use h3arrow::export::h3o::Resolution;
use h3arrow::h3o::geom::dissolve;
use h3arrow::h3o::LatLng;
use itertools::multizip;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyRecordBatch};

use crate::array::{PyCellArray, PyDirectedEdgeArray, PyVertexArray};
use crate::arrow_interop::*;
use crate::error::IntoPyResult;

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
#[pyclass(name = "ContainmentMode", eq, eq_int)]
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
pub(crate) fn cells_bounds(py: Python<'_>, cellarray: PyCellArray) -> PyResult<Option<PyObject>> {
    if let Some(rect) = py.allow_threads(|| cellarray.as_ref().bounding_rect()) {
        Ok(Some(
            PyTuple::new_bound(py, [rect.min().x, rect.min().y, rect.max().x, rect.max().y])
                .to_object(py),
        ))
    } else {
        Ok(None)
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray,))]
pub(crate) fn cells_bounds_arrays(py: Python, cellarray: PyCellArray) -> PyArrowResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
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
            if let Some(rect) = LineString::from(cell.boundary()).bounding_rect() {
                *validity = true;
                *minx = rect.min().x;
                *miny = rect.min().y;
                *maxx = rect.max().x;
                *maxy = rect.max().y;
            };
        }
    }

    let validity = NullBuffer::from(validity_vec);

    let schema = Schema::new(vec![
        Field::new("minx", DataType::Float64, true),
        Field::new("miny", DataType::Float64, true),
        Field::new("maxx", DataType::Float64, true),
        Field::new("maxy", DataType::Float64, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::new(minx_vec.into(), Some(validity.clone()))),
        Arc::new(Float64Array::new(miny_vec.into(), Some(validity.clone()))),
        Arc::new(Float64Array::new(maxx_vec.into(), Some(validity.clone()))),
        Arc::new(Float64Array::new(maxy_vec.into(), Some(validity.clone()))),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_coordinates(
    py: Python,
    cellarray: PyCellArray,
    radians: bool,
) -> PyArrowResult<PyObject> {
    let coordinate_arrays = if radians {
        cellarray.as_ref().to_coordinates_radians()
    } else {
        cellarray.as_ref().to_coordinates()
    }
    .into_pyresult()?;

    let schema = Schema::new(vec![
        Field::new("lat", DataType::Float64, true),
        Field::new("lng", DataType::Float64, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(coordinate_arrays.lat),
        Arc::new(coordinate_arrays.lng),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
    Ok(PyRecordBatch::new(batch).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (latarray, lngarray, resolution, radians = false))]
pub(crate) fn coordinates_to_cells(
    py: Python<'_>,
    latarray: &Bound<PyAny>,
    lngarray: &Bound<PyAny>,
    resolution: &Bound<PyAny>,
    radians: bool,
) -> PyResult<PyObject> {
    let latarray: Float64Array = pyarray_to_native(latarray)?;
    let lngarray: Float64Array = pyarray_to_native(lngarray)?;
    if lngarray.len() != latarray.len() {
        return Err(PyValueError::new_err(
            "latarray and lngarray must be of the same length",
        ));
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
        let resarray = ResolutionArray::try_from(pyarray_to_native::<UInt8Array>(resolution)?)
            .into_pyresult()?;

        if resarray.len() != latarray.len() {
            return Err(PyValueError::new_err(
                "resarray must be of the same length as the coordinate arrays",
            ));
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

    h3array_to_pyarray(cells, py)
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false, link_cells = false))]
pub(crate) fn cells_to_wkb_polygons(
    py: Python,
    cellarray: PyCellArray,
    radians: bool,
    link_cells: bool,
) -> PyResult<PyObject> {
    let cellindexarray = cellarray.into_inner();
    let use_degrees = !radians;

    let out: WKBArray<i64> = py.allow_threads(|| {
        if link_cells {
            let mut cells = cellindexarray.iter().flatten().collect::<Vec<_>>();
            cells.sort_unstable();
            cells.dedup();

            let geoms = dissolve(cells)
                .into_pyresult()?
                .into_iter()
                .map(|mut poly| {
                    if radians {
                        poly.to_radians_in_place();
                    }
                    Some(geo_types::Geometry::from(poly))
                })
                .collect::<Vec<_>>();
            let mut builder = WKBBuilder::with_capacity(WKBCapacity::from_geometries(
                geoms.iter().map(|v| v.as_ref()),
            ));
            builder.extend_from_iter(geoms.iter().map(|v| v.as_ref()));
            Ok::<_, PyErr>(builder.finish())
        } else {
            Ok(cellindexarray
                .to_wkb_polygons(use_degrees)
                .expect("wkbarray"))
        }
    })?;

    let field = out.extension_field();
    PyArray::new(out.into_array_ref(), field).to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_wkb_points(
    py: Python,
    cellarray: PyCellArray,
    radians: bool,
) -> PyResult<PyObject> {
    let out = py.allow_threads(|| {
        cellarray
            .as_ref()
            .to_wkb_points::<i64>(!radians)
            .expect("wkbarray")
    });

    let field = out.extension_field();
    PyArray::new(out.into_array_ref(), field).to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (vertexarray, radians = false))]
pub(crate) fn vertexes_to_wkb_points(
    py: Python,
    vertexarray: PyVertexArray,
    radians: bool,
) -> PyResult<PyObject> {
    let out = py.allow_threads(|| {
        vertexarray
            .as_ref()
            .to_wkb_points::<i64>(!radians)
            .expect("wkbarray")
    });

    let field = out.extension_field();
    PyArray::new(out.into_array_ref(), field).to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (array, radians = false))]
pub(crate) fn directededges_to_wkb_linestrings(
    py: Python,
    array: PyDirectedEdgeArray,
    radians: bool,
) -> PyResult<PyObject> {
    let out = py.allow_threads(|| {
        array
            .as_ref()
            .to_wkb_linestrings::<i64>(!radians)
            .expect("wkbarray")
    });

    let field = out.extension_field();
    PyArray::new(out.into_array_ref(), field).to_arro3(py)
}

fn get_to_cells_options(
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
) -> PyResult<ToCellsOptions> {
    Ok(
        ToCellsOptions::new(Resolution::try_from(resolution).into_pyresult()?)
            .containment_mode(containment_mode.unwrap_or_default().containment_mode())
            .compact(compact),
    )
}

#[pyfunction]
#[pyo3(signature = (array, resolution, containment_mode = None, compact = false, flatten = false))]
pub(crate) fn wkb_to_cells(
    py: Python,
    array: PyArray,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
    flatten: bool,
) -> PyResult<PyObject> {
    let options = get_to_cells_options(resolution, containment_mode, compact)?;

    match array.field().data_type() {
        DataType::Binary => generic_wkb_to_cells(
            py,
            array.array().as_binary::<i32>().clone(),
            flatten,
            &options,
        ),
        DataType::LargeBinary => generic_wkb_to_cells(
            py,
            array.array().as_binary::<i64>().clone(),
            flatten,
            &options,
        ),
        _ => Err(PyValueError::new_err(
            "unsupported array type for WKB input",
        )),
    }
}

fn generic_wkb_to_cells<O: OffsetSizeTrait>(
    py: Python,
    binarray: GenericBinaryArray<O>,
    flatten: bool,
    options: &ToCellsOptions,
) -> PyResult<PyObject> {
    let wkbarray = WKBArray::new(binarray, Default::default());

    if flatten {
        let cells = py
            .allow_threads(|| wkbarray.to_cellindexarray(options))
            .into_pyresult()?;

        h3array_to_pyarray(cells, py)
    } else {
        let listarray: GenericListArray<O> = py
            .allow_threads(|| wkbarray.to_celllistarray(options))
            .into_pyresult()?
            .into();
        PyArray::from_array_ref(Arc::new(listarray)).to_arro3(py)
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
) -> PyResult<PyObject> {
    if obj.0.is_empty() {
        return h3array_to_pyarray(CellIndexArray::new_null(0), py);
    }
    let options = get_to_cells_options(resolution, containment_mode, compact)?;
    let cellindexarray = py.allow_threads(|| {
        Ok::<_, PyErr>(CellIndexArray::from(
            h3arrow::array::from_geo::geometry_to_cells(&obj.0, &options).into_pyresult()?,
        ))
    })?;
    h3array_to_pyarray(cellindexarray, py)
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
