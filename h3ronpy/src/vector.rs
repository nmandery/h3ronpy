use arrow::array::{
    make_array, Array, ArrayData, BinaryArray, Float64Array, GenericBinaryArray, GenericListArray,
    LargeBinaryArray, OffsetSizeTrait, UInt8Array,
};
use arrow::buffer::NullBuffer;
use arrow::pyarrow::{FromPyArrow, IntoPyArrow, ToPyArrow};
use geo::{BoundingRect, HasDimensions};
use h3arrow::algorithm::ToCoordinatesOp;
use h3arrow::array::from_geo::{ToCellIndexArray, ToCellListArray, ToCellsOptions};
use h3arrow::array::to_geoarrow::{ToWKBLineStrings, ToWKBPoints, ToWKBPolygons};
use h3arrow::array::{CellIndexArray, ResolutionArray};
use h3arrow::export::geoarrow::array::{WKBArray, WKBBuilder, WKBCapacity};
use h3arrow::export::h3o::geom::{ContainmentMode, ToGeo};
use h3arrow::export::h3o::Resolution;
use h3arrow::h3o::geom::PolyfillConfig;
use h3arrow::h3o::LatLng;
use itertools::multizip;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

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
pub(crate) fn cells_bounds(cellarray: &PyAny) -> PyResult<Option<PyObject>> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    if let Some(rect) = cellindexarray.bounding_rect() {
        Python::with_gil(|py| {
            Ok(Some(
                PyTuple::new(py, [rect.min().x, rect.min().y, rect.max().x, rect.max().y])
                    .to_object(py),
            ))
        })
    } else {
        Ok(None)
    }
}

#[pyfunction]
#[pyo3(signature = (cellarray,))]
pub(crate) fn cells_bounds_arrays(cellarray: &PyAny) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
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

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            Float64Array::new(minx_vec.into(), Some(validity.clone()))
                .into_data()
                .into_pyarrow(py)?,
            Float64Array::new(miny_vec.into(), Some(validity.clone()))
                .into_data()
                .into_pyarrow(py)?,
            Float64Array::new(maxx_vec.into(), Some(validity.clone()))
                .into_data()
                .into_pyarrow(py)?,
            Float64Array::new(maxy_vec.into(), Some(validity))
                .into_data()
                .into_pyarrow(py)?,
        ];
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_arrays", (arrays, ["minx", "miny", "maxx", "maxy"]))?;
        Ok(table.to_object(py))
    })
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_coordinates(cellarray: &PyAny, radians: bool) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;

    let coordinate_arrays = if radians {
        cellindexarray.to_coordinates_radians()
    } else {
        cellindexarray.to_coordinates()
    }
    .into_pyresult()?;

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            coordinate_arrays.lat.into_data().into_pyarrow(py)?,
            coordinate_arrays.lng.into_data().into_pyarrow(py)?,
        ];
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_arrays", (arrays, ["lat", "lng"]))?;
        Ok(table.to_object(py))
    })
}

#[pyfunction]
#[pyo3(signature = (latarray, lngarray, resolution, radians = false))]
pub(crate) fn coordinates_to_cells(
    latarray: &PyAny,
    lngarray: &PyAny,
    resolution: &PyAny,
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
            .collect::<PyResult<CellIndexArray>>()?
    } else {
        let resarray = ResolutionArray::try_from(pyarray_to_native::<UInt8Array>(resolution)?)
            .into_pyresult()?;

        if resarray.len() != latarray.len() {
            return Err(PyValueError::new_err(
                "resarray must be of the same length as the coordinate arrays",
            ));
        }

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
            .collect::<PyResult<CellIndexArray>>()?
    };

    Python::with_gil(|py| h3array_to_pyarray(cells, py))
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false, link_cells = false))]
pub(crate) fn cells_to_wkb_polygons(
    cellarray: &PyAny,
    radians: bool,
    link_cells: bool,
) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let use_degrees = !radians;

    let out: WKBArray<i64> = if link_cells {
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
        builder.finish()
    } else {
        cellindexarray
            .to_wkb_polygons(use_degrees)
            .expect("wkbarray")
    };

    Python::with_gil(|py| out.into_inner().into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_wkb_points(cellarray: &PyAny, radians: bool) -> PyResult<PyObject> {
    let out = pyarray_to_cellindexarray(cellarray)?
        .to_wkb_points::<i64>(!radians)
        .expect("wkbarray");

    Python::with_gil(|py| out.into_inner().into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (vertexarray, radians = false))]
pub(crate) fn vertexes_to_wkb_points(vertexarray: &PyAny, radians: bool) -> PyResult<PyObject> {
    let out = pyarray_to_vertexindexarray(vertexarray)?
        .to_wkb_points::<i64>(!radians)
        .expect("wkbarray");

    Python::with_gil(|py| out.into_inner().into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (array, radians = false))]
pub(crate) fn directededges_to_wkb_linestrings(array: &PyAny, radians: bool) -> PyResult<PyObject> {
    let out = pyarray_to_directededgeindexarray(array)?
        .to_wkb_linestrings::<i64>(!radians)
        .expect("wkbarray");

    Python::with_gil(|py| out.into_inner().into_data().into_pyarrow(py))
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
    array: &PyAny,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
    flatten: bool,
) -> PyResult<PyObject> {
    let options = get_to_cells_options(resolution, containment_mode, compact)?;
    let array_ref = make_array(ArrayData::from_pyarrow(array)?);

    if let Some(binarray) = array_ref.as_any().downcast_ref::<LargeBinaryArray>() {
        generic_wkb_to_cells(binarray.clone(), flatten, &options)
    } else if let Some(binarray) = array_ref.as_any().downcast_ref::<BinaryArray>() {
        generic_wkb_to_cells(binarray.clone(), flatten, &options)
    } else {
        Err(PyValueError::new_err(
            "unsupported array type for WKB input",
        ))
    }
}

fn generic_wkb_to_cells<O: OffsetSizeTrait>(
    binarray: GenericBinaryArray<O>,
    flatten: bool,
    options: &ToCellsOptions,
) -> PyResult<PyObject> {
    let wkbarray = WKBArray::new(binarray, Default::default());

    if flatten {
        let cells = wkbarray.to_cellindexarray(options).into_pyresult()?;

        Python::with_gil(|py| h3array_to_pyarray(cells, py))
    } else {
        let listarray: GenericListArray<O> =
            wkbarray.to_celllistarray(options).into_pyresult()?.into();
        Python::with_gil(|py| listarray.into_data().to_pyarrow(py))
    }
}

#[pyfunction]
#[pyo3(signature = (obj, resolution, containment_mode = None, compact = false))]
pub(crate) fn geometry_to_cells(
    obj: py_geo_interface::Geometry,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
) -> PyResult<PyObject> {
    if obj.0.is_empty() {
        return Python::with_gil(|py| h3array_to_pyarray(CellIndexArray::new_null(0), py));
    }
    let options = get_to_cells_options(resolution, containment_mode, compact)?;
    let cellindexarray = CellIndexArray::from(
        h3arrow::array::from_geo::geometry_to_cells(&obj.0, &options).into_pyresult()?,
    );
    Python::with_gil(|py| h3array_to_pyarray(cellindexarray, py))
}

pub fn init_vector_submodule(m: &PyModule) -> PyResult<()> {
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
