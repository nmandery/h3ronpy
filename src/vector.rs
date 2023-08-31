use geo::BoundingRect;
use h3arrow::algorithm::ToCoordinatesOp;
use h3arrow::array::from_geo::{ToCellIndexArray, ToCellListArray, ToCellsOptions};
use h3arrow::array::to_geoarrow::{ToWKBLineStrings, ToWKBLines, ToWKBPoints, ToWKBPolygons};
use h3arrow::array::{CellIndexArray, ResolutionArray};
use h3arrow::export::arrow2::array::{BinaryArray, Float64Array, ListArray, UInt8Array};
use h3arrow::export::arrow2::bitmap::Bitmap;
use h3arrow::export::geoarrow::{array::WKBArray, GeometryArrayTrait};
use h3arrow::export::h3o::geom::{ContainmentMode, ToGeo};
use h3arrow::export::h3o::Resolution;
use h3arrow::h3o::geom::PolyfillConfig;
use h3arrow::h3o::LatLng;
use itertools::multizip;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::arrow_interop::*;
use crate::error::{warn_deprecated, IntoPyResult};

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
#[pyclass(name = "ContainmentMode")]
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum PyContainmentMode {
    ContainsCentroid,
    ContainsBoundary,
    IntersectsBoundary,
}

impl PyContainmentMode {
    fn containment_mode(&self) -> ContainmentMode {
        match self {
            PyContainmentMode::ContainsCentroid => ContainmentMode::ContainsCentroid,
            PyContainmentMode::ContainsBoundary => ContainmentMode::ContainsBoundary,
            PyContainmentMode::IntersectsBoundary => ContainmentMode::IntersectsBoundary,
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

    let validity_bm = Bitmap::from(validity_vec.as_slice());

    with_pyarrow(|py, pyarrow| {
        let arrays = [
            native_to_pyarray(
                Float64Array::from_vec(minx_vec)
                    .with_validity(Some(validity_bm.clone()))
                    .boxed(),
                py,
                pyarrow,
            )?,
            native_to_pyarray(
                Float64Array::from_vec(miny_vec)
                    .with_validity(Some(validity_bm.clone()))
                    .boxed(),
                py,
                pyarrow,
            )?,
            native_to_pyarray(
                Float64Array::from_vec(maxx_vec)
                    .with_validity(Some(validity_bm.clone()))
                    .boxed(),
                py,
                pyarrow,
            )?,
            native_to_pyarray(
                Float64Array::from_vec(maxy_vec)
                    .with_validity(Some(validity_bm.clone()))
                    .boxed(),
                py,
                pyarrow,
            )?,
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
            native_to_pyarray(coordinate_arrays.lat.boxed(), py, pyarrow)?,
            native_to_pyarray(coordinate_arrays.lng.boxed(), py, pyarrow)?,
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
                        LatLng::from_radians(*lat, *lng).into_pyresult()
                    } else {
                        LatLng::new(*lat, *lng).into_pyresult()
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
                        LatLng::from_radians(*lat, *lng).into_pyresult()
                    } else {
                        LatLng::new(*lat, *lng).into_pyresult()
                    }
                    .map(|ll| Some(ll.to_cell(res)))
                } else {
                    Ok(None)
                }
            })
            .collect::<PyResult<CellIndexArray>>()?
    };

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(cells, py, pyarrow))
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

    let cells = if link_cells {
        WKBArray::from(
            cellindexarray
                .iter()
                .flatten()
                .to_geom(use_degrees)
                .into_pyresult()?
                .0
                .into_iter()
                .map(|poly| Some(geo_types::Geometry::from(poly)))
                .collect::<Vec<_>>(),
        )
    } else {
        cellindexarray.to_wkb_polygons(use_degrees).unwrap()
    }
    .into_arrow()
    .boxed();

    with_pyarrow(|py, pyarrow| native_to_pyarray(cells, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (cellarray, radians = false))]
pub(crate) fn cells_to_wkb_points(cellarray: &PyAny, radians: bool) -> PyResult<PyObject> {
    let cellindexarray = pyarray_to_cellindexarray(cellarray)?;
    let out = cellindexarray
        .to_wkb_points(!radians)
        .unwrap()
        .into_arrow()
        .boxed();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (vertexarray, radians = false))]
pub(crate) fn vertexes_to_wkb_points(vertexarray: &PyAny, radians: bool) -> PyResult<PyObject> {
    let vertexindexarray = pyarray_to_vertexindexarray(vertexarray)?;
    let out = vertexindexarray
        .to_wkb_points(!radians)
        .unwrap()
        .into_arrow()
        .boxed();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (array, radians = false))]
pub(crate) fn directededges_to_wkb_lines(array: &PyAny, radians: bool) -> PyResult<PyObject> {
    let array = pyarray_to_directededgeindexarray(array)?;
    let out = array.to_wkb_lines(!radians).unwrap().into_arrow().boxed();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (array, radians = false))]
pub(crate) fn directededges_to_wkb_linestrings(array: &PyAny, radians: bool) -> PyResult<PyObject> {
    let array = pyarray_to_directededgeindexarray(array)?;
    let out = array
        .to_wkb_linestrings(!radians)
        .unwrap()
        .into_arrow()
        .boxed();
    with_pyarrow(|py, pyarrow| native_to_pyarray(out, py, pyarrow))
}

fn get_containment_mode(
    pycm: Option<PyContainmentMode>,
    all_intersecting: Option<bool>,
) -> PyResult<ContainmentMode> {
    if all_intersecting.is_some() {
        warn_deprecated("The all_intersecting parameter is deprecated and will be removed. Use containment_mode instead.")?;
    }
    if let Some(pycm) = pycm {
        return Ok(pycm.containment_mode());
    }
    if all_intersecting == Some(true) {
        return Ok(ContainmentMode::IntersectsBoundary);
    }
    Ok(ContainmentMode::ContainsCentroid)
}

fn get_to_cells_options(
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    all_intersecting: Option<bool>,
    compact: bool,
) -> PyResult<ToCellsOptions> {
    Ok(ToCellsOptions::new(
        PolyfillConfig::new(Resolution::try_from(resolution).into_pyresult()?)
            .containment_mode(get_containment_mode(containment_mode, all_intersecting)?),
    )
    .compact(compact))
}

#[pyfunction]
#[pyo3(signature = (array, resolution, containment_mode = None, compact = false, all_intersecting = None, flatten = false))]
pub(crate) fn wkb_to_cells(
    array: &PyAny,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
    all_intersecting: Option<bool>, // todo: DEPRECATED. Remove in v0.19
    flatten: bool,
) -> PyResult<PyObject> {
    let options = get_to_cells_options(resolution, containment_mode, all_intersecting, compact)?;
    let wkbarray = WKBArray::new(pyarray_to_native::<BinaryArray<i64>>(array)?);

    if flatten {
        let cells = wkbarray.to_cellindexarray(&options).into_pyresult()?;

        with_pyarrow(|py, pyarrow| h3array_to_pyarray(cells, py, pyarrow))
    } else {
        let listarray: ListArray<_> = wkbarray.to_celllistarray(&options).into_pyresult()?.into();
        with_pyarrow(|py, pyarrow| native_to_pyarray(listarray.boxed(), py, pyarrow))
    }
}

#[pyfunction]
#[pyo3(signature = (obj, resolution, containment_mode = None, compact = false, all_intersecting = None))]
pub(crate) fn geometry_to_cells(
    obj: py_geo_interface::Geometry,
    resolution: u8,
    containment_mode: Option<PyContainmentMode>,
    compact: bool,
    all_intersecting: Option<bool>,
) -> PyResult<PyObject> {
    let options = get_to_cells_options(resolution, containment_mode, all_intersecting, compact)?;
    let cellindexarray = CellIndexArray::from(
        h3arrow::array::from_geo::geometry_to_cells(&obj.0, &options).into_pyresult()?,
    );
    with_pyarrow(|py, pyarrow| h3array_to_pyarray(cellindexarray, py, pyarrow))
}

pub fn init_vector_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cells_to_coordinates, m)?)?;
    m.add_function(wrap_pyfunction!(cells_bounds, m)?)?;
    m.add_function(wrap_pyfunction!(cells_bounds_arrays, m)?)?;
    m.add_function(wrap_pyfunction!(cells_to_wkb_polygons, m)?)?;
    m.add_function(wrap_pyfunction!(cells_to_wkb_points, m)?)?;
    m.add_function(wrap_pyfunction!(vertexes_to_wkb_points, m)?)?;
    m.add_function(wrap_pyfunction!(directededges_to_wkb_linestrings, m)?)?;
    m.add_function(wrap_pyfunction!(directededges_to_wkb_lines, m)?)?;
    m.add_function(wrap_pyfunction!(wkb_to_cells, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_to_cells, m)?)?;
    m.add_function(wrap_pyfunction!(coordinates_to_cells, m)?)?;
    Ok(())
}
