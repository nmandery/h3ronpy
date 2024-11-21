use geo_types::Point;
use pyo3_arrow::PyArray;
use std::hash::Hash;
use std::iter::repeat;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use geo::{AffineOps, AffineTransform};
use h3arrow::array::CellIndexArray;
use h3arrow::export::h3o::{CellIndex, Resolution};
use ndarray::ArrayView2;
use numpy::PyReadonlyArray2;
use ordered_float::OrderedFloat;
use pyo3::exceptions::PyValueError;
use pyo3::{prelude::*, wrap_pyfunction};

use crate::arrow_interop::h3array_to_pyarray;
use crate::error::IntoPyResult;
use crate::transform::Transform;

pub struct AxisOrder {
    pub inner: rasterh3::AxisOrder,
}

impl FromStr for AxisOrder {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "yx" | "YX" => Ok(Self {
                inner: rasterh3::AxisOrder::YX,
            }),
            "xy" | "XY" => Ok(Self {
                inner: rasterh3::AxisOrder::XY,
            }),
            _ => Err(PyValueError::new_err("unknown axis order")),
        }
    }
}

fn sanitycheck_wgs84_bounds(
    transform: &AffineTransform<f64>,
    axis_order: &rasterh3::AxisOrder,
    shape: &(usize, usize),
) -> PyResult<()> {
    let mn = Point::from((0.0, 0.0)).affine_transform(transform);
    let mx = match axis_order {
        rasterh3::AxisOrder::XY => Point::from((shape.0 as f64, shape.1 as f64)),
        rasterh3::AxisOrder::YX => Point::from((shape.1 as f64, shape.0 as f64)),
    }
    .affine_transform(transform);

    // note: coordinates itself are not validated as multiple rotations around an axis are
    //       still perfectly valid.

    if mx.x() - mn.x() > 361.0 || mx.y() - mn.y() > 181.0 {
        Err(PyValueError::new_err(
            "Input array spans significantly more than the bounds of WGS84 - input needs to be in WGS84 projection with lat/lon coordinates",
        ))
    } else {
        Ok(())
    }
}

pub struct ResolutionSearchMode {
    pub inner: rasterh3::ResolutionSearchMode,
}

impl FromStr for ResolutionSearchMode {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "min_diff" | "min-diff" => Ok(Self {
                inner: rasterh3::ResolutionSearchMode::MinDiff,
            }),
            "smaller_than_pixel" | "smaller-than-pixel" => Ok(Self {
                inner: rasterh3::ResolutionSearchMode::SmallerThanPixel,
            }),
            _ => Err(PyValueError::new_err("unknown resolution search mode")),
        }
    }
}

/// find the h3 resolution closed to the size of a pixel in an array
/// of the given shape with the given transform
#[pyfunction]
pub fn nearest_h3_resolution(
    shape: [usize; 2],
    transform: &Transform,
    axis_order_str: &str,
    search_mode_str: &str,
) -> PyResult<u8> {
    let axis_order = AxisOrder::from_str(axis_order_str)?;
    sanitycheck_wgs84_bounds(&transform.inner, &axis_order.inner, &(shape[0], shape[1]))?;
    let search_mode = ResolutionSearchMode::from_str(search_mode_str)?;

    search_mode
        .inner
        .nearest_h3_resolution(shape, &transform.inner, &axis_order.inner)
        .into_pyresult()
        .map(Into::into)
}

#[allow(clippy::type_complexity)]
fn raster_to_h3<'a, T>(
    arr: &'a ArrayView2<'a, T>,
    transform: &'a Transform,
    nodata_value: &'a Option<T>,
    h3_resolution: u8,
    axis_order_str: &str,
    compact: bool,
) -> PyResult<(Vec<T>, Vec<CellIndex>)>
where
    T: PartialEq + Sized + Sync + Eq + Hash + Copy,
{
    let axis_order = AxisOrder::from_str(axis_order_str)?;
    sanitycheck_wgs84_bounds(&transform.inner, &axis_order.inner, &arr.dim())?;

    let h3_resolution = Resolution::try_from(h3_resolution).into_pyresult()?;

    let conv = rasterh3::H3Converter::new(arr, nodata_value, &transform.inner, axis_order.inner);

    let mut values = vec![];
    let mut cells = vec![];
    for (value, cell_coverage) in conv.to_h3(h3_resolution, compact).into_pyresult()? {
        let len_before = cells.len();
        if compact {
            cells.extend(cell_coverage.into_compacted_iter());
        } else {
            cells.extend(cell_coverage.into_uncompacted_iter(h3_resolution));
        };
        values.extend(repeat(*value).take(cells.len() - len_before));
    }
    Ok((values, cells))
}

macro_rules! make_raster_to_h3_variant {
    ($name:ident, $dtype:ty, $array_dtype:ty) => {
        #[pyfunction]
        #[pyo3(signature = (np_array, transform, h3_resolution, axis_order_str, compact, nodata_value=None))]
        fn $name(
            py: Python,
            np_array: PyReadonlyArray2<$dtype>,
            transform: &Transform,
            h3_resolution: u8,
            axis_order_str: &str,
            compact: bool,
            nodata_value: Option<$dtype>,
        ) -> PyResult<(PyObject, PyObject)> {
            let arr = np_array.as_array();
            let (values, cells) = py.allow_threads(|| raster_to_h3(
                &arr,
                transform,
                &nodata_value,
                h3_resolution,
                axis_order_str,
                compact,
            ).map(|(values, cells)| (<$array_dtype>::from(values), cells)))?;

            let values = PyArray::from_array_ref(Arc::new(values)).to_arro3(py)?;
            let cells = h3array_to_pyarray(CellIndexArray::from(cells), py)?;

            Ok((values, cells))
        }
    };
}

macro_rules! make_raster_to_h3_float_variant {
    ($name:ident, $dtype:ty, $array_dtype:ty) => {
        #[pyfunction]
        #[pyo3(signature = (np_array, transform, h3_resolution, axis_order_str, compact, nodata_value=None))]
        fn $name(
            py: Python,
            np_array: PyReadonlyArray2<$dtype>,
            transform: &Transform,
            h3_resolution: u8,
            axis_order_str: &str,
            compact: bool,
            nodata_value: Option<$dtype>,
        ) -> PyResult<(PyObject, PyObject)> {
            let arr = np_array.as_array();
            // create a copy with the values wrapped in ordered floats to
            // support the internal hashing
            let of_arr = arr.map(|v| OrderedFloat::from(*v));
            let (values, cells) = py.allow_threads(|| raster_to_h3(
                &of_arr.view(),
                transform,
                &nodata_value.map(OrderedFloat::from),
                h3_resolution,
                axis_order_str,
                compact,
            ).map(|(values, cells)| (
                <$array_dtype>::from(values.into_iter().map(|v| v.into_inner()).collect::<Vec<$dtype>>()),
                cells)))?;

            let values = PyArray::from_array_ref(Arc::new(values)).to_arro3(py)?;
            let cells = h3array_to_pyarray(CellIndexArray::from(cells), py)?;

            Ok((values, cells))
        }
    };
}

// generate some specialized variants of raster_to_h3 to expose to python
make_raster_to_h3_variant!(raster_to_h3_u8, u8, UInt8Array);
make_raster_to_h3_variant!(raster_to_h3_i8, i8, Int8Array);
make_raster_to_h3_variant!(raster_to_h3_u16, u16, UInt16Array);
make_raster_to_h3_variant!(raster_to_h3_i16, i16, Int16Array);
make_raster_to_h3_variant!(raster_to_h3_u32, u32, UInt32Array);
make_raster_to_h3_variant!(raster_to_h3_i32, i32, Int32Array);
make_raster_to_h3_variant!(raster_to_h3_u64, u64, UInt64Array);
make_raster_to_h3_variant!(raster_to_h3_i64, i64, Int64Array);
make_raster_to_h3_float_variant!(raster_to_h3_f32, f32, Float32Array);
make_raster_to_h3_float_variant!(raster_to_h3_f64, f64, Float64Array);

pub fn init_raster_submodule(m: &Bound<PyModule>) -> PyResult<()> {
    m.add("Transform", m.py().get_type_bound::<Transform>())?;

    m.add_function(wrap_pyfunction!(nearest_h3_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_u8, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_i8, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_u16, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_i16, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_u32, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_i32, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_u64, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_i64, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_f32, m)?)?;
    m.add_function(wrap_pyfunction!(raster_to_h3_f64, m)?)?;

    Ok(())
}
