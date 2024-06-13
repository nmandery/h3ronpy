use crate::array::CellIndexArray;
use crate::error::Error;
use arrow::array::{Float64Array, Float64Builder};
use h3o::LatLng;

pub struct CoordinateArrays {
    pub lat: Float64Array,
    pub lng: Float64Array,
}

pub trait ToCoordinatesOp {
    /// convert to point coordinates in degrees
    fn to_coordinates(&self) -> Result<CoordinateArrays, Error>;

    /// convert to point coordinates in radians
    fn to_coordinates_radians(&self) -> Result<CoordinateArrays, Error>;
}

impl ToCoordinatesOp for CellIndexArray {
    fn to_coordinates(&self) -> Result<CoordinateArrays, Error> {
        Ok(to_coordinatearrays(self, |ll| ll.lat(), |ll| ll.lng()))
    }

    fn to_coordinates_radians(&self) -> Result<CoordinateArrays, Error> {
        Ok(to_coordinatearrays(
            self,
            |ll| ll.lat_radians(),
            |ll| ll.lng_radians(),
        ))
    }
}

fn to_coordinatearrays<ExtractLat, ExtractLng>(
    cellindexarray: &CellIndexArray,
    extract_lat: ExtractLat,
    extract_lng: ExtractLng,
) -> CoordinateArrays
where
    ExtractLat: Fn(&LatLng) -> f64,
    ExtractLng: Fn(&LatLng) -> f64,
{
    let mut lat_builder = Float64Builder::with_capacity(cellindexarray.len());
    let mut lng_builder = Float64Builder::with_capacity(cellindexarray.len());

    cellindexarray.iter().for_each(|cell| {
        if let Some(cell) = cell {
            let ll = LatLng::from(cell);
            lat_builder.append_value(extract_lat(&ll));
            lng_builder.append_value(extract_lng(&ll));
        } else {
            lat_builder.append_null();
            lng_builder.append_null();
        }
    });

    CoordinateArrays {
        lat: lat_builder.finish(),
        lng: lng_builder.finish(),
    }
}
