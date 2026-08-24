use crate::array::to_geo::{
    IterLines, IterPoints, IterPolygons, ToLineStrings, ToPoints, ToPolygons,
};
use crate::array::{H3Array, H3IndexArrayValue};
use crate::error::Error;
use arrow::array::{Array, OffsetSizeTrait};
use geo::point;
use geo_types::LineString;
use geoarrow::array::{
    GenericWkbArray, LineStringArray, LineStringBuilder, PointArray, PointBuilder, PolygonArray,
    PolygonBuilder, WkbBuilder,
};
use geoarrow::datatypes::{Dimension, LineStringType, PointType, PolygonType};
use geoarrow_array::capacity::WkbCapacity;
use std::convert::Infallible;

pub trait ToGeoArrowPolygons {
    type Error;
    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray, Self::Error>;
}

impl<T> ToGeoArrowPolygons for T
where
    T: ToPolygons,
{
    type Error = T::Error;

    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray, Self::Error> {
        Ok(PolygonBuilder::from_nullable_polygons(
            &self.to_polygons(use_degrees)?,
            PolygonType::new(Dimension::XY, Default::default()),
        )
        .finish())
    }
}

pub trait ToGeoArrowPoints {
    type Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error>;
}

impl<T> ToGeoArrowPoints for T
where
    T: ToPoints,
{
    type Error = T::Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error> {
        Ok(PointBuilder::from_nullable_points(
            self.to_points(use_degrees)?.iter().map(|x| x.as_ref()),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish())
    }
}

pub trait ToGeoArrowLineStrings {
    type Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray, Self::Error>;
}

impl<T> ToGeoArrowLineStrings for T
where
    T: ToLineStrings,
{
    type Error = T::Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray, Self::Error> {
        Ok(LineStringBuilder::from_nullable_line_strings(
            &self.to_linestrings(use_degrees)?,
            LineStringType::new(Dimension::XY, Default::default()),
        )
        .finish())
    }
}

pub trait ToWKBPolygons {
    type Error;
    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error>;
}

impl<T> ToWKBPolygons for H3Array<T>
where
    Self: IterPolygons<Error = Infallible>,
    T: H3IndexArrayValue,
{
    type Error = Error;

    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size. This may be off a bit and require
        // a re-allocation in case the first element is a pentagon
        let geometry_wkb_size = if let Some(first_value) = self
            .iter_polygons(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
        {
            let mut cap = WkbCapacity::new_empty();
            cap.add_geometry(Some(&first_value));
            cap.buffer_capacity()
        } else {
            0
        };

        // number of non-null geometries
        let num_non_null = self.primitive_array.len().saturating_sub(
            self.primitive_array
                .nulls()
                .map(|nb| nb.null_count())
                .unwrap_or(0),
        );
        let mut builder = WkbBuilder::with_capacity(
            Default::default(),
            WkbCapacity::new(num_non_null * geometry_wkb_size, self.len()),
        );
        for poly in self.iter_polygons(use_degrees) {
            let poly = poly.transpose().unwrap_infallible();
            builder.push_geometry(poly.as_ref())?
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBLineStrings {
    type Error;
    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error>;
}

impl<T> ToWKBLineStrings for H3Array<T>
where
    Self: IterLines<Error = Infallible>,
    T: H3IndexArrayValue,
{
    type Error = Error;

    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size. All geometries have the same number of coordinates
        let geometry_wkb_size = if let Some(first_value) = self
            .iter_lines(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
        {
            let mut cap = WkbCapacity::new_empty();
            cap.add_geometry(Some(&LineString::from(first_value)));
            cap.buffer_capacity()
        } else {
            0
        };

        // number of non-null geometries
        let num_non_null = self.primitive_array.len().saturating_sub(
            self.primitive_array
                .nulls()
                .map(|nb| nb.null_count())
                .unwrap_or(0),
        );

        let mut builder = WkbBuilder::with_capacity(
            Default::default(),
            WkbCapacity::new(num_non_null * geometry_wkb_size, self.len()),
        );
        for line in self.iter_lines(use_degrees) {
            let linestring = line.transpose().unwrap_infallible().map(LineString::from);
            builder.push_geometry(linestring.as_ref())?
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBPoints {
    type Error;
    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error>;
}

impl<T> ToWKBPoints for H3Array<T>
where
    Self: IterPoints<Error = Infallible>,
    T: H3IndexArrayValue,
{
    type Error = Error;

    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<GenericWkbArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size
        let geometry_wkb_size = if self
            .iter_points(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
            .is_some()
        {
            let mut cap = WkbCapacity::new_empty();
            let point = point! {x:0.0f64, y:0.0f64};
            cap.add_geometry(Some(&point));
            cap.buffer_capacity()
        } else {
            0
        };

        // number of non-null geometries
        let num_non_null = self.primitive_array.len().saturating_sub(
            self.primitive_array
                .nulls()
                .map(|nb| nb.null_count())
                .unwrap_or(0),
        );
        let mut builder = WkbBuilder::with_capacity(
            Default::default(),
            WkbCapacity::new(geometry_wkb_size * num_non_null, self.len()),
        );
        for point in self.iter_points(use_degrees) {
            let point = point.transpose().unwrap_infallible();
            builder.push_geometry(point.as_ref())?;
        }
        Ok(builder.finish())
    }
}

trait UnwrapInfallible {
    type Value;
    fn unwrap_infallible(self) -> Self::Value;
}

impl<T> UnwrapInfallible for Result<T, Infallible> {
    type Value = T;
    fn unwrap_infallible(self) -> T {
        self.expect("unwrap_infallible called on Ok")
    }
}
