use crate::array::to_geo::{
    IterLines, IterPoints, IterPolygons, ToLineStrings, ToPoints, ToPolygons,
};
use crate::array::{H3Array, H3IndexArrayValue};
use arrow::array::{Array, OffsetSizeTrait};
use geo::point;
use geo_types::LineString;
use geoarrow::array::{
    LineStringArray, PointArray, PolygonArray, WKBArray, WKBBuilder, WKBCapacity,
};

pub trait ToGeoArrowPolygons {
    type Error;
    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray<2>, Self::Error>;
}

impl<T> ToGeoArrowPolygons for T
where
    T: ToPolygons,
{
    type Error = T::Error;

    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray<2>, Self::Error> {
        Ok(self.to_polygons(use_degrees)?.into())
    }
}

pub trait ToGeoArrowPoints {
    type Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray<2>, Self::Error>;
}

impl<T> ToGeoArrowPoints for T
where
    T: ToPoints,
{
    type Error = T::Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray<2>, Self::Error> {
        Ok(self.to_points(use_degrees)?.into())
    }
}

pub trait ToGeoArrowLineStrings {
    type Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray<2>, Self::Error>;
}

impl<T> ToGeoArrowLineStrings for T
where
    T: ToLineStrings,
{
    type Error = T::Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray<2>, Self::Error> {
        Ok(self.to_linestrings(use_degrees)?.into())
    }
}

pub trait ToWKBPolygons {
    type Error;
    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBPolygons for H3Array<T>
where
    Self: IterPolygons,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterPolygons>::Error;

    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size. This may be off a bit and require
        // a re-allocation in case the first element is a pentagon
        let geometry_wkb_size = if let Some(first_value) = self
            .iter_polygons(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
        {
            let mut cap = WKBCapacity::new_empty();
            cap.add_polygon(Some(&first_value));
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
        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(
            num_non_null * geometry_wkb_size,
            self.len(),
        ));
        for poly in self.iter_polygons(use_degrees) {
            let poly = poly.transpose()?;
            builder.push_polygon(poly.as_ref())
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBLineStrings {
    type Error;
    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBLineStrings for H3Array<T>
where
    Self: IterLines,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterLines>::Error;

    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size. All geometries have the same number of coordinates
        let geometry_wkb_size = if let Some(first_value) = self
            .iter_lines(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
        {
            let mut cap = WKBCapacity::new_empty();
            cap.add_line_string(Some(&LineString::from(first_value)));
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

        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(
            num_non_null * geometry_wkb_size,
            self.len(),
        ));
        for line in self.iter_lines(use_degrees) {
            let linestring = line.transpose()?.map(LineString::from);
            builder.push_line_string(linestring.as_ref())
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBPoints {
    type Error;
    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBPoints for H3Array<T>
where
    Self: IterPoints,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterPoints>::Error;

    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        // just use the first value to estimate the required buffer size
        let geometry_wkb_size = if self
            .iter_points(use_degrees)
            .flat_map(|v| v.transpose().ok().flatten())
            .next()
            .is_some()
        {
            let mut cap = WKBCapacity::new_empty();
            let point = point! {x:0.0f64, y:0.0f64};
            cap.add_point(Some(&point));
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
        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(
            geometry_wkb_size * num_non_null,
            self.len(),
        ));
        for point in self.iter_points(use_degrees) {
            let point = point.transpose()?;
            builder.push_point(point.as_ref())
        }
        Ok(builder.finish())
    }
}
