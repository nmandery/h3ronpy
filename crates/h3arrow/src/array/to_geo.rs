use crate::array::{
    CellIndexArray, DirectedEdgeIndexArray, H3ListArray, PrimitiveArrayH3IndexIter,
    VertexIndexArray,
};
use crate::error::Error;
use geo::{CoordsIter, ToRadians};
use geo_types::{Coord, Line, LineString, MultiPoint, MultiPolygon, Point, Polygon};
use h3o::{CellIndex, DirectedEdgeIndex, LatLng, VertexIndex};
use std::convert::Infallible;
use std::iter::{repeat, Map, Repeat, Zip};

pub trait IterPolygons {
    type Error;

    type Iter<'a>: Iterator<Item = Option<Result<Polygon, Self::Error>>>
    where
        Self: 'a;

    fn iter_polygons(&self, use_degrees: bool) -> Self::Iter<'_>;
}

impl IterPolygons for CellIndexArray {
    type Error = Infallible;
    type Iter<'a> = Map<
        Zip<PrimitiveArrayH3IndexIter<'a, CellIndex>, Repeat<bool>>,
        fn((Option<CellIndex>, bool)) -> Option<Result<Polygon, Self::Error>>,
    >;

    fn iter_polygons(&self, use_degrees: bool) -> Self::Iter<'_> {
        self.iter()
            .zip(repeat(use_degrees))
            .map(|(v, use_degrees)| {
                v.map(|cell| {
                    let mut poly = Polygon::new(LineString::from(cell.boundary()), vec![]);
                    if !use_degrees {
                        poly.to_radians_in_place();
                    }
                    Ok(poly)
                })
            })
    }
}

pub trait ToPolygons {
    type Error;
    fn to_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error>;
}

impl<T> ToPolygons for T
where
    T: IterPolygons,
{
    type Error = <T as IterPolygons>::Error;

    fn to_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error> {
        self.iter_polygons(use_degrees)
            .map(|p| p.transpose())
            .collect()
    }
}

pub trait IterPoints {
    type Error;

    type Iter<'a>: Iterator<Item = Option<Result<Point, Self::Error>>>
    where
        Self: 'a;

    fn iter_points(&self, use_degrees: bool) -> Self::Iter<'_>;
}

macro_rules! impl_iter_points {
    ($($array:ty, $index_type:ty),*) => {
        $(
            impl IterPoints for $array {
                type Error = Infallible;
                type Iter<'a> = Map<
                    Zip<PrimitiveArrayH3IndexIter<'a, $index_type>, Repeat<bool>>,
                    fn((Option<$index_type>, bool)) -> Option<Result<Point, Self::Error>>,
                >;

                fn iter_points(&self, use_degrees: bool) -> Self::Iter<'_> {
                    self.iter()
                        .zip(repeat(use_degrees))
                        .map(|(v, use_degrees)| {
                            v.map(|cell| {
                                let ll = LatLng::from(cell);
                                let pt: Point = if use_degrees {
                                    Coord {
                                        x: ll.lng(),
                                        y: ll.lat(),
                                    }
                                    .into()
                                } else {
                                    Coord {
                                        x: ll.lng_radians(),
                                        y: ll.lat_radians(),
                                    }
                                    .into()
                                };
                                Ok(pt)
                            })
                        })
                }
            }

        )*
    };
}

impl_iter_points!(CellIndexArray, CellIndex, VertexIndexArray, VertexIndex);

pub trait ToPoints {
    type Error;
    fn to_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error>;
}

impl<T> ToPoints for T
where
    T: IterPoints,
{
    type Error = <T as IterPoints>::Error;

    fn to_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error> {
        self.iter_points(use_degrees)
            .map(|p| p.transpose())
            .collect()
    }
}

pub trait IterLines {
    type Error;

    type Iter<'a>: Iterator<Item = Option<Result<Line, Self::Error>>>
    where
        Self: 'a;

    fn iter_lines(&self, use_degrees: bool) -> Self::Iter<'_>;
}

impl IterLines for DirectedEdgeIndexArray {
    type Error = Infallible;
    type Iter<'a> = Map<
        Zip<PrimitiveArrayH3IndexIter<'a, DirectedEdgeIndex>, Repeat<bool>>,
        fn((Option<DirectedEdgeIndex>, bool)) -> Option<Result<Line, Self::Error>>,
    >;

    fn iter_lines(&self, use_degrees: bool) -> Self::Iter<'_> {
        self.iter()
            .zip(repeat(use_degrees))
            .map(|(v, use_degrees)| {
                v.map(|edge| {
                    let mut line = Line::from(edge);
                    if !use_degrees {
                        line.to_radians_in_place();
                    }
                    Ok(line)
                })
            })
    }
}

pub trait ToLines {
    type Error;
    fn to_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error>;
}

impl ToLines for DirectedEdgeIndexArray {
    type Error = Infallible;

    fn to_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error> {
        self.iter_lines(use_degrees)
            .map(|v| v.transpose())
            .collect()
    }
}

pub trait ToLineStrings {
    type Error;
    fn to_linestrings(&self, use_degrees: bool) -> Result<Vec<Option<LineString>>, Self::Error>;
}

impl ToLineStrings for DirectedEdgeIndexArray {
    type Error = Infallible;
    fn to_linestrings(&self, use_degrees: bool) -> Result<Vec<Option<LineString>>, Self::Error> {
        self.iter_lines(use_degrees)
            .map(|v| v.transpose().map(|res| res.map(LineString::from)))
            .collect()
    }
}

pub trait ToMultiPolygons {
    type Error;
    type Output;
    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error>;
}

impl ToMultiPolygons for H3ListArray<CellIndex> {
    type Error = Error;
    type Output = Vec<Option<MultiPolygon>>;

    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
        self.iter_arrays()
            .map(|opt| {
                opt.map(|res| {
                    res.and_then(|array| {
                        array
                            .to_multipolygons(use_degrees)
                            .map_err(Self::Error::from)
                    })
                })
                .transpose()
            })
            .collect()
    }
}

impl ToMultiPolygons for CellIndexArray {
    type Error = Error;
    type Output = MultiPolygon;

    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
        let mut multi_polygons = h3o::geom::dissolve(self.iter().flatten())?;
        if !use_degrees {
            multi_polygons.to_radians_in_place();
        }
        Ok(multi_polygons)
    }
}

/// used as base for the algorithms of the `geo` crate
pub(crate) fn directededgeindexarray_to_multipoint(array: &DirectedEdgeIndexArray) -> MultiPoint {
    MultiPoint::new(
        array
            .to_lines(true)
            .expect("line vec")
            .into_iter()
            .flatten()
            .flat_map(|line| line.coords_iter().map(Point::from))
            .collect(),
    )
}

/// used as base for the algorithms of the `geo` crate
pub(crate) fn vertexindexarray_to_multipoint(array: &VertexIndexArray) -> MultiPoint {
    MultiPoint::new(
        array
            .to_points(true)
            .expect("point vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}

pub(crate) fn cellindexarray_to_multipolygon(array: &CellIndexArray) -> MultiPolygon {
    MultiPolygon::new(
        array
            .to_polygons(true)
            .expect("polygon vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}
