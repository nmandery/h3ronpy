use crate::array::to_geo::{
    cellindexarray_to_multipolygon, directededgeindexarray_to_multipoint,
    vertexindexarray_to_multipoint,
};
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use geo::convex_hull::ConvexHull;
use geo_types::Polygon;

impl<'a> ConvexHull<'a, f64> for CellIndexArray {
    type Scalar = f64;

    fn convex_hull(&'a self) -> Polygon<Self::Scalar> {
        cellindexarray_to_multipolygon(self).convex_hull()
    }
}

impl<'a> ConvexHull<'a, f64> for VertexIndexArray {
    type Scalar = f64;

    fn convex_hull(&'a self) -> Polygon<Self::Scalar> {
        vertexindexarray_to_multipoint(self).convex_hull()
    }
}

impl<'a> ConvexHull<'a, f64> for DirectedEdgeIndexArray {
    type Scalar = f64;

    fn convex_hull(&'a self) -> Polygon<Self::Scalar> {
        directededgeindexarray_to_multipoint(self).convex_hull()
    }
}
