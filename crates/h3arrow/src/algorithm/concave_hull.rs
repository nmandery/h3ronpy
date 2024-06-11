use crate::array::to_geo::{
    cellindexarray_to_multipolygon, directededgeindexarray_to_multipoint,
    vertexindexarray_to_multipoint,
};
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use geo::concave_hull::ConcaveHull;
use geo_types::Polygon;

impl ConcaveHull for CellIndexArray {
    type Scalar = f64;

    fn concave_hull(&self, concavity: Self::Scalar) -> Polygon<Self::Scalar> {
        cellindexarray_to_multipolygon(self).concave_hull(concavity)
    }
}

impl ConcaveHull for VertexIndexArray {
    type Scalar = f64;

    fn concave_hull(&self, concavity: Self::Scalar) -> Polygon<Self::Scalar> {
        vertexindexarray_to_multipoint(self).concave_hull(concavity)
    }
}

impl ConcaveHull for DirectedEdgeIndexArray {
    type Scalar = f64;

    fn concave_hull(&self, concavity: Self::Scalar) -> Polygon<Self::Scalar> {
        directededgeindexarray_to_multipoint(self).concave_hull(concavity)
    }
}
