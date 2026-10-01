use crate::array::to_geo::{
    cellindexarray_to_multipolygon, directededgeindexarray_to_multipoint,
    vertexindexarray_to_multipoint,
};
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use geo::concave_hull::{ConcaveHull, ConcaveHullOptions};
use geo_types::Polygon;

impl ConcaveHull for CellIndexArray {
    type Scalar = f64;

    fn concave_hull(&self) -> Polygon<Self::Scalar> {
        cellindexarray_to_multipolygon(self).concave_hull()
    }

    fn concave_hull_with_options(
        &self,
        concave_hull_options: ConcaveHullOptions<Self::Scalar>,
    ) -> Polygon<Self::Scalar> {
        cellindexarray_to_multipolygon(self).concave_hull_with_options(concave_hull_options)
    }
}

impl ConcaveHull for VertexIndexArray {
    type Scalar = f64;

    fn concave_hull(&self) -> Polygon<Self::Scalar> {
        vertexindexarray_to_multipoint(self).concave_hull()
    }

    fn concave_hull_with_options(
        &self,
        concave_hull_options: ConcaveHullOptions<Self::Scalar>,
    ) -> Polygon<Self::Scalar> {
        vertexindexarray_to_multipoint(self).concave_hull_with_options(concave_hull_options)
    }
}

impl ConcaveHull for DirectedEdgeIndexArray {
    type Scalar = f64;

    fn concave_hull(&self) -> Polygon<Self::Scalar> {
        directededgeindexarray_to_multipoint(self).concave_hull()
    }

    fn concave_hull_with_options(
        &self,
        concave_hull_options: ConcaveHullOptions<Self::Scalar>,
    ) -> Polygon<Self::Scalar> {
        directededgeindexarray_to_multipoint(self).concave_hull_with_options(concave_hull_options)
    }
}
