use arrow::array::{Array, BooleanArray, BooleanBufferBuilder};
use geo::{BoundingRect, Intersects};
use geo_types::{Coord, Line, LineString, MultiPolygon, Point, Polygon, Rect};
use h3o::{CellIndex, DirectedEdgeIndex, LatLng, VertexIndex};
use rstar::primitives::{GeomWithData, Rectangle};
use rstar::{RTree, AABB};

use crate::array::{H3Array, H3IndexArrayValue};

pub trait RectIndexable {
    fn spatial_index_rect(&self) -> Option<Rect>;
    fn intersects_with_polygon(&self, poly: &Polygon) -> bool;
}

impl RectIndexable for CellIndex {
    fn spatial_index_rect(&self) -> Option<Rect> {
        LineString::from(self.boundary()).bounding_rect()
    }

    fn intersects_with_polygon(&self, poly: &Polygon) -> bool {
        // do a cheaper centroid containment check first before comparing the polygons
        let centroid: Coord = LatLng::from(*self).into();
        if poly.intersects(&centroid) {
            let self_poly = Polygon::new(LineString::from(self.boundary()), vec![]);
            poly.intersects(&self_poly)
        } else {
            false
        }
    }
}

impl RectIndexable for DirectedEdgeIndex {
    fn spatial_index_rect(&self) -> Option<Rect> {
        Some(Line::from(*self).bounding_rect())
    }

    fn intersects_with_polygon(&self, poly: &Polygon) -> bool {
        poly.intersects(&Line::from(*self))
    }
}

impl RectIndexable for VertexIndex {
    fn spatial_index_rect(&self) -> Option<Rect> {
        Some(Point::from(*self).bounding_rect())
    }

    fn intersects_with_polygon(&self, poly: &Polygon) -> bool {
        poly.intersects(&Point::from(*self))
    }
}

type RTreeCoord = [f64; 2];
type RTreeBBox = Rectangle<RTreeCoord>;
type LocatedArrayPosition = GeomWithData<RTreeBBox, usize>;

#[inline]
fn to_coord(coord: Coord) -> RTreeCoord {
    [coord.x, coord.y]
}

#[inline]
fn to_bbox(rect: &Rect) -> RTreeBBox {
    RTreeBBox::from_corners(to_coord(rect.min()), to_coord(rect.max()))
}

pub struct SpatialIndex<IX> {
    array: H3Array<IX>,
    rtree: RTree<LocatedArrayPosition>,
}

impl<IX> From<H3Array<IX>> for SpatialIndex<IX>
where
    IX: H3IndexArrayValue + RectIndexable,
{
    fn from(array: H3Array<IX>) -> Self {
        let entries: Vec<_> = array
            .iter()
            .enumerate()
            .filter_map(|(pos, maybe_index)| match maybe_index {
                Some(index) => index
                    .spatial_index_rect()
                    .map(|rect| LocatedArrayPosition::new(to_bbox(&rect), pos)),
                _ => None,
            })
            .collect();

        let rtree = RTree::bulk_load(entries);
        Self { array, rtree }
    }
}

impl<IX> H3Array<IX>
where
    IX: H3IndexArrayValue + RectIndexable,
{
    pub fn spatial_index(&self) -> SpatialIndex<IX> {
        SpatialIndex::from(self.clone())
    }
}

impl<IX> SpatialIndex<IX>
where
    IX: H3IndexArrayValue + RectIndexable,
{
    fn intersect_impl<F>(&self, rect: &Rect, builder: &mut BooleanBufferBuilder, detailed_check: F)
    where
        F: Fn(IX) -> bool,
    {
        debug_assert_eq!(builder.len(), self.array.len());

        let envelope = AABB::from_corners(to_coord(rect.min()), to_coord(rect.max()));
        let locator = self.rtree.locate_in_envelope_intersecting(&envelope);
        for located_array_position in locator {
            if let Some(value) = self.array.get(located_array_position.data) {
                if !builder.get_bit(located_array_position.data) {
                    builder.set_bit(located_array_position.data, detailed_check(value))
                }
            }
        }
    }

    fn finish_bufferbuilder(&self, mut builder: BooleanBufferBuilder) -> BooleanArray {
        BooleanArray::new(
            builder.finish(),
            self.array.primitive_array().nulls().cloned(),
        )
    }

    pub fn intersect_envelopes(&self, rect: &Rect) -> BooleanArray {
        let mut builder = negative_mask(self.array.len());
        self.intersect_impl(rect, &mut builder, |_| true);
        self.finish_bufferbuilder(builder)
    }

    pub fn intersect_polygon(&self, poly: &Polygon) -> BooleanArray {
        let mut builder = negative_mask(self.array.len());
        if let Some(poly_rect) = poly.bounding_rect() {
            self.intersect_impl(&poly_rect, &mut builder, |ix| {
                ix.intersects_with_polygon(poly)
            });
        }
        self.finish_bufferbuilder(builder)
    }

    pub fn intersect_multipolygon(&self, mpoly: &MultiPolygon) -> BooleanArray {
        let mut builder = negative_mask(self.array.len());
        for poly in mpoly.iter() {
            if let Some(poly_rect) = poly.bounding_rect() {
                self.intersect_impl(&poly_rect, &mut builder, |ix| {
                    ix.intersects_with_polygon(poly)
                })
            }
        }
        self.finish_bufferbuilder(builder)
    }

    /// The envelope of the indexed elements is with `distance` of the given [Coord] `coord`.
    pub fn envelopes_within_distance(&self, coord: Coord, distance: f64) -> BooleanArray {
        let mut builder = negative_mask(self.array.len());
        let locator = self.rtree.locate_within_distance(to_coord(coord), distance);
        for located_array_position in locator {
            builder.set_bit(located_array_position.data, true);
        }

        self.finish_bufferbuilder(builder)
    }
}

pub(crate) fn negative_mask(size: usize) -> BooleanBufferBuilder {
    let mut builder = BooleanBufferBuilder::new(size);
    builder.append_n(size, false);
    builder
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use geo_types::{coord, polygon};
    use h3o::{LatLng, Resolution};

    use crate::array::CellIndexArray;

    #[test]
    fn cell_create_empty_index() {
        let arr: CellIndexArray = Vec::<u64>::new().try_into().unwrap();
        let _ = arr.spatial_index();
    }

    fn some_cell_array() -> CellIndexArray {
        vec![
            Some(LatLng::new(45.5, 45.5).unwrap().to_cell(Resolution::Seven)),
            Some(
                LatLng::new(-60.5, -60.5)
                    .unwrap()
                    .to_cell(Resolution::Seven),
            ),
            Some(
                LatLng::new(120.5, -70.5)
                    .unwrap()
                    .to_cell(Resolution::Seven),
            ),
            None,
        ]
        .into()
    }

    #[test]
    fn cell_envelopes_within_distance() {
        let idx = some_cell_array().spatial_index();
        let mask = idx.envelopes_within_distance((-60.0, -60.0).into(), 2.0);

        assert_eq!(mask.len(), 4);

        assert!(mask.is_valid(0));
        assert!(!mask.value(0));

        assert!(mask.is_valid(1));
        assert!(mask.value(1));

        assert!(mask.is_valid(2));
        assert!(!mask.value(2));

        assert!(!mask.is_valid(3));
    }

    #[test]
    fn cell_geometries_intersect_polygon() {
        let idx = some_cell_array().spatial_index();
        let mask = idx.intersect_polygon(&polygon!(exterior: [
                    coord! {x: 40.0, y: 40.0},
                    coord! {x: 40.0, y: 50.0},
                    coord! {x: 49.0, y: 50.0},
                    coord! {x: 49.0, y: 40.0},
                    coord! {x: 40.0, y: 40.0},
                ], interiors: []));

        assert_eq!(mask.len(), 4);

        assert!(mask.is_valid(0));
        assert!(mask.value(0));

        assert!(mask.is_valid(1));
        assert!(!mask.value(1));

        assert!(mask.is_valid(2));
        assert!(!mask.value(2));

        assert!(!mask.is_valid(3));
    }
}
