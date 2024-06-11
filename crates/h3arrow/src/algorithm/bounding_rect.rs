use crate::array::to_geo::{ToLines, ToPoints, ToPolygons};
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use geo::bounding_rect::BoundingRect;
use geo_types::Rect;

impl BoundingRect<f64> for CellIndexArray {
    type Output = Option<Rect>;

    fn bounding_rect(&self) -> Self::Output {
        collect_rect(
            self.to_polygons(true)
                .expect("polygon vec")
                .into_iter()
                .flatten()
                .filter_map(|p| p.bounding_rect()),
        )
    }
}

impl BoundingRect<f64> for VertexIndexArray {
    type Output = Option<Rect>;

    fn bounding_rect(&self) -> Self::Output {
        collect_rect(
            self.to_points(true)
                .expect("point vec")
                .into_iter()
                .flatten()
                .map(|point| point.bounding_rect()),
        )
    }
}

impl BoundingRect<f64> for DirectedEdgeIndexArray {
    type Output = Option<Rect>;

    fn bounding_rect(&self) -> Self::Output {
        collect_rect(
            self.to_lines(true)
                .expect("line vec")
                .into_iter()
                .flatten()
                .map(|line| line.bounding_rect()),
        )
    }
}

fn collect_rect<I>(iter: I) -> Option<Rect>
where
    I: Iterator<Item = Rect>,
{
    iter.reduce(|a, b| {
        Rect::new(
            (a.min().x.min(b.min().x), a.min().y.min(b.min().y)),
            (a.max().x.max(b.max().x), a.max().y.max(b.max().y)),
        )
    })
}

// todo: H3ListArray
