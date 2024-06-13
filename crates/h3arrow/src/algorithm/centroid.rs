use crate::array::to_geo::{ToLines, ToPoints};
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use geo::centroid::Centroid;
use geo_types::{MultiPoint, Point};

macro_rules! impl_point_based_centroid {
    ($($array_type:ty),*) => {
        $(
        impl Centroid for $array_type {
            type Output = Option<Point>;

            fn centroid(&self) -> Self::Output {
                MultiPoint::new(
                    self.to_points(true)
                        .expect("point vec")
                        .into_iter()
                        .flatten()
                        .collect(),
                )
                .centroid()
            }
        }
        )*
    };
}

impl_point_based_centroid!(CellIndexArray, VertexIndexArray);

impl Centroid for DirectedEdgeIndexArray {
    type Output = Option<Point>;

    fn centroid(&self) -> Self::Output {
        MultiPoint::new(
            self.to_lines(true)
                .expect("line vec")
                .into_iter()
                .flatten()
                .map(|line| line.centroid())
                .collect(),
        )
        .centroid()
    }
}

// todo: H3ListArray
