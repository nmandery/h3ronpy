use super::from_geo::{
    cell_vecs_to_h3listarray, IterToCellIndexArray, IterToCellListArray, ToCellIndexArray,
    ToCellListArray, ToCellsOptions,
};
use crate::algorithm::CompactOp;
use crate::array::from_geo::geometry_to_cells;
use crate::array::{CellIndexArray, H3ListArray};
use crate::error::Error;
use arrow::array::OffsetSizeTrait;
use geo_types::Geometry;
use geoarrow::array::WKBArray;
use geoarrow::trait_::GeometryArrayAccessor;
use geoarrow::GeometryArrayTrait;
use h3o::CellIndex;
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

macro_rules! impl_to_cells {
    ($array_type:ty, $offset:tt) => {
        impl<$offset: OffsetSizeTrait> ToCellListArray<$offset> for $array_type {
            fn to_celllistarray(
                &self,
                options: &ToCellsOptions,
            ) -> Result<H3ListArray<CellIndex, $offset>, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_celllistarray(options)
            }
        }

        impl<$offset: OffsetSizeTrait> ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_cellindexarray(options)
            }
        }
    };
    ($array_type:ty) => {
        impl<O: OffsetSizeTrait> ToCellListArray<O> for $array_type {
            fn to_celllistarray(
                &self,
                options: &ToCellsOptions,
            ) -> Result<H3ListArray<CellIndex, O>, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_celllistarray(options)
            }
        }

        impl ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_cellindexarray(options)
            }
        }
    };
}

impl_to_cells!(geoarrow::array::LineStringArray<O>, O);
impl_to_cells!(geoarrow::array::MultiLineStringArray<O>, O);
impl_to_cells!(geoarrow::array::MultiPointArray<O>, O);
impl_to_cells!(geoarrow::array::MultiPolygonArray<O>, O);
impl_to_cells!(geoarrow::array::PointArray);
impl_to_cells!(geoarrow::array::PolygonArray<O>, O);

impl<O: OffsetSizeTrait> ToCellListArray<O> for WKBArray<O> {
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error> {
        #[cfg(not(feature = "rayon"))]
        let pos_iter = 0..self.len();

        #[cfg(feature = "rayon")]
        let pos_iter = (0..self.len()).into_par_iter();

        let cell_vecs = pos_iter
            .map(|pos| {
                self.get_as_geo(pos)
                    .map(|geom| geometry_to_cells(&geom, options))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        cell_vecs_to_h3listarray(cell_vecs)
    }
}

impl<O: OffsetSizeTrait> ToCellIndexArray for WKBArray<O> {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cellindexarray = self.to_celllistarray(options)?.into_flattened()?;

        if options.compact {
            cellindexarray.compact()
        } else {
            Ok(cellindexarray) // may contain duplicates
        }
    }
}
