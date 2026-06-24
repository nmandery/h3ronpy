use super::from_geo::{
    cell_vecs_to_h3listarray, IterToCellIndexArray, IterToCellListArray, ToCellIndexArray,
    ToCellListArray, ToCellsOptions,
};
use crate::algorithm::CompactOp;
use crate::array::from_geo::geometry_to_cells;
use crate::array::{CellIndexArray, H3ListArray};
use crate::error::Error;
use arrow::array::OffsetSizeTrait;
use geo_traits::to_geo::ToGeoGeometry;
use geo_types::Geometry;
use geoarrow::array::{GenericWkbArray, GeometryArray};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use h3o::CellIndex;
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

macro_rules! impl_to_cells {
    ($array_type:ty) => {
        impl<O: OffsetSizeTrait> ToCellListArray<O> for $array_type {
            fn to_celllistarray(
                &self,
                options: &ToCellsOptions,
            ) -> Result<H3ListArray<CellIndex, O>, Error> {
                let geom_array: GeometryArray = self.clone().into();
                let geoms: Vec<Option<Geometry>> = geom_array
                    .iter()
                    .map(|opt| match opt {
                        None => Ok(None),
                        Some(r) => Ok(r.map_err(Error::from)?.try_to_geometry()),
                    })
                    .collect::<Result<_, Error>>()?;
                geoms.into_iter().to_celllistarray(options)
            }
        }

        impl ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
                let geom_array: GeometryArray = self.clone().into();
                let geoms: Vec<Option<Geometry>> = geom_array
                    .iter()
                    .map(|opt| match opt {
                        None => Ok(None),
                        Some(r) => Ok(r.map_err(Error::from)?.try_to_geometry()),
                    })
                    .collect::<Result<_, Error>>()?;
                geoms.into_iter().to_cellindexarray(options)
            }
        }
    };
}

impl_to_cells!(geoarrow::array::LineStringArray);
impl_to_cells!(geoarrow::array::MultiLineStringArray);
impl_to_cells!(geoarrow::array::MultiPointArray);
impl_to_cells!(geoarrow::array::MultiPolygonArray);
impl_to_cells!(geoarrow::array::PointArray);
impl_to_cells!(geoarrow::array::PolygonArray);

impl<O: OffsetSizeTrait> ToCellListArray<O> for GenericWkbArray<O> {
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
                match self.get(pos).map_err(Error::from)? {
                    None => Ok(None),
                    Some(wkb) => {
                        match wkb.try_to_geometry() {
                            Some(geom) => geometry_to_cells(&geom, options).map(Some),
                            None => Ok(None),
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        cell_vecs_to_h3listarray(cell_vecs)
    }
}

impl<O: OffsetSizeTrait> ToCellIndexArray for GenericWkbArray<O> {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cellindexarray = self.to_celllistarray(options)?.into_flattened()?;

        if options.compact {
            cellindexarray.compact()
        } else {
            Ok(cellindexarray) // may contain duplicates
        }
    }
}
