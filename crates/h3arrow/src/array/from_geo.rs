use arrow::array::OffsetSizeTrait;
use geo::HasDimensions;
use geo_types::*;
use h3o::geom::{ContainmentMode, Plotter, PlotterBuilder, Tiler, TilerBuilder};
use h3o::{CellIndex, LatLng, Resolution};
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::array::list::H3ListArray;
use crate::array::{CellIndexArray, H3ListArrayBuilder};
use crate::error::Error;

#[derive(Clone, Copy, Debug)]
pub struct ToCellsOptions {
    pub(crate) h3_resolution: Resolution,
    pub(crate) containment_mode: ContainmentMode,
    pub(crate) compact: bool,
}

impl ToCellsOptions {
    pub fn new(h3_resolution: Resolution) -> Self {
        Self {
            h3_resolution,
            containment_mode: ContainmentMode::ContainsCentroid,
            compact: false,
        }
    }

    pub fn compact(mut self, compact: bool) -> Self {
        self.compact = compact;
        self
    }

    pub fn containment_mode(mut self, containment_mode: ContainmentMode) -> Self {
        self.containment_mode = containment_mode;
        self
    }

    pub(crate) fn tiler(&self) -> Tiler {
        TilerBuilder::new(self.h3_resolution)
            .containment_mode(self.containment_mode)
            .build()
    }

    pub(crate) fn plotter(&self) -> Plotter {
        PlotterBuilder::new(self.h3_resolution).build()
    }
}

impl From<Resolution> for ToCellsOptions {
    fn from(resolution: Resolution) -> Self {
        Self::new(resolution)
    }
}

pub trait ToClonedGeometry {
    fn to_cloned_geometry(&self) -> Option<Geometry>;
}

impl ToClonedGeometry for Geometry {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        Some(self.clone())
    }
}

impl ToClonedGeometry for Option<Geometry> {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        self.clone()
    }
}

impl ToClonedGeometry for Coord {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        Some(Geometry::from(Point::from(*self)))
    }
}

impl ToClonedGeometry for Option<Coord> {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        self.as_ref().map(|g| Geometry::from(Point::from(*g)))
    }
}

macro_rules! impl_to_cloned {
    ($($geomtype:ty),*) => {
        $(
        impl ToClonedGeometry for $geomtype {
            fn to_cloned_geometry(&self) -> Option<Geometry> {
                Some(Geometry::from(self.clone()))
            }
        }

        impl ToClonedGeometry for Option<$geomtype> {
            fn to_cloned_geometry(&self) -> Option<Geometry> {
                self.as_ref().map(|g| Geometry::from(g.clone()))
            }
        }
        )*
    };
}

impl_to_cloned!(
    Polygon,
    Point,
    LineString,
    Line,
    Rect,
    Triangle,
    MultiPoint,
    MultiPolygon,
    MultiLineString
);

/// convert to a single `CellIndexArray`
pub trait ToCellIndexArray {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

pub(crate) trait IterToCellIndexArray {
    fn to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

#[cfg(feature = "rayon")]
pub(crate) trait ParIterToCellIndexArray {
    fn par_to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellIndexArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cells = self
            .into_par_iter()
            .try_fold(Vec::new, |acc, geom| match geom {
                Some(geom) => to_cells(geom, options, acc),
                None => Ok(acc),
            })
            .try_reduce(Vec::new, |mut a, mut b| {
                if a.len() > b.len() {
                    a.append(&mut b);
                    Ok(a)
                } else {
                    b.append(&mut a);
                    Ok(b)
                }
            })?;
        Ok(cells.into())
    }
}

impl<T> IterToCellIndexArray for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cells = self.into_iter().try_fold(vec![], |acc, geom| {
            if let Some(geom) = geom {
                to_cells(geom, options, acc)
            } else {
                Ok(acc)
            }
        })?;
        Ok(cells.into())
    }
}

#[cfg(feature = "rayon")]
impl<T> ToCellIndexArray for &[T]
where
    T: ToClonedGeometry + Sync,
{
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        self.into_par_iter()
            .map(|v| v.to_cloned_geometry())
            .par_to_cellindexarray(options)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T> ToCellIndexArray for &[T]
where
    T: ToClonedGeometry,
{
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        self.iter()
            .map(|v| v.to_cloned_geometry())
            .to_cellindexarray(options)
    }
}

pub trait ToCellListArray<O: OffsetSizeTrait> {
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error>;
}

pub(crate) trait IterToCellListArray<O: OffsetSizeTrait> {
    fn to_celllistarray(self, options: &ToCellsOptions)
        -> Result<H3ListArray<CellIndex, O>, Error>;
}

#[cfg(feature = "rayon")]
trait ParIterToCellListArray<O: OffsetSizeTrait> {
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error>;
}

#[cfg(feature = "rayon")]
impl<T, O: OffsetSizeTrait> ParIterToCellListArray<O> for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error> {
        let cell_vecs = self
            .map(|geom| geom.map(|geom| to_cells(geom, options, vec![])).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        cell_vecs_to_h3listarray(cell_vecs)
    }
}

pub(crate) fn cell_vecs_to_h3listarray<O: OffsetSizeTrait>(
    cell_vecs: Vec<Option<Vec<CellIndex>>>,
) -> Result<H3ListArray<CellIndex, O>, Error> {
    let uint64_capacity: usize = cell_vecs
        .iter()
        .map(|cells| cells.as_ref().map(|v| v.len()).unwrap_or(0))
        .sum();

    let mut builder = H3ListArrayBuilder::with_capacity(cell_vecs.len(), uint64_capacity);

    for cells in cell_vecs.into_iter() {
        let is_valid = if let Some(cells) = cells {
            builder.values().append_many(cells);
            true
        } else {
            false
        };
        builder.append(is_valid);
    }
    builder.finish()
}

impl<T, O: OffsetSizeTrait> IterToCellListArray<O> for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error> {
        let mut builder = H3ListArrayBuilder::with_capacity(self.size_hint().0, self.size_hint().0);

        for geom in self {
            if let Some(geom) = geom {
                builder
                    .values()
                    .append_many(geometry_to_cells(&geom, options)?);
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        builder.finish()
    }
}

#[cfg(feature = "rayon")]
impl<T, O: OffsetSizeTrait> ToCellListArray<O> for &[T]
where
    T: ToClonedGeometry + Sync,
{
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error> {
        self.into_par_iter()
            .map(|g| g.to_cloned_geometry())
            .par_to_celllistarray(options)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T, O: OffsetSizeTrait> ToCellListArray<O> for &[T]
where
    T: ToClonedGeometry,
{
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex, O>, Error> {
        self.iter()
            .map(|g| g.to_cloned_geometry())
            .to_celllistarray(options)
    }
}

pub fn geometry_to_cells(
    geom: &Geometry,
    options: &ToCellsOptions,
) -> Result<Vec<CellIndex>, Error> {
    if geom.is_empty() {
        return Ok(vec![]);
    }

    let mut cells = vec![];
    geometry_to_cells_internal(geom, options, &mut cells)?;

    // deduplicate, in the case of overlaps or lines
    cells.sort_unstable();
    cells.dedup();

    let cells = if options.compact {
        CellIndex::compact(cells)?.collect()
    } else {
        cells
    };
    Ok(cells)
}

fn geometry_to_cells_internal(
    geom: &Geometry,
    options: &ToCellsOptions,
    out_cells: &mut Vec<CellIndex>,
) -> Result<(), Error> {
    match geom {
        Geometry::Point(pt) => {
            out_cells.push(LatLng::try_from(pt.0)?.to_cell(options.h3_resolution))
        }
        Geometry::Line(line) => {
            let mut plotter = options.plotter();
            plotter.add(*line)?;
            push_plotter_contents(out_cells, plotter)?;
        }
        Geometry::LineString(line_string) => {
            let mut plotter = options.plotter();
            plotter.add_batch(line_string.lines())?;
            push_plotter_contents(out_cells, plotter)?;
        }
        Geometry::Polygon(polygon) => {
            let mut tiler = options.tiler();
            tiler.add(polygon.clone())?;
            out_cells.extend(tiler.into_coverage());
        }
        Geometry::MultiPoint(multi_point) => {
            out_cells.reserve(multi_point.len());
            for point in multi_point.iter() {
                out_cells.push(LatLng::try_from(point.0)?.to_cell(options.h3_resolution))
            }
        }
        Geometry::MultiLineString(multi_line_string) => {
            let mut plotter = options.plotter();
            for line_string in multi_line_string.iter() {
                plotter.add_batch(line_string.lines())?;
            }
            push_plotter_contents(out_cells, plotter)?;
        }
        Geometry::MultiPolygon(multi_polygon) => {
            let mut tiler = options.tiler();
            tiler.add_batch(multi_polygon.iter().cloned())?;
            out_cells.extend(tiler.into_coverage());
        }
        Geometry::GeometryCollection(geometry_collection) => geometry_collection
            .iter()
            .try_for_each(|g| geometry_to_cells_internal(g, options, out_cells))?,
        Geometry::Rect(rect) => {
            let mut tiler = options.tiler();
            tiler.add(rect.to_polygon())?;
            out_cells.extend(tiler.into_coverage());
        }
        Geometry::Triangle(triangle) => {
            let mut tiler = options.tiler();
            tiler.add(triangle.to_polygon())?;
            out_cells.extend(tiler.into_coverage());
        }
    }
    Ok(())
}

fn push_plotter_contents(out_cells: &mut Vec<CellIndex>, plotter: Plotter) -> Result<(), Error> {
    let cell_iter = plotter.plot();
    out_cells.reserve(cell_iter.size_hint().0);
    for cell_result in cell_iter {
        out_cells.push(cell_result?);
    }
    Ok(())
}

fn to_cells(
    geom: Geometry,
    options: &ToCellsOptions,
    mut acc: Vec<CellIndex>,
) -> Result<Vec<CellIndex>, Error> {
    acc.extend(geometry_to_cells(&geom, options)?);
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use crate::array::from_geo::{ToCellIndexArray, ToCellsOptions};
    use geo_types::Rect;
    use h3o::Resolution;

    #[test]
    fn from_rect() {
        let rect = vec![Rect::new((10., 10.), (20., 20.))];
        let options = ToCellsOptions::from(Resolution::Four);
        let cells = rect.as_slice().to_cellindexarray(&options).unwrap();
        assert!(cells.len() > 400);
        let resolution = cells.resolution();
        assert_eq!(cells.len(), resolution.len());

        for r in resolution.iter() {
            assert_eq!(r, Some(Resolution::Four));
        }
    }
}
