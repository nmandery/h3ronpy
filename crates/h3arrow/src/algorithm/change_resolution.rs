use crate::array::{CellIndexArray, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;
use h3o::{CellIndex, Resolution};
use std::cmp::Ordering;
use std::iter::repeat;

pub struct ChangedResolutionPair<T> {
    /// values before the resolution change
    pub before: T,

    /// values after the resolution change
    pub after: T,
}

pub trait ChangeResolutionOp
where
    Self: Sized,
{
    /// change the H3 resolutions of all contained values to `resolution`.
    ///
    /// In case of resolution increases all child indexes will be added, so the returned
    /// value may contain more indexes than `self`.
    ///
    /// Invalid/empty values are omitted.
    fn change_resolution(&self, resolution: Resolution) -> Result<Self, Error>;

    /// Change the H3 resolutions of all contained values to `resolution`.
    ///
    /// The output list array has the same length as the input array, positions of the elements
    /// in input and output are corresponding to each other.
    ///
    /// Invalid/empty values are preserved as such.
    fn change_resolution_list(
        &self,
        resolution: Resolution,
    ) -> Result<H3ListArray<CellIndex>, Error>;

    /// change the H3 resolutions of all contained values to `resolution` and build a before-array
    /// with the input values for each after-value.
    ///
    /// The length of the returned `before` and `after` values are guaranteed to be the same, as
    /// before-elements are repeated according to the resulting number of after-elements.
    ///
    /// Invalid/empty values are omitted.
    fn change_resolution_paired(
        &self,
        resolution: Resolution,
    ) -> Result<ChangedResolutionPair<Self>, Error>;
}

#[inline]
fn extend_with_cell(out_vec: &mut Vec<CellIndex>, cell: CellIndex, target_resolution: Resolution) {
    match cell.resolution().cmp(&target_resolution) {
        Ordering::Less => out_vec.extend(cell.children(target_resolution)),
        Ordering::Equal => out_vec.push(cell),
        Ordering::Greater => out_vec.extend(cell.parent(target_resolution).iter()),
    }
}

impl ChangeResolutionOp for CellIndexArray {
    fn change_resolution(&self, resolution: Resolution) -> Result<Self, Error> {
        let mut out_vec: Vec<CellIndex> = Vec::with_capacity(self.len());

        self.iter()
            .flatten()
            .for_each(|cell| extend_with_cell(&mut out_vec, cell, resolution));

        Ok(out_vec.into())
    }

    fn change_resolution_list(
        &self,
        resolution: Resolution,
    ) -> Result<H3ListArray<CellIndex>, Error> {
        let mut builder = H3ListArrayBuilder::with_capacity(self.len(), self.len());

        self.iter().for_each(|cell| match cell {
            Some(cell) => match cell.resolution().cmp(&resolution) {
                Ordering::Less => {
                    builder.values().append_many(cell.children(resolution));
                    builder.append(true)
                }
                Ordering::Equal => {
                    builder.values().append_value(cell);
                    builder.append(true)
                }
                Ordering::Greater => match cell.parent(resolution) {
                    Some(parent_cell) => {
                        builder.values().append_value(parent_cell);
                        builder.append(true)
                    }
                    None => builder.append(false),
                },
            },
            None => {
                builder.append(false);
            }
        });
        builder.finish()
    }

    fn change_resolution_paired(
        &self,
        resolution: Resolution,
    ) -> Result<ChangedResolutionPair<Self>, Error> {
        let mut before_vec: Vec<CellIndex> = Vec::with_capacity(self.len());
        let mut after_vec: Vec<CellIndex> = Vec::with_capacity(self.len());

        self.iter().flatten().for_each(|cell| {
            let len_before = after_vec.len();
            extend_with_cell(&mut after_vec, cell, resolution);
            before_vec.extend(repeat(cell).take(after_vec.len() - len_before));
        });

        Ok(ChangedResolutionPair {
            before: before_vec.into(),
            after: after_vec.into(),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::algorithm::ChangeResolutionOp;
    use crate::array::CellIndexArray;
    use ahash::HashSet;
    use arrow::array::Array;
    use h3o::{LatLng, Resolution};

    #[test]
    fn change_resolution() {
        let arr: CellIndexArray = vec![
            Some(LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five)),
            None,
            Some(LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine)),
        ]
        .into();

        let arr_res_six = arr.change_resolution(Resolution::Six).unwrap();
        assert_eq!(arr_res_six.len(), 7 + 1);

        // no invalid values
        assert_eq!(
            arr_res_six
                .primitive_array()
                .nulls()
                .map(|nullbuf| nullbuf.null_count())
                .unwrap_or(0),
            0
        )
    }

    #[test]
    fn change_resolution_list() {
        let arr: CellIndexArray = vec![
            Some(LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five)),
            None,
            Some(LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine)),
        ]
        .into();

        let list_arr = arr.change_resolution_list(Resolution::Six).unwrap();
        assert_eq!(list_arr.len(), 3);

        assert_eq!(list_arr.list_array.value(0).len(), 7);
        assert_eq!(list_arr.list_array.value(1).len(), 0);
        assert_eq!(list_arr.list_array.value(2).len(), 1);
    }

    #[test]
    fn change_resolution_paired() {
        let arr: CellIndexArray = vec![
            Some(LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five)),
            None,
            Some(LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine)),
        ]
        .into();

        let arr_res_six = arr.change_resolution_paired(Resolution::Six).unwrap();
        assert_eq!(arr_res_six.after.len(), 7 + 1);
        assert_eq!(arr_res_six.before.len(), arr_res_six.after.len());
        assert_eq!(
            arr_res_six
                .before
                .iter()
                .flatten()
                .collect::<HashSet<_>>()
                .len(),
            2
        )
    }
}
