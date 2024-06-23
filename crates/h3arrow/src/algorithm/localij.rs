use crate::array::{CellIndexArray, H3ArrayBuilder};
use crate::error::Error;
use arrow::array::Int32Array;
use h3o::{CellIndex, CoordIJ, LocalIJ};
use std::iter::repeat;

pub struct LocalIJArrays {
    pub anchors: CellIndexArray,
    pub i: Int32Array,
    pub j: Int32Array,
}

impl LocalIJArrays {
    pub fn try_new(anchors: CellIndexArray, i: Int32Array, j: Int32Array) -> Result<Self, Error> {
        let instance = Self { anchors, i, j };
        instance.validate()?;
        Ok(instance)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.j.len() != self.i.len() || self.j.len() != self.anchors.len() {
            return Err(Error::LengthMismatch);
        }
        Ok(())
    }

    fn to_cells_internal<Adder>(&self, mut adder: Adder) -> Result<CellIndexArray, Error>
    where
        Adder: FnMut(LocalIJ, &mut H3ArrayBuilder<CellIndex>) -> Result<(), Error>,
    {
        self.validate()?;

        let mut builder = CellIndexArray::builder(self.i.len());
        for ((i, j), anchor) in self.i.iter().zip(self.j.iter()).zip(self.anchors.iter()) {
            match (i, j, anchor) {
                (Some(i), Some(j), Some(anchor)) => {
                    let localij = LocalIJ::new(anchor, CoordIJ::new(i, j));
                    adder(localij, &mut builder)?;
                }
                _ => builder.append_null(),
            }
        }
        Ok(builder.finish())
    }

    pub fn to_cells(&self) -> Result<CellIndexArray, Error> {
        self.to_cells_internal(|localij, builder| match CellIndex::try_from(localij) {
            Ok(cell) => {
                builder.append_value(cell);
                Ok(())
            }
            Err(e) => Err(e.into()),
        })
    }

    pub fn to_cells_failing_to_invalid(&self) -> Result<CellIndexArray, Error> {
        self.to_cells_internal(|local_ij, builder| {
            match CellIndex::try_from(local_ij) {
                Ok(cell) => {
                    builder.append_value(cell);
                }
                Err(_) => {
                    builder.append_null();
                }
            }
            Ok(())
        })
    }
}

impl TryFrom<LocalIJArrays> for CellIndexArray {
    type Error = Error;

    fn try_from(value: LocalIJArrays) -> Result<Self, Self::Error> {
        value.to_cells()
    }
}

pub trait ToLocalIJOp {
    fn to_local_ij(
        &self,
        anchor: CellIndex,
        set_failing_to_invalid: bool,
    ) -> Result<LocalIJArrays, Error>;

    /// convert to point coordinates in radians
    fn to_local_ij_array(
        &self,
        anchors: CellIndexArray,
        set_failing_to_invalid: bool,
    ) -> Result<LocalIJArrays, Error>;
}

impl ToLocalIJOp for CellIndexArray {
    fn to_local_ij(
        &self,
        anchor: CellIndex,
        set_failing_to_invalid: bool,
    ) -> Result<LocalIJArrays, Error> {
        let anchors = CellIndexArray::from_iter(repeat(anchor).take(self.len()));
        self.to_local_ij_array(anchors, set_failing_to_invalid)
    }

    fn to_local_ij_array(
        &self,
        anchors: CellIndexArray,
        set_failing_to_invalid: bool,
    ) -> Result<LocalIJArrays, Error> {
        if self.len() != anchors.len() {
            return Err(Error::LengthMismatch);
        }
        let mut i_array_builder = Int32Array::builder(self.len());
        let mut j_array_builder = Int32Array::builder(self.len());

        for (cell, anchor) in self.iter().zip(anchors.iter()) {
            match (cell, anchor) {
                (Some(cell), Some(anchor)) => match cell.to_local_ij(anchor) {
                    Ok(localij) => {
                        i_array_builder.append_value(localij.coord.i);
                        j_array_builder.append_value(localij.coord.j);
                    }
                    Err(e) => {
                        if set_failing_to_invalid {
                            i_array_builder.append_null();
                            j_array_builder.append_null();
                        } else {
                            return Err(e.into());
                        }
                    }
                },
                _ => {
                    i_array_builder.append_null();
                    j_array_builder.append_null();
                }
            }
        }

        Ok(LocalIJArrays {
            anchors,
            i: i_array_builder.finish(),
            j: j_array_builder.finish(),
        })
    }
}
