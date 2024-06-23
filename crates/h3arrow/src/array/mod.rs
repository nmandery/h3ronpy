use std::marker::PhantomData;
use std::mem::transmute;

use arrow::array::{Array, ArrayIter, PrimitiveArray, UInt64Array, UInt64Builder};
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};

#[allow(unused_imports)]
pub use list::*;
#[allow(unused_imports)]
pub use resolution::*;
#[allow(unused_imports)]
pub use validity::*;

use crate::error::Error;

mod cell;
mod directededge;
pub mod from_geo;
#[cfg(feature = "geoarrow")]
pub mod from_geoarrow;
mod list;
mod resolution;
pub mod to_geo;
#[cfg(feature = "geoarrow")]
pub mod to_geoarrow;
mod validity;
mod vertex;

pub trait H3IndexArrayValue: Into<u64> + TryFrom<u64> + Clone {
    fn transmute_from_u64(value: u64) -> Self;
}

impl H3IndexArrayValue for CellIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

impl H3IndexArrayValue for VertexIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

impl H3IndexArrayValue for DirectedEdgeIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

pub struct PrimitiveArrayH3IndexIter<'a, IX> {
    primitive_array_iter: ArrayIter<&'a UInt64Array>,
    h3index_phantom: PhantomData<IX>,
}

impl<'a, IX> Iterator for PrimitiveArrayH3IndexIter<'a, IX>
where
    IX: H3IndexArrayValue,
{
    type Item = Option<IX>;

    fn next(&mut self) -> Option<Self::Item> {
        self.primitive_array_iter
            .next()
            .map(|index| index.map(IX::transmute_from_u64))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.primitive_array_iter.size_hint()
    }
}

#[derive(Clone, PartialEq)]
pub struct H3Array<IX> {
    h3index_phantom: PhantomData<IX>,
    primitive_array: UInt64Array,
}

impl<IX> H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    pub fn new_null(length: usize) -> Self {
        Self {
            h3index_phantom: Default::default(),
            primitive_array: UInt64Array::new_null(length),
        }
    }

    pub fn builder(capacity: usize) -> H3ArrayBuilder<IX> {
        H3ArrayBuilder::with_capacity(capacity)
    }

    pub fn primitive_array(&self) -> &UInt64Array {
        &self.primitive_array
    }

    pub fn len(&self) -> usize {
        self.primitive_array().len()
    }

    pub fn is_empty(&self) -> bool {
        self.primitive_array().is_empty()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            h3index_phantom: Default::default(),
            primitive_array: self.primitive_array.slice(offset, length),
        }
    }

    /// Returns an iterator over the values and validity as Option.
    #[allow(clippy::type_complexity)]
    pub fn iter(&self) -> PrimitiveArrayH3IndexIter<IX> {
        // as the array contents have been validated upon construction, we just transmute to the h3o type
        PrimitiveArrayH3IndexIter {
            primitive_array_iter: self.primitive_array.iter(),
            h3index_phantom: Default::default(),
        }
    }

    /// Returns the element at index `i` or `None` if it is null
    /// # Panics
    /// iff `i >= self.len()`
    pub fn get(&self, i: usize) -> Option<IX> {
        if self.primitive_array.is_valid(i) {
            Some(IX::transmute_from_u64(self.primitive_array.value(i)))
        } else {
            None
        }
    }
}

pub type CellIndexArray = H3Array<CellIndex>;
pub type VertexIndexArray = H3Array<VertexIndex>;
pub type DirectedEdgeIndexArray = H3Array<DirectedEdgeIndex>;

pub struct H3ArrayBuilder<IX> {
    h3index_phantom: PhantomData<IX>,
    builder: UInt64Builder,
}

impl<IX> H3ArrayBuilder<IX>
where
    IX: H3IndexArrayValue,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            h3index_phantom: Default::default(),
            builder: UInt64Builder::with_capacity(capacity),
        }
    }

    /// Returns the capacity of this builder measured in slots of type `T`
    pub fn capacity(&self) -> usize {
        self.builder.capacity()
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value(&mut self, v: IX) {
        self.builder.append_value(v.into());
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_null();
    }

    /// Builds the [`H3Array`] and reset this builder.
    pub fn finish(&mut self) -> H3Array<IX> {
        H3Array {
            h3index_phantom: Default::default(),
            primitive_array: self.builder.finish(),
        }
    }
}

/// Conversion corresponding to `From` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromWithValidity<T> {
    fn from_with_validity(value: T) -> Self;
}

/// Conversion corresponding to `FromIterator` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromIteratorWithValidity<A: Sized> {
    fn from_iter_with_validity<T: IntoIterator<Item = A>>(iter: T) -> Self;
}

impl<IX> TryFrom<UInt64Array> for H3Array<IX>
where
    IX: H3IndexArrayValue + TryFrom<u64>,
    Error: From<<IX as TryFrom<u64>>::Error>,
{
    type Error = Error;

    fn try_from(value: UInt64Array) -> Result<Self, Self::Error> {
        // validate the contained h3 cells
        value
            .iter()
            .flatten() // TODO: should this really flatten or preserve unset positions?
            .try_for_each(|h3index| IX::try_from(h3index).map(|_| ()))?;
        Ok(H3Array {
            primitive_array: value,
            h3index_phantom: PhantomData::<IX>,
        })
    }
}

impl<IX> TryFrom<Vec<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue + TryFrom<u64>,
    Error: From<<IX as TryFrom<u64>>::Error>,
{
    type Error = Error;

    fn try_from(value: Vec<u64>) -> Result<Self, Self::Error> {
        // validate the contained h3 cells
        let validated = value
            .into_iter()
            .map(|h3index| IX::try_from(h3index).map(|v| v.into()))
            .collect::<Result<Vec<u64>, _>>()
            .map_err(Self::Error::from)?;
        Ok(Self {
            primitive_array: PrimitiveArray::new(validated.into(), None),
            h3index_phantom: PhantomData::<IX>,
        })
    }
}

impl<IX> From<Vec<IX>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from(value: Vec<IX>) -> Self {
        Self::from_iter(value)
    }
}

impl<IX> From<Vec<Option<IX>>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from(value: Vec<Option<IX>>) -> Self {
        Self::from_iter(value)
    }
}

impl<IX> FromIterator<IX> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter<T: IntoIterator<Item = IX>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(iter.into_iter().map(|v| Some(v.into()))),
            h3index_phantom: PhantomData::<IX>,
        }
    }
}

impl<IX> FromIterator<Option<IX>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter<T: IntoIterator<Item = Option<IX>>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(
                iter.into_iter().map(|v| v.map(|v| v.into())),
            ),
            h3index_phantom: PhantomData::<IX>,
        }
    }
}

impl<IX> From<H3Array<IX>> for UInt64Array {
    fn from(v: H3Array<IX>) -> Self {
        v.primitive_array
    }
}

impl<IX> FromIteratorWithValidity<u64> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter_with_validity<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(
                iter.into_iter()
                    .map(|h3index| IX::try_from(h3index).ok().map(|v| v.into())),
            ),
            h3index_phantom: PhantomData::<IX>,
        }
    }
}

impl<IX> FromIteratorWithValidity<Option<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter_with_validity<T: IntoIterator<Item = Option<u64>>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(iter.into_iter().map(|h3index| {
                h3index.and_then(|h3index| IX::try_from(h3index).ok().map(|v| v.into()))
            })),
            h3index_phantom: PhantomData::<IX>,
        }
    }
}

impl<IX> FromWithValidity<Vec<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: Vec<u64>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<IX> FromWithValidity<Vec<Option<u64>>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: Vec<Option<u64>>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<IX> FromWithValidity<UInt64Array> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: UInt64Array) -> Self {
        Self::from_iter_with_validity(value.iter())
    }
}
