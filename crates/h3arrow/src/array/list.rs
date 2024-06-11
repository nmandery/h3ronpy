use crate::array::{H3Array, H3IndexArrayValue};
use crate::error::Error;
use arrow::array::{Array, GenericListBuilder, UInt64Array, UInt64Builder};
use arrow::array::{GenericListArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use std::marker::PhantomData;

pub struct H3ListArray<IX, O: OffsetSizeTrait = i64> {
    pub(crate) list_array: GenericListArray<O>,
    pub(crate) h3index_phantom: PhantomData<IX>,
}

impl<IX, O: OffsetSizeTrait> H3ListArray<IX, O>
where
    IX: H3IndexArrayValue,
    H3Array<IX>: TryFrom<UInt64Array, Error = Error>,
{
    pub fn listarray(&self) -> &GenericListArray<O> {
        &self.list_array
    }

    pub fn len(&self) -> usize {
        self.list_array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list_array.is_empty()
    }

    pub fn iter_arrays(&self) -> impl Iterator<Item = Option<Result<H3Array<IX>, Error>>> + '_ {
        self.list_array.iter().map(|opt| {
            opt.map(|array| {
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    // TODO: this should already be validated. unwrap/expect?
                    .ok_or(Error::NotAUint64Array)
                    .and_then(|pa| pa.clone().try_into())
            })
        })
    }

    pub fn into_flattened(self) -> Result<H3Array<IX>, Error> {
        // TODO: check validity correctness
        self.list_array
            .values()
            .as_any()
            .downcast_ref::<UInt64Array>()
            // TODO: this should already be validated. unwrap/expect?
            .ok_or(Error::NotAUint64Array)
            .and_then(|pa| pa.clone().try_into())
    }

    pub(crate) fn from_genericlistarray_unvalidated(
        value: GenericListArray<O>,
    ) -> Result<H3ListArray<IX, O>, Error> {
        if value.data_type() != &DataType::UInt64 {
            return Err(Error::NotAUint64Array);
        }

        Ok(Self {
            list_array: value,
            h3index_phantom: PhantomData::<IX>,
        })
    }
}

impl<IX, O: OffsetSizeTrait> From<H3ListArray<IX, O>> for GenericListArray<O> {
    fn from(value: H3ListArray<IX, O>) -> Self {
        value.list_array
    }
}

pub(crate) fn genericlistarray_to_h3listarray_unvalidated<IX, O: OffsetSizeTrait>(
    value: GenericListArray<O>,
) -> Result<H3ListArray<IX, O>, Error> {
    let nested_datatype = match value.data_type() {
        DataType::List(field_ref) => field_ref.data_type().clone(),
        DataType::LargeList(field_ref) => field_ref.data_type().clone(),
        _ => return Err(Error::NotAUint64Array),
    };
    if !nested_datatype.equals_datatype(&DataType::UInt64) {
        return Err(Error::NotAUint64Array);
    }

    Ok(H3ListArray {
        list_array: value,
        h3index_phantom: PhantomData::<IX>,
    })
}

impl<IX, O: OffsetSizeTrait> TryFrom<GenericListArray<O>> for H3ListArray<IX, O>
where
    IX: H3IndexArrayValue,
    H3Array<IX>: TryFrom<UInt64Array, Error = Error>,
{
    type Error = Error;

    fn try_from(value: GenericListArray<O>) -> Result<Self, Self::Error> {
        let instance = Self::from_genericlistarray_unvalidated(value)?;

        // validate all values
        for a in instance.iter_arrays().flatten() {
            let _ = a?;
        }
        Ok(instance)
    }
}

pub struct H3ArrayBuilder<'a, IX> {
    array_builder: &'a mut UInt64Builder,
    h3index_phantom: PhantomData<IX>,
}

impl<'a, IX> H3ArrayBuilder<'a, IX>
where
    IX: H3IndexArrayValue,
{
    #[inline]
    pub fn append_value(&mut self, value: IX) {
        self.array_builder.append_value(value.into())
    }

    pub fn append_many<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = IX>,
    {
        iter.into_iter().for_each(|value| self.append_value(value))
    }
}

pub struct H3ListArrayBuilder<IX, O: OffsetSizeTrait = i64> {
    h3index_phantom: PhantomData<IX>,
    builder: GenericListBuilder<O, UInt64Builder>,
}

impl<IX, O: OffsetSizeTrait> H3ListArrayBuilder<IX, O>
where
    IX: H3IndexArrayValue,
{
    pub fn with_capacity(list_capacity: usize, values_capacity: usize) -> Self {
        let builder = GenericListBuilder::with_capacity(
            UInt64Builder::with_capacity(values_capacity),
            list_capacity,
        );
        Self {
            h3index_phantom: Default::default(),
            builder,
        }
    }

    pub fn append(&mut self, is_valid: bool) {
        self.builder.append(is_valid)
    }

    pub fn values(&mut self) -> H3ArrayBuilder<'_, IX> {
        H3ArrayBuilder {
            array_builder: self.builder.values(),
            h3index_phantom: self.h3index_phantom,
        }
    }

    pub fn finish(mut self) -> Result<H3ListArray<IX, O>, Error> {
        genericlistarray_to_h3listarray_unvalidated(self.builder.finish())
    }
}

impl<IX, O: OffsetSizeTrait> Default for H3ListArrayBuilder<IX, O>
where
    IX: H3IndexArrayValue,
{
    fn default() -> Self {
        Self::with_capacity(10, 10)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::H3ListArrayBuilder;
    use h3o::{CellIndex, LatLng, Resolution};

    #[test]
    fn construct() {
        let cell = LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five);

        let mut builder = H3ListArrayBuilder::<CellIndex>::default();
        builder.values().append_many(cell.grid_disk::<Vec<_>>(1));
        builder.append(true);
        builder.append(false);
        builder.values().append_many(cell.grid_disk::<Vec<_>>(2));
        builder.append(true);

        let list = builder.finish().unwrap();

        /*
        let list = H3ListArray::<CellIndex>::try_from_iter(
            [Some(1), None, Some(2)]
                .into_iter()
                .map(|k| k.map(|k| cell.grid_disk::<Vec<_>>(k))),
        )
        .unwrap();

         */
        assert_eq!(list.len(), 3);
        let mut list_iter = list.iter_arrays();
        assert_eq!(list_iter.next().unwrap().unwrap().unwrap().len(), 7);
        assert!(list_iter.next().unwrap().is_none());
        assert_eq!(list_iter.next().unwrap().unwrap().unwrap().len(), 19);
        assert!(list_iter.next().is_none());
        drop(list_iter);

        let cells = list.into_flattened().unwrap();
        assert_eq!(cells.len(), 26);
    }
}
