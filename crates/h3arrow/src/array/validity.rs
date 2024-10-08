use arrow::array::UInt64Array;

/// Conversion corresponding to `From` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromWithValidity<T> {
    #[allow(dead_code)]
    fn from_with_validity(value: T) -> Self;
}

/// Conversion corresponding to `FromIterator` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromIteratorWithValidity<A: Sized> {
    fn from_iter_with_validity<T: IntoIterator<Item = A>>(iter: T) -> Self;
}

impl<T> FromWithValidity<Vec<u64>> for T
where
    T: FromIteratorWithValidity<u64>,
{
    fn from_with_validity(value: Vec<u64>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<T> FromWithValidity<Vec<Option<u64>>> for T
where
    T: FromIteratorWithValidity<Option<u64>>,
{
    fn from_with_validity(value: Vec<Option<u64>>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<T> FromWithValidity<UInt64Array> for T
where
    T: FromIteratorWithValidity<Option<u64>>,
{
    fn from_with_validity(value: UInt64Array) -> Self {
        Self::from_iter_with_validity(value.iter())
    }
}
