use arrow::array::{Array, GenericStringArray, OffsetSizeTrait};
use arrow::buffer::OffsetBuffer;
use std::fmt::Display;
use std::io::Write;
use std::str::FromStr;

use crate::array::{
    CellIndexArray, DirectedEdgeIndexArray, H3Array, H3IndexArrayValue, VertexIndexArray,
};
use geo_types::Coord;
use h3o::{CellIndex, DirectedEdgeIndex, LatLng, Resolution, VertexIndex};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while_m_n};
use nom::combinator::map_res;
use nom::number::complete::double;
use nom::IResult;

use crate::error::Error;

pub fn parse_cell(s: &str) -> Result<CellIndex, Error> {
    if let Ok(cell) = CellIndex::from_str(s) {
        return Ok(cell);
    }

    if let Ok(cell_int) = u64::from_str(s) {
        if let Ok(cell) = CellIndex::try_from(cell_int) {
            return Ok(cell);
        }
    }

    // attempt to parse as coordinate pair and resolution
    if let Ok((_, (coord, res))) = parse_coordinate_and_resolution(s) {
        return Ok(LatLng::new(coord.y, coord.x)?.to_cell(Resolution::try_from(res)?));
    }

    Err(Error::NonParsableCellIndex)
}

pub fn parse_directededge(s: &str) -> Result<DirectedEdgeIndex, Error> {
    if let Ok(de) = DirectedEdgeIndex::from_str(s) {
        return Ok(de);
    }

    if let Ok(index_int) = u64::from_str(s) {
        if let Ok(de) = DirectedEdgeIndex::try_from(index_int) {
            return Ok(de);
        }
    }
    Err(Error::NonParsableDirectedEdgeIndex)
}

pub fn parse_vertex(s: &str) -> Result<VertexIndex, Error> {
    if let Ok(vx) = VertexIndex::from_str(s) {
        return Ok(vx);
    }

    if let Ok(index_int) = u64::from_str(s) {
        if let Ok(vx) = VertexIndex::try_from(index_int) {
            return Ok(vx);
        }
    }
    Err(Error::NonParsableVertexIndex)
}

fn is_whitespace(c: char) -> bool {
    c.is_ascii_whitespace()
}

fn seperator(s: &str) -> IResult<&str, &str> {
    alt((tag(","), (tag(";"))))(s)
}

fn u8_str(s: &str) -> IResult<&str, u8> {
    map_res(take_while_m_n(1, 2, |c: char| c.is_ascii_digit()), |u8s| {
        u8::from_str(u8s)
    })(s)
}

fn parse_coordinate_and_resolution(s: &str) -> IResult<&str, (Coord, u8)> {
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, x) = double(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, _) = seperator(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, y) = double(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, _) = seperator(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, r) = u8_str(s)?;
    Ok((s, (Coord::from((x, y)), r)))
}

pub trait ToGenericStringArray<O: OffsetSizeTrait> {
    fn to_genericstringarray(&self) -> Result<GenericStringArray<O>, Error>;
}

impl<O: OffsetSizeTrait, IX> ToGenericStringArray<O> for H3Array<IX>
where
    IX: H3IndexArrayValue + Display,
{
    fn to_genericstringarray(&self) -> Result<GenericStringArray<O>, Error> {
        let mut values: Vec<u8> =
            Vec::with_capacity(self.len() * 16 /* assuming 64bit hex values */);
        let mut offsets: Vec<O> = Vec::with_capacity(self.len() + 1);
        offsets.push(O::default());

        for value in self.iter() {
            if let Some(value) = value {
                write!(&mut values, "{}", value)?;
            }
            offsets.push(O::from_usize(values.len()).unwrap());
        }
        values.shrink_to_fit();

        Ok(GenericStringArray::<O>::new(
            OffsetBuffer::new(offsets.into()),
            values.into(),
            self.primitive_array().nulls().cloned(),
        ))
    }
}

impl<IX, O: OffsetSizeTrait> TryFrom<H3Array<IX>> for GenericStringArray<O>
where
    H3Array<IX>: ToGenericStringArray<O>,
    IX: H3IndexArrayValue + Display,
{
    type Error = Error;

    fn try_from(value: H3Array<IX>) -> Result<Self, Self::Error> {
        value.to_genericstringarray()
    }
}

/// parse H3 indexes from string arrays
pub trait ParseGenericStringArray {
    /// parse H3 indexes from string arrays
    ///
    /// Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    /// the successful parsing of an individual element. Having this set to false will cause the
    /// method to fail upon encountering the first unparsable value.
    ///
    /// This function is able to parse multiple representations of H3 cells:
    ///
    /// * hexadecimal (Example: "8552dc63fffffff")
    /// * numeric integer strings (Example: "600436454824345599")
    /// * strings like "[x], [y], [resolution]" or "[x]; [y]; [resolution]". (Example: "10.2,45.5,5")
    fn parse_genericstringarray<O: OffsetSizeTrait>(
        utf8array: &GenericStringArray<O>,
        set_failing_to_invalid: bool,
    ) -> Result<Self, Error>
    where
        Self: Sized;
}

macro_rules! impl_parse_genericstringarray {
    ($arr:ty, $conv: expr) => {
        impl ParseGenericStringArray for $arr {
            fn parse_genericstringarray<O: OffsetSizeTrait>(
                genericstringarray: &GenericStringArray<O>,
                set_failing_to_invalid: bool,
            ) -> Result<Self, Error> {
                let h3indexes = if set_failing_to_invalid {
                    genericstringarray
                        .iter()
                        .map(|value| match value {
                            Some(value_str) => match $conv(value_str) {
                                Ok(cell) => Ok(Some(cell)),
                                Err(_) => Ok(None),
                            },
                            None => Ok(None),
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                } else {
                    genericstringarray
                        .iter()
                        .map(|value| match value {
                            Some(value_str) => match $conv(value_str) {
                                Ok(cell) => Ok(Some(cell)),
                                Err(e) => Err(e.into()),
                            },
                            None => Ok(None),
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                };
                Ok(h3indexes.into())
            }
        }
    };
}

impl_parse_genericstringarray!(VertexIndexArray, parse_vertex);
impl_parse_genericstringarray!(DirectedEdgeIndexArray, parse_directededge);
impl_parse_genericstringarray!(CellIndexArray, parse_cell);

impl<IX, O: OffsetSizeTrait> TryFrom<GenericStringArray<O>> for H3Array<IX>
where
    H3Array<IX>: ParseGenericStringArray + Sized,
{
    type Error = Error;

    fn try_from(value: GenericStringArray<O>) -> Result<Self, Self::Error> {
        Self::parse_genericstringarray(&value, false)
    }
}

#[cfg(test)]
mod test {
    use crate::algorithm::{parse_cell, ParseGenericStringArray, ToGenericStringArray};
    use crate::array::{CellIndexArray, FromWithValidity};
    use arrow::array::{Array, GenericStringArray};
    use h3o::{CellIndex, LatLng, Resolution};

    #[test]
    fn parse_cell_from_numeric() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();
        let s = format!("{}", u64::from(cell));

        let cell2 = parse_cell(&s).unwrap();
        assert_eq!(cell, cell2);
    }

    #[test]
    fn parse_cell_from_coordinate_and_resolution() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();
        let ll = LatLng::from(cell);
        let s = format!("{},{},{}", ll.lng(), ll.lat(), cell.resolution());

        let cell2 = parse_cell(&s).unwrap();
        assert_eq!(cell, cell2);
    }

    #[test]
    fn parse_utf8_array_cells() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();

        let stringarray = GenericStringArray::<i32>::from_iter(
            vec![cell.to_string(), u64::from(cell).to_string()]
                .into_iter()
                .map(Some),
        );
        let cell_array = CellIndexArray::parse_genericstringarray(&stringarray, false).unwrap();
        assert_eq!(cell_array.len(), stringarray.len());
        assert!(cell_array.iter().all(|v| v == Some(cell)))
    }

    #[test]
    fn parse_utf8_array_cells_invalid_fail() {
        let stringarray =
            GenericStringArray::<i32>::from_iter(vec![Some("invalid".to_string())].into_iter());
        assert!(CellIndexArray::parse_genericstringarray(&stringarray, false).is_err());
    }

    #[test]
    fn parse_utf8_array_cells_invalid_to_invalid() {
        let utf8_array =
            GenericStringArray::<i32>::from_iter(vec![Some("invalid".to_string())].into_iter());
        let cell_array = CellIndexArray::parse_genericstringarray(&utf8_array, true).unwrap();
        assert_eq!(1, cell_array.len());
        assert!(cell_array.iter().all(|v| v.is_none()))
    }

    #[test]
    fn to_stringarray() {
        let cellindexarray =
            CellIndexArray::from_with_validity(vec![Some(0x89283080ddbffff_u64), None]);

        let stringarray: GenericStringArray<i64> = cellindexarray.to_genericstringarray().unwrap();

        assert_eq!(cellindexarray.len(), stringarray.len());
        assert_eq!(stringarray.is_valid(0), true);
        assert_eq!(stringarray.value(0), "89283080ddbffff");
        assert_eq!(stringarray.is_valid(1), false);
    }

    #[test]
    fn to_stringarray_roundtrip() {
        let arr: CellIndexArray = vec![
            LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five),
            LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine),
        ]
        .into();

        let stringarray: GenericStringArray<i32> = arr.clone().try_into().unwrap();
        assert_eq!(stringarray.len(), arr.len());

        assert_eq!(
            stringarray.iter().flatten().collect::<Vec<_>>(),
            vec!["855968a3fffffff", "89599da10d3ffff"]
        );

        let arr2: CellIndexArray = stringarray.try_into().unwrap();
        assert!(arr == arr2);
    }
}
