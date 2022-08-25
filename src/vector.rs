use geo_types::Geometry;
use std::io::Cursor;

use geozero::wkb::{FromWkb, WkbDialect};
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::{prelude::*, wrap_pyfunction};
use rayon::prelude::*;

use h3ron::{compact_cells, H3Cell, Index, ToH3Cells};

use crate::error::IntoPyResult;

fn wkbbytes_to_h3(wkbdata: &[u8], h3_resolution: u8, do_compact: bool) -> PyResult<Vec<u64>> {
    // geozero parses empty geometries as Point(0.0, 0.0), so for now we sort out empty geometries
    // based on the number of bytes
    if wkbdata.len() <= 9 {
        return Ok(vec![]);
    }
    let mut cursor = Cursor::new(wkbdata);
    match Geometry::from_wkb(&mut cursor, WkbDialect::Wkb) {
        Ok(g) => {
            let mut cells: Vec<H3Cell> = g
                .to_h3_cells(h3_resolution)
                .into_pyresult()?
                .iter()
                .collect();

            // deduplicate, in the case of overlaps or lines
            cells.sort_unstable();
            cells.dedup();

            if do_compact {
                cells = compact_cells(&cells).into_pyresult()?.iter().collect();
            }
            Ok(cells.drain(..).map(|i| i.h3index()).collect())
        }
        Err(err) => Err(PyValueError::new_err(format!("invalid WKB: {:?}", err))),
    }
}

#[allow(clippy::type_complexity)]
#[pyfunction]
fn wkbbytes_with_ids_to_h3(
    py: Python,
    id_array: PyReadonlyArray1<u64>,
    wkb_list: Vec<&[u8]>,
    h3_resolution: u8,
    do_compact: bool,
) -> PyResult<(Py<PyArray<u64, Ix1>>, Py<PyArray<u64, Ix1>>)> {
    // the solution with the argument typed as list of byte-instances is not great. This
    // maybe can be improved with https://github.com/PyO3/rust-numpy/issues/175

    if id_array.len() != wkb_list.len() {
        return Err(PyValueError::new_err(
            "input Ids and WKBs must be of the same length",
        ));
    }
    let out = id_array
        .as_array()
        .iter()
        .zip(wkb_list.iter())
        .par_bridge()
        .map(|(id, wkbdata)| {
            wkbbytes_to_h3(*wkbdata, h3_resolution, do_compact).map(|h3indexes| (*id, h3indexes))
        })
        .try_fold(
            || (vec![], vec![]),
            |mut a, b| match b {
                Ok((id, mut indexes)) => {
                    for _ in 0..indexes.len() {
                        a.0.push(id);
                    }
                    a.1.append(&mut indexes);
                    Ok(a)
                }
                Err(err) => Err(err),
            },
        )
        .try_reduce(
            || (vec![], vec![]),
            |mut a, mut b| {
                b.0.append(&mut a.0);
                b.1.append(&mut a.1);
                Ok(b)
            },
        )?;

    Ok((
        out.0.into_pyarray(py).to_owned(),
        out.1.into_pyarray(py).to_owned(),
    ))
}

pub fn init_vector_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(wkbbytes_with_ids_to_h3, m)?)?;
    Ok(())
}
