use geo_types::Geometry;
use std::io::Cursor;

use geozero::wkb::{FromWkb, WkbDialect};
use numpy::{IntoPyArray, Ix1, PyArray, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::{prelude::*, wrap_pyfunction};
use rayon::prelude::*;

use h3ron::{compact_cells, H3Cell, Index, ToH3Cells, ToIntersectingH3Cells};
use ndarray::{Array1, Zip};
use pyo3::types::PyBytes;

use crate::error::IntoPyResult;

fn geom_to_h3(
    geom: &Geometry,
    h3_resolution: u8,
    do_compact: bool,
    partial_intersecting: bool,
) -> PyResult<Vec<u64>> {
    let mut cells: Vec<H3Cell> = match (geom, partial_intersecting) {
        (Geometry::Polygon(poly), true) => poly
            .to_intersecting_h3_cells(h3_resolution)
            .into_pyresult()?,
        (Geometry::MultiPolygon(mpoly), true) => mpoly
            .to_intersecting_h3_cells(h3_resolution)
            .into_pyresult()?,
        _ => geom
            .to_h3_cells(h3_resolution)
            .into_pyresult()?
            .iter()
            .collect(),
    };

    // deduplicate, in the case of overlaps or lines
    cells.sort_unstable();
    cells.dedup();

    let cells = if do_compact {
        compact_cells(&cells)
            .into_pyresult()?
            .iter()
            .map(|i| i.h3index())
            .collect()
    } else {
        cells.into_iter().map(|i| i.h3index()).collect()
    };
    Ok(cells)
}

#[allow(clippy::type_complexity)]
#[pyfunction]
fn wkbbytes_with_ids_to_h3(
    id_array: PyReadonlyArray1<u64>,
    wkb_array: PyReadonlyArray1<PyObject>,
    h3_resolution: u8,
    do_compact: bool,
    partial_intersecting: bool,
) -> PyResult<(Py<PyArray<u64, Ix1>>, Py<PyArray<u64, Ix1>>)> {
    // the solution with the argument typed as list of byte-instances is not great. This
    // maybe can be improved with https://github.com/PyO3/rust-numpy/issues/175

    if id_array.len() != wkb_array.len() {
        return Err(PyValueError::new_err(
            "input Ids and WKBs must be of the same length",
        ));
    }

    let geom_array: Array1<_> = extract_geometries(wkb_array)?.into();

    let out = Zip::from(id_array.as_array())
        .and(geom_array.view())
        .into_par_iter()
        .map(|(id, geom)| {
            geom_to_h3(geom, h3_resolution, do_compact, partial_intersecting)
                .map(|h3indexes| (*id, h3indexes))
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

    Ok(Python::with_gil(|py| {
        (
            out.0.into_pyarray(py).to_owned(),
            out.1.into_pyarray(py).to_owned(),
        )
    }))
}

pub fn init_vector_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(wkbbytes_with_ids_to_h3, m)?)?;
    Ok(())
}

fn extract_geometries(array: PyReadonlyArray1<PyObject>) -> PyResult<Vec<Geometry>> {
    let array = array.as_array();
    Python::with_gil(|py| {
        array
            .iter()
            .map(|obj| match obj.extract::<&PyBytes>(py) {
                // is WKB
                Ok(pb) => {
                    let mut cur = Cursor::new(pb.as_bytes());
                    Geometry::from_wkb(&mut cur, WkbDialect::Wkb)
                        .map_err(|e| PyValueError::new_err(format!("unable to parse WKB: {:?}", e)))
                }

                // is some kind ob object, trying to extract a geometry from it
                Err(_) => obj.extract::<py_geo_interface::Geometry>(py).map(|gi| gi.0),
            })
            .collect::<PyResult<Vec<_>>>()
    })
}
