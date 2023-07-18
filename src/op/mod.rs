use pyo3::prelude::*;

mod compact;
mod measure;
mod neighbor;
mod resolution;
mod utf8;
mod valid;

pub fn init_op_submodule(m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolution::change_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(resolution::change_resolution_paired, m)?)?;
    m.add_function(wrap_pyfunction!(resolution::cells_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk_distances, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_ring_distances, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk_aggregate_k, m)?)?;
    m.add_function(wrap_pyfunction!(utf8::cells_parse, m)?)?;
    m.add_function(wrap_pyfunction!(utf8::cells_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(utf8::vertexes_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(utf8::directededges_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(compact::compact, m)?)?;
    m.add_function(wrap_pyfunction!(compact::uncompact, m)?)?;
    m.add_function(wrap_pyfunction!(valid::cells_valid, m)?)?;
    m.add_function(wrap_pyfunction!(valid::vertexes_valid, m)?)?;
    m.add_function(wrap_pyfunction!(valid::directededges_valid, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_m2, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_km2, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_rads2, m)?)?;

    Ok(())
}
