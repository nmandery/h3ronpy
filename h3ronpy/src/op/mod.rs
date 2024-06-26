use pyo3::prelude::*;

mod compact;
mod localij;
mod measure;
mod neighbor;
mod resolution;
mod string;
mod valid;

pub fn init_op_submodule(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolution::change_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(resolution::change_resolution_list, m)?)?;
    m.add_function(wrap_pyfunction!(resolution::change_resolution_paired, m)?)?;
    m.add_function(wrap_pyfunction!(resolution::cells_resolution, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk_distances, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_ring_distances, m)?)?;
    m.add_function(wrap_pyfunction!(neighbor::grid_disk_aggregate_k, m)?)?;
    m.add_function(wrap_pyfunction!(string::cells_parse, m)?)?;
    m.add_function(wrap_pyfunction!(string::vertexes_parse, m)?)?;
    m.add_function(wrap_pyfunction!(string::directededges_parse, m)?)?;
    m.add_function(wrap_pyfunction!(string::cells_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(string::vertexes_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(string::directededges_to_string, m)?)?;
    m.add_function(wrap_pyfunction!(compact::compact, m)?)?;
    m.add_function(wrap_pyfunction!(compact::uncompact, m)?)?;
    m.add_function(wrap_pyfunction!(valid::cells_valid, m)?)?;
    m.add_function(wrap_pyfunction!(valid::vertexes_valid, m)?)?;
    m.add_function(wrap_pyfunction!(valid::directededges_valid, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_m2, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_km2, m)?)?;
    m.add_function(wrap_pyfunction!(measure::cells_area_rads2, m)?)?;
    m.add_function(wrap_pyfunction!(localij::cells_to_localij, m)?)?;
    m.add_function(wrap_pyfunction!(localij::localij_to_cells, m)?)?;

    Ok(())
}
