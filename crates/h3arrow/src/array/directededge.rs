use crate::array::{CellIndexArray, DirectedEdgeIndexArray};
use arrow::array::Float64Array;

impl DirectedEdgeIndexArray {
    pub fn origin(&self) -> CellIndexArray {
        self.iter()
            .map(|edge| edge.map(|edge| edge.origin()))
            .collect()
    }

    pub fn destination(&self) -> CellIndexArray {
        self.iter()
            .map(|edge| edge.map(|edge| edge.destination()))
            .collect()
    }

    pub fn length_rads(&self) -> Float64Array {
        self.iter()
            .map(|edge| edge.map(|edge| edge.length_rads()))
            .collect()
    }

    pub fn length_km(&self) -> Float64Array {
        self.iter()
            .map(|edge| edge.map(|edge| edge.length_km()))
            .collect()
    }

    pub fn length_m(&self) -> Float64Array {
        self.iter()
            .map(|edge| edge.map(|edge| edge.length_m()))
            .collect()
    }
}
