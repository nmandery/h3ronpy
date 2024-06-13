use crate::array::{CellIndexArray, VertexIndexArray};

impl VertexIndexArray {
    pub fn owner(&self) -> CellIndexArray {
        self.iter().map(|vx| vx.map(|vx| vx.owner())).collect()
    }
}
