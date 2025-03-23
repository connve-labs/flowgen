use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Source {
    pub path: String,
    pub batch_size: Option<usize>,
    pub has_header: Option<bool>,
}

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Target {
    pub path: PathBuf,
}
