use std::{collections::HashMap, sync::Arc};

#[derive(serde::Deserialize, Clone, Debug)]
pub struct Processor {
    pub payload: HashMap<String, String>,
    pub endpoint: Arc<String>,
    pub metadata: Option<HashMap<String, String>>,
}
