use std::{collections::HashMap, sync::Arc};

#[derive(serde::Deserialize, Clone, Debug)]
pub struct Processor {
    pub payload: HashMap<String, String>,
    pub endpoint: Arc<String>,
    pub kvs: Option<HashMap<String, String>>,
}
