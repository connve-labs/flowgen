use flowgen_core::config::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub label: Option<String>,
    pub endpoint: String,
    pub method: HttpMethod,
    pub payload: Option<Payload>,
    pub headers: Option<HashMap<String, String>>,
    pub credentials: Option<String>,
    pub inputs: Option<HashMap<String, Input>>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Payload {
    pub object: Option<Map<String, Value>>,
    pub input: Option<String>,
    pub send_as: PayloadSendAs,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum PayloadSendAs {
    #[default]
    Json,
    UrlEncoded,
    QueryParams,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum HttpMethod {
    #[default]
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
}
