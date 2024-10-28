use serde::Deserialize;

#[derive(Deserialize)]
pub struct Nats {
    pub credentials: String,
    pub host: String,
    pub stream_name: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub kv_bucket_name: String,
    pub kv_bucket_description: String,
}

#[derive(Deserialize)]
#[allow(non_camel_case_types)]
pub enum Source {
    salesforce(flowgen_salesforce::config::Source),
}

#[derive(Deserialize)]
#[allow(non_camel_case_types)]
pub enum Target {
    nats(Nats),
}

#[derive(Deserialize)]
pub struct Flow {
    pub source: Source,
    pub target: Target,
}
