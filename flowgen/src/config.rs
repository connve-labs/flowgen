use std::fmt::Display;

use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub flow: Flow,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Flow {
    pub tasks: Vec<Task>, // pub source: Source,
                          // pub processor: Option<Vec<Processor>>,
                          // pub target: Target,
}

#[derive(Deserialize, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Task {
    source(Source),
    processor(Processor),
    target(Target),
}

#[derive(Deserialize, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Source {
    file(flowgen_file::config::Source),
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Source),
    gcp_storage(flowgen_google::storage::config::Source),
    nats_jetstream(flowgen_nats::jetstream::config::Source),
}

#[derive(Deserialize, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Processor {
    http(flowgen_http::config::Processor),
}

#[derive(Deserialize, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::config::Target),
    deltalake(flowgen_deltalake::config::Target),
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Target),
}
