use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FileMessage {
    pub record_batch: arrow::array::RecordBatch,
    pub file_chunk: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SalesforcePubSubMessage {
    pub consumer_event: salesforce_pubsub::eventbus::v1::ConsumerEvent,
    pub topic_info: salesforce_pubsub::eventbus::v1::TopicInfo,
}

impl From<SalesforcePubSubMessage> for bytes::Bytes {
    fn from(val: SalesforcePubSubMessage) -> Self {
        let event: Vec<u8> = bincode::serialize(&val).unwrap();
        let bytes: bytes::Bytes = event.into();
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct HttpMessage {
    pub response: HashMap<String, String>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub enum ChannelMessage {
    nats_jetstream(async_nats::message::Message),
    file(FileMessage),
    salesforce_pubsub(SalesforcePubSubMessage),
    http(HttpMessage),
}
