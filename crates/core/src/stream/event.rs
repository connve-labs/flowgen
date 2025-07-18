#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
}

#[derive(Debug, Clone)]
pub struct Event {
    pub data: EventData,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: String,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum EventData {
    ArrowRecordBatch(arrow::array::RecordBatch),
    Avro(AvroData),
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    pub schema: String,
    pub raw_bytes: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct EventBuilder {
    pub data: Option<EventData>,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
    pub id: Option<String>,
}

impl EventBuilder {
    pub fn new() -> Self {
        EventBuilder {
            ..Default::default()
        }
    }
    pub fn data(mut self, data: EventData) -> Self {
        self.data = Some(data);
        self
    }
    pub fn subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = Some(current_task_id);
        self
    }
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    pub fn extensions(mut self, extensions: arrow::array::RecordBatch) -> Self {
        self.extensions = Some(extensions);
        self
    }
    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            extensions: self.extensions,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            id: self.id,
            current_task_id: self.current_task_id,
        })
    }
}
