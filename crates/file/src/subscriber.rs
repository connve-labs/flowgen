use arrow::{
    array::RecordBatch,
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use flowgen_core::stream::event::{Event, EventBuilder};
use futures::future::try_join_all;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{sync::broadcast::Sender, task::JoinHandle};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "file.in";
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_HAS_HEADER: bool = true;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error reading file")]
    IO(#[source] std::io::Error),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error constructing Flowgen Event")]
    Event(#[source] flowgen_core::stream::event::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
}

pub trait RecordBatchConverter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl RecordBatchConverter for RecordBatch {
    type Error = Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let buffer: Vec<u8> = Vec::new();

        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.schema()).map_err(Error::Arrow)?;
        stream_writer.write(self).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;

        Ok(stream_writer.get_mut().to_vec())
    }
}

pub struct Subscriber {
    config: Arc<super::config::Source>,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let config = Arc::clone(&self.config);
            let mut file = File::open(&config.path).map_err(Error::IO)?;
            let (schema, _) = Format::default()
                .with_header(true)
                .infer_schema(&mut file, Some(100))
                .map_err(Error::Arrow)?;
            file.rewind().map_err(Error::IO)?;

            let batch_size = match self.config.batch_size {
                Some(batch_size) => batch_size,
                None => DEFAULT_BATCH_SIZE,
            };

            let has_header = match self.config.has_header {
                Some(has_header) => has_header,
                None => DEFAULT_HAS_HEADER,
            };

            let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(has_header)
                .with_batch_size(batch_size)
                .build(file)
                .map_err(Error::Arrow)?;

            for batch in csv {
                let recordbatch = batch.map_err(Error::Arrow)?;
                let timestamp = Utc::now().timestamp_micros();
                let subject = match &config.path.split("/").last() {
                    Some(filename) => {
                        format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, filename, timestamp)
                    }
                    None => format!("{}.{}", DEFAULT_MESSAGE_SUBJECT, timestamp),
                };

                let e = EventBuilder::new()
                    .data(recordbatch)
                    .subject(subject)
                    .current_task_id(self.current_task_id)
                    .build()
                    .map_err(Error::Event)?;

                event!(Level::INFO, "event received: {}", e.subject);
                self.tx.send(e).map_err(Error::SendMessage)?;
            }
            Ok(())
        });
        handle_list.push(handle);

        let _ = try_join_all(handle_list.iter_mut()).await;

        Ok(())
    }
}

#[derive(Default)]
pub struct SubscriberBuilder {
    config: Option<Arc<super::config::Source>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Source>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
