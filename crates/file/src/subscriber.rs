use arrow::{
    array::RecordBatch,
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use flowgen_core::event::{Event, EventBuilder};
use futures::future::TryJoinAll;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{sync::broadcast::Sender, task::JoinHandle};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error reading file")]
    IO(#[source] std::io::Error),
    #[error("error executing async task")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("error deserializing data into binary format")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error with sending message over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error constructing Flowgen Event")]
    Event(#[source] flowgen_core::event::Error),
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
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Subscriber {
    pub async fn subscribe(self) -> Result<(), Error> {
        tokio::spawn(async move {
            let _ = self
                .handle_list
                .into_iter()
                .collect::<TryJoinAll<_>>()
                .await
                .map_err(Error::TaskJoin);
        });
        Ok(())
    }
}

/// A builder of the file reader.
pub struct SubscriberBuilder {
    config: super::config::Source,
    tx: Sender<Event>,
    current_task_id: usize,
}

impl SubscriberBuilder {
    /// Creates a new instance of a Builder.
    pub fn new(
        config: super::config::Source,
        tx: &Sender<Event>,
        current_task_id: usize,
    ) -> SubscriberBuilder {
        SubscriberBuilder {
            config,
            tx: tx.clone(),
            current_task_id,
        }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let mut file = File::open(self.config.path.clone()).map_err(Error::IO)?;
            let (schema, _) = Format::default()
                .with_header(true)
                .infer_schema(&mut file, Some(100))
                .map_err(Error::Arrow)?;
            file.rewind().map_err(Error::IO)?;

            let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(true)
                .with_batch_size(1000)
                .build(file)
                .map_err(Error::Arrow)?;

            for batch in csv {
                let recordbatch = batch.map_err(Error::Arrow)?;
                let timestamp = Utc::now().timestamp_micros();
                let filename = match self.config.path.split("/").last() {
                    Some(filename) => filename,
                    None => break,
                };
                let subject = format!("{}.{}", filename, timestamp);

                let e = EventBuilder::new()
                    .data(recordbatch)
                    .subject(subject)
                    .current_task_id(self.current_task_id)
                    .build()
                    .map_err(Error::Event)?;

                self.tx.send(e).map_err(Error::SendMessage)?;
            }
            Ok(())
        });
        handle_list.push(handle);

        Ok(Subscriber { handle_list })
    }
}
