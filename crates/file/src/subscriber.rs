use arrow::{
    array::RecordBatch,
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use flowgen_core::event::Event;
use futures::future::TryJoinAll;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{sync::broadcast::Sender, task::JoinHandle};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error deserializing data into binary format.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
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
                .map_err(Error::TokioJoin);
        });
        Ok(())
    }
}

/// A builder of the file reader.
pub struct Builder {
    config: super::config::Source,
    tx: Sender<Event>,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Source, tx: &Sender<Event>) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
        }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let mut file = File::open(self.config.path.clone()).map_err(Error::InputOutput)?;
            let (schema, _) = Format::default()
                .with_header(true)
                .infer_schema(&mut file, Some(100))
                .map_err(Error::Arrow)?;
            file.rewind().map_err(Error::InputOutput)?;

            let csv = ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(true)
                .with_batch_size(1000)
                .build(file)
                .map_err(Error::Arrow)?;

            for batch in csv {
                let record_batch = batch.map_err(Error::Arrow)?;
                let timestamp = Utc::now().timestamp_micros();
                let filename = match self.config.path.split("/").last() {
                    Some(filename) => filename,
                    None => break,
                };
                let file_chunk = format!("{}.{}", filename, timestamp);

                // let m = flowgen_core::message::FileMessage {
                //     record_batch,
                //     file_chunk,
                // };
                // self.tx
                //     .send(ChannelMessage::file(m))
                //     .map_err(Error::TokioSendMessage)?;
            }
            Ok(())
        });
        handle_list.push(handle);

        Ok(Subscriber { handle_list })
    }
}
