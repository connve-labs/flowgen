use arrow::{
    array::RecordBatch,
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use chrono::Utc;
use flowgen_core::messages::ChannelMessage;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{sync::broadcast::Sender, sync::mpsc::Receiver, task::JoinHandle};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error deserializing data into binary format.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::mpsc::error::SendError<ChannelMessage>),
}

pub trait Converter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

impl Converter for RecordBatch {
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
    pub async_task_list: Vec<JoinHandle<Result<(), Error>>>,
    pub path: String,
    // pub rx: Receiver<Message>,
    // pub tx: Sender<Message>,
}

/// A builder of the file reader.
pub struct Builder {
    config: super::config::Source,
    tx: Sender<ChannelMessage>,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Source, tx: &Sender<ChannelMessage>) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
        }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        // let (tx, rx) = tokio::sync::broadcast::channel(200);
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let path = self.config.path.clone();

        {
            // let tx = self.tx.clone();
            let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let mut file = File::open(path.clone()).map_err(Error::InputOutput)?;
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
                    let filename = match path.split("/").last() {
                        Some(filename) => filename,
                        None => break,
                    };
                    let file_chunk = format!("{}.{}", filename, timestamp);
                    let m = flowgen_core::messages::FileMessage {
                        record_batch,
                        file_chunk,
                    };
                    self.tx.send(ChannelMessage::FileMessage(m)).unwrap();
                }
                Ok(())
            });
            async_task_list.push(subscribe_task);
        }

        Ok(Subscriber {
            path: self.config.path,
            async_task_list,
            // tx,
            // rx,
        })
    }
}
