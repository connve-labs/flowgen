use arrow::{
    csv::{reader::Format, ReaderBuilder},
    ipc::writer::StreamWriter,
};
use futures::future::try_join_all;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error deserializing data into binary format.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::mpsc::error::SendError<Vec<u8>>),
}

pub struct Subscriber {
    pub path: String,
    pub async_task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
    pub rx: Receiver<Vec<u8>>,
    tx: Sender<Vec<u8>>,
}

impl Subscriber {
    pub fn init(mut self) -> Result<Self, Error> {
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let tx = self.tx.clone();
        let path = self.path.clone();

        let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let mut file = File::open(path).map_err(Error::InputOutput)?;
            let (schema, _) = Format::default()
                .infer_schema(&mut file, Some(100))
                .map_err(Error::Arrow)?;
            file.rewind().map_err(Error::InputOutput)?;

            let mut csv = ReaderBuilder::new(Arc::new(schema.clone()))
                .build(file)
                .map_err(Error::Arrow)?;

            if let Some(batch) = csv.next() {
                let record_batch = batch.map_err(Error::Arrow)?;
                let buffer: Vec<u8> = Vec::new();

                let mut stream_writer =
                    StreamWriter::try_new(buffer, &schema).map_err(Error::Arrow)?;
                stream_writer.write(&record_batch).map_err(Error::Arrow)?;
                stream_writer.finish().map_err(Error::Arrow)?;

                tx.send(stream_writer.get_ref().to_vec())
                    .await
                    .map_err(Error::TokioSendMessage)?;
            }
            Ok(())
        });

        async_task_list.push(subscribe_task);
        self.async_task_list = Some(async_task_list);

        Ok(self)
    }

    pub async fn subscribe(&mut self) -> Result<(), Error> {
        if let Some(async_task_list) = self.async_task_list.take() {
            try_join_all(async_task_list)
                .await
                .map_err(Error::TokioJoin)?
                .into_iter()
                .try_for_each(|result| result)?;
        }
        Ok(())
    }
}

/// A builder of the file reader.
pub struct Builder {
    config: super::config::Source,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Source) -> Builder {
        Builder { config }
    }

    pub async fn build(self) -> Result<Subscriber, Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(200);
        Ok(Subscriber {
            path: self.config.path,
            async_task_list: None,
            tx,
            rx,
        })
    }
}
