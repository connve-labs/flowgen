use flowgen_core::client::Client;
use std::{fs::File, io::Seek, sync::Arc};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error authorizating to Nats Client.")]
    NatsClientAuth(#[source] crate::client::Error),
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::mpsc::error::SendError<Vec<u8>>),
}

pub trait Converter {
    type Error;
    fn to_bytes(&self) -> Result<Vec<u8>, Self::Error>;
}

pub struct Subscriber {
    pub async_task_list: Vec<JoinHandle<Result<(), Error>>>,
    pub rx: Receiver<Vec<u8>>,
    pub tx: Sender<Vec<u8>>,
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
        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();

        // Connect to Nats Server.
        let client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        let jetstream = async_nats::jetstream::new(client.nats_client.unwrap());

        Ok(Subscriber {
            async_task_list,
            tx,
            rx,
        })
    }
}
