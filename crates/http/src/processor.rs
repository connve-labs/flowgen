use flowgen_core::message::{ChannelMessage, HttpMessage};
use futures_util::future::TryJoinAll;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error reading/writing/seeking file.")]
    InputOutput(#[source] std::io::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<ChannelMessage>),
}

pub struct Processor {
    handle_list: Vec<JoinHandle<Result<(), Error>>>,
}

impl Processor {
    pub async fn process(self) -> Result<(), Error> {
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

/// A builder of the http processor.
pub struct Builder {
    config: super::config::Processor,
    tx: Sender<ChannelMessage>,
    rx: Receiver<ChannelMessage>,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(config: super::config::Processor, tx: &Sender<ChannelMessage>) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
            rx: tx.subscribe(),
        }
    }

    pub async fn build(mut self) -> Result<Processor, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client = reqwest::Client::builder().https_only(true).build().unwrap();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            while let Ok(message) = self.rx.recv().await {
                match message {
                    ChannelMessage::http(_) => {}
                    _ => {
                        let response = client
                            .get(self.config.endpoint.as_str())
                            .send()
                            .await
                            .unwrap()
                            .json::<HashMap<String, String>>()
                            .await
                            .unwrap();

                        let m = HttpMessage {
                            response,
                            metadata: None,
                        };

                        self.tx
                            .send(ChannelMessage::http(m))
                            .map_err(Error::TokioSendMessage)?;
                    }
                }
            }
            Ok(())
        });

        handle_list.push(handle);

        Ok(Processor { handle_list })
    }
}
