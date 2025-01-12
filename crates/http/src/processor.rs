use flowgen_core::event::{Event, EventBuilder, RecordBatchExt};
use futures_util::future::TryJoinAll;
use serde_json::Value;
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
    #[error("There was an error with sending event over channel.")]
    TokioSendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("There was an error constructing Flowgen Event.")]
    FlowgenEvent(#[source] flowgen_core::event::Error),
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
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Builder {
    /// Creates a new instance of a Builder.
    pub fn new(
        config: super::config::Processor,
        tx: &Sender<Event>,
        current_task_id: usize,
    ) -> Builder {
        Builder {
            config,
            tx: tx.clone(),
            rx: tx.subscribe(),
            current_task_id,
        }
    }

    pub async fn build(mut self) -> Result<Processor, Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client = reqwest::Client::builder().https_only(true).build().unwrap();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            while let Ok(m) = self.rx.recv().await {
                if m.current_task_id == Some(self.current_task_id - 1) {
                    let response = client
                        .get(self.config.endpoint.as_str())
                        .send()
                        .await
                        .unwrap()
                        .json::<Value>()
                        .await
                        .unwrap();

                    let data = response.to_recordbatch().unwrap();
                    let subject = "http.respone.out".to_string();

                    let e = EventBuilder::new()
                        .data(data)
                        .subject(subject)
                        .current_task_id(self.current_task_id)
                        .build()
                        .map_err(Error::FlowgenEvent)?;

                    self.tx.send(e).map_err(Error::TokioSendMessage)?;
                }
            }
            Ok(())
        });

        handle_list.push(handle);

        Ok(Processor { handle_list })
    }
}
