use super::config;
use arrow::array::RecordBatch;
use async_nats::jetstream::context::Publish;
use bytes::Bytes;
use chrono::Utc;
use flowgen_core::{
    client::Client,
    message::{ChannelMessage, SalesforcePubSubMessage},
};
use flowgen_file::subscriber::RecordBatchConverter;
use futures::future::{try_join_all, TryJoinAll};
use std::{any::Any, ops::DerefMut, path::PathBuf, sync::Arc};
use tokio::{
    sync::{
        broadcast::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, event, info, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] serde_json::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("There was an error with Flowgen Nats JetStream Publisher.")]
    FlowgenNatsJetStreamPublisher(#[source] flowgen_nats::jetstream::publisher::Error),
    #[error("There was an error with Flowgen Nats JetStream Subscriber.")]
    FlowgenNatsJetStreamSubscriber(#[source] flowgen_nats::jetstream::subscriber::Error),
    #[error("There was an error with Flowgen File Subscriber.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::Error),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
}

#[allow(non_camel_case_types)]
pub enum Processor {}

pub struct Flow {
    config: config::Config,
    pub handle_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
}

impl Flow {
    pub async fn run(mut self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!("{0}:443", "https://api.pubsub.salesforce.com"))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        let config = self.config.clone();

        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, mut rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) =
            tokio::sync::broadcast::channel(1000);

        // Setup source subscribers.
        match config.flow.source {
            config::Source::nats_jetstream(config) => {
                flowgen_nats::jetstream::subscriber::Builder::new(config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamSubscriber)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamSubscriber)?;
            }
            config::Source::file(config) => {
                flowgen_file::subscriber::Builder::new(config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?;
            }
            config::Source::salesforce_pubsub(config) => {
                flowgen_salesforce::pubsub::subscriber::Builder::new(service.clone(), config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?
                    .subscribe()
                    .await
                    .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
            }
            _ => {
                info!("unimplemented");
            }
        }

        for processor in config.flow.processor.iter() {
            match processor {
                _ => {
                    println!("here")
                }
            }
        }

        // Setup target publishers.
        match config.flow.target {
            config::Target::nats_jetstream(config) => {
                let publisher = flowgen_nats::jetstream::publisher::Builder::new(config)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamPublisher)?;
                let publisher = Arc::new(publisher);

                {
                    let publisher = publisher.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        while let Ok(message) = rx.recv().await {
                            if let ChannelMessage::file(m) = message {
                                let event = m.record_batch.to_bytes().unwrap();
                                let subject = format!("filedrop.in.{}", m.file_chunk);

                                publisher
                                    .jetstream
                                    .send_publish(subject, Publish::build().payload(event.into()))
                                    .await
                                    .map_err(Error::NatsPublish)?;

                                event!(Level::INFO, "file_chunk: {}", m.file_chunk);
                            }
                        }
                        Ok(())
                    });
                    handle_list.push(handle);
                }

                {
                    let publisher = publisher.clone();
                    let mut rx = tx.subscribe();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        while let Ok(message) = rx.recv().await {
                            if let ChannelMessage::salesforce_pubsub(m) = message {
                                let event = Bytes::from(m.clone());
                                let s = m.topic_info.topic_name.replace('/', ".").to_lowercase();
                                let event_name = &s[1..];
                                let subject = format!(
                                    "salesforce.pubsub.in.{}.{}",
                                    event_name, m.fetch_response.rpc_id
                                );

                                publisher
                                    .jetstream
                                    .send_publish(subject, Publish::build().payload(event))
                                    .await
                                    .map_err(Error::NatsPublish)?;

                                event!(
                                    Level::INFO,
                                    "salesforce_pubsub: {}",
                                    m.fetch_response.rpc_id
                                );
                            }
                        }
                        Ok(())
                    });
                    handle_list.push(handle);
                }
            }

            config::Target::deltalake(config) => {
                // let publisher = flowgen_deltalake::publisher::Builder::new(config)
                //     .build()
                //     .await
                //     .unwrap();
                let mut rx = tx.subscribe();
                let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Ok(message) = rx.recv().await {
                        if let ChannelMessage::nats_jetstream(m) = message {
                            // m.into()
                        }
                        // match message {
                        //     ChannelMessage::FileMessage(m) => {
                        //         let event = m.record_batch.to_bytes().unwrap();
                        //         let subject = format!("filedrop.in.{}", m.file_chunk);
                        //         publisher
                        //             .jetstream
                        //             .send_publish(subject, Publish::build().payload(event.into()))
                        //             .await
                        //             .map_err(Error::NatsPublish)?;
                        //         event!(Level::INFO, "event: file processed {}", m.file_chunk);
                        //     }
                        // }
                    }
                    Ok(())
                });
                handle_list.push(handle);
            }
            config::Target::salesforce_pubsub(config) => {
                let publisher =
                    flowgen_salesforce::pubsub::publisher::Builder::new(config, service)
                        .build()
                        .await
                        .unwrap();

                let mut rx = tx.subscribe();
                let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Ok(message) = rx.recv().await {
                        if let ChannelMessage::nats_jetstream(m) = message {
                            println!("{:?}", m);
                            // m.into()
                        }
                        // match message {
                        //     ChannelMessage::FileMessage(m) => {
                        //         let event = m.record_batch.to_bytes().unwrap();
                        //         let subject = format!("filedrop.in.{}", m.file_chunk);
                        //         publisher
                        //             .jetstream
                        //             .send_publish(subject, Publish::build().payload(event.into()))
                        //             .await
                        //             .map_err(Error::NatsPublish)?;
                        //         event!(Level::INFO, "event: file processed {}", m.file_chunk);
                        //     }
                        // }
                    }
                    Ok(())
                });
                handle_list.push(handle);
            }
        }
        self.handle_list = Some(handle_list);
        Ok(self)
    }
}

#[derive(Default)]
pub struct Builder {
    config_path: PathBuf,
}

impl Builder {
    pub fn new(config_path: PathBuf) -> Builder {
        Builder { config_path }
    }
    pub fn build(&mut self) -> Result<Flow, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = serde_json::from_str(&c).map_err(Error::ParseConfig)?;
        let f = Flow {
            config,
            handle_list: None,
        };
        Ok(f)
    }
}
