use super::config;
use arrow::array::RecordBatch;
use async_nats::jetstream::context::Publish;
use chrono::Utc;
use flowgen_core::{client::Client, messages::ChannelMessage};
use flowgen_file::subscriber::Converter;
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
    #[error("Failed to setup Nats JetStream as flow target.")]
    FlowgenNatsJetStreamPublisher(#[source] flowgen_nats::jetstream::publisher::Error),
    #[error("There was an error with Flowgen File Subscriber.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::Error),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
}

#[allow(non_camel_case_types)]
pub enum Source {
    file(flowgen_file::subscriber::Subscriber),
    salesforce_pubsub(flowgen_salesforce::pubsub::subscriber::Subscriber),
    gcp_storage(flowgen_google::storage::subscriber::Subscriber),
    nats_jetstream(flowgen_nats::jetstream::subscriber::Subscriber),
}

#[allow(non_camel_case_types)]
pub enum Processor {}

#[allow(non_camel_case_types)]
pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::publisher::Publisher),
}

pub struct Flow {
    config: config::Config,
    // source_channel: Option<Channel>,
    // target_channel: Option<Channel>,
    pub source: Option<Source>,
    pub processor: Option<Processor>,
    pub target: Option<Target>,
}

impl Flow {
    pub async fn init(mut self) -> Result<Self, Error> {
        // Setup Flowgen service.
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!(
                "{0}:443",
                flowgen_salesforce::pubsub::eventbus::ENDPOINT
            ))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        // Get cloned version of the config.
        let config = self.config.clone();

        let (tx, mut rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) =
            tokio::sync::broadcast::channel(200);

        // Setup source subscribers.
        match config.flow.source {
            config::Source::file(config) => {
                let subscriber = flowgen_file::subscriber::Builder::new(config, &tx)
                    .build()
                    .await
                    .map_err(Error::FlowgenFileSubscriberError)?;

                subscriber
                    .async_task_list
                    .into_iter()
                    .collect::<TryJoinAll<_>>()
                    .await
                    .unwrap();
            }
            config::Source::salesforce_pubsub(config) => {
                let subscriber =
                    flowgen_salesforce::pubsub::subscriber::Builder::new(service.clone(), config)
                        .build()
                        .await
                        .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
                self.source = Some(Source::salesforce_pubsub(subscriber));
            }
            config::Source::nats_jetstream(config) => {
                let subscriber = flowgen_nats::jetstream::subscriber::Builder::new(config)
                    .build()
                    .await
                    .unwrap();
                self.source = Some(Source::nats_jetstream(subscriber));
            }
            _ => {
                info!("unimplemented")
            }
        }

        // match config.flow.processor {
        //     _ => info!("unimplemented"),
        // }

        // Setup target publishers.
        match config.flow.target {
            config::Target::nats_jetstream(config) => {
                let publisher = flowgen_nats::jetstream::publisher::Builder::new(config)
                    .build()
                    .await
                    .map_err(Error::FlowgenNatsJetStreamPublisher)?;

                // if let Some(ref channel) = self.target_channel {
                let receiver_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                    while let Ok(message) = rx.recv().await {
                        match message {
                            ChannelMessage::FileMessage(m) => {
                                let event = m.record_batch.to_bytes().unwrap();
                                let subject = format!("filedrop.in.{}", m.file_chunk);
                                publisher
                                    .jetstream
                                    .send_publish(subject, Publish::build().payload(event.into()))
                                    .await
                                    .map_err(Error::NatsPublish)?;
                                event!(name: "file_processed", Level::INFO, "file processed: {}", m.file_chunk);
                            }
                        }
                    }
                    Ok(())
                });
                let _ = receiver_task.await.map_err(Error::TokioJoin)?;
                // {
                //     tokio::spawn(async move {
                //         let mut rx = rx.lock().await;
                //         print!("{:?}", "before");
                //         match rx.deref_mut() {
                //             Rx::file(rx) => {
                //                 print!("{:?}", "after");
                //                 while let Some(m) = rx.recv().await {
                //                     let event = m.record_batch.to_bytes().unwrap();
                //                     let subject = format!("filedrop.in.{}", m.file_chunk);
                //                     publisher
                //                         .jetstream
                //                         .send_publish(
                //                             subject,
                //                             Publish::build().payload(event.into()),
                //                         )
                //                         .await
                //                         .unwrap();
                //                     event!(name: "file_processed", Level::INFO, "file processed: {}", m.file_chunk);
                //                 }
                //             }
                //             _ => {
                //                 print!("{:?}", "after");
                //             }
                //         }
                //     });
                //     //Run all receiver tasks.
                // }
            } // if let Some(rx) = self.target_receiver.clone() {
              //     let mut rx = rx.lock().await;
              //     match rx.deref_mut() {
              //         Message::file(rx) => {
              //             while let Some(m) = rx.recv().await {
              //                 let event = m.record_batch.to_bytes().unwrap();
              //                 let subject = format!("filedrop.in.{}", m.file_chunk);
              //                 publisher
              //                     .jetstream
              //                     .send_publish(subject, Publish::build().payload(event.into()))
              //                     .await
              //                     .unwrap();
              //                 event!(name: "file_processed", Level::INFO, "file processed: {}", m.file_chunk);
              //             }
              //         }
              //         _ => {
              //             info!("unimplemented")
              //         }
              //     }
              // }

              // self.target = Some(Target::nats_jetstream(publisher));
        }

        // match config.flow.processor {
        //     _ => {
        //         print!("{:?}", "herep");
        //         if let Some(ref target_channel) = self.target_channel {
        //             if let Some(ref source_channel) = self.source_channel {
        //                 let tx = target_channel.tx.clone();
        //                 let rx = source_channel.rx.clone();
        //                 tokio::spawn(async move {
        //                     let mut rx = rx.lock().await;
        //                     match rx.deref_mut() {
        //                         Rx::file(rx) => {
        //                             while let Some(m) = rx.recv().await {
        //                                 match tx.as_ref() {
        //                                     Tx::file(tx) => {
        //                                         print!("{:?}", "m");
        //                                         tx.send(m).await.unwrap();
        //                                     }
        //                                     _ => {}
        //                                 }
        //                             }
        //                         }
        //                         _ => todo!(),
        //                     }
        //                 });

        //                 // Continue executing the rest of the code
        //                 print!("{:?}", "here");
        //                 print!("{:?}", "here1"); // This should print immediately
        //                                          // print!("{:?}", "here");
        //                                          // try_join_all(vec![receiver_task]).await.unwrap();
        //                                          // print!("{:?}", "here1");
        //             }
        //         }
        //     }
        // }

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
            // processor_receiver: None,
            // target_receiver: None,
            // target_channel: None,
            // source_channel: None,
            source: None,
            processor: None,
            target: None,
        };
        Ok(f)
    }
}
