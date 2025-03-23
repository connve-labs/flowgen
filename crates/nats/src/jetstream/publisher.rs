use super::message::FlowgenMessageExt;
use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy};
use flowgen_core::connect::client::Client;
use flowgen_core::stream::event::Event;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::Receiver;
use tracing::{event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to connect to a NATS Server")]
    NatsClientAuth(#[source] crate::client::Error),
    #[error("NATS Client is missing / not initialized properly")]
    MissingNatsClient(),
    #[error("error publish message to NATS Jetstream")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("failed to create or update Nats Jetstream")]
    NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    #[error("failed to get NATS Jetstream")]
    NatsGetStream(#[source] async_nats::jetstream::context::GetStreamError),
    #[error("failed to get process request to NATS Server")]
    NatsRequest(#[source] async_nats::jetstream::context::RequestError),
    #[error("error with NATS JetStream Event")]
    NatsJetStreamEvent(#[source] super::message::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
}

pub struct Publisher {
    config: Arc<super::config::Target>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::stream::publisher::Publisher for Publisher {
    type Error = Error;
    async fn publish(mut self) -> Result<(), Self::Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.clone().into())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        if let Some(jetstream) = client.jetstream {
            let mut max_age = 86400;
            if let Some(config_max_age) = self.config.max_age {
                max_age = config_max_age
            }

            let mut stream_config = Config {
                name: self.config.stream.clone(),
                description: self.config.stream_description.clone(),
                max_messages_per_subject: 1,
                subjects: self.config.subjects.clone(),
                discard: DiscardPolicy::Old,
                retention: RetentionPolicy::Limits,
                max_age: Duration::new(max_age, 0),
                ..Default::default()
            };

            let stream = jetstream.get_stream(self.config.stream.clone()).await;

            match stream {
                Ok(_) => {
                    let mut subjects = stream
                        .map_err(Error::NatsGetStream)?
                        .info()
                        .await
                        .map_err(Error::NatsRequest)?
                        .config
                        .subjects
                        .clone();

                    subjects.extend(self.config.subjects.clone());
                    subjects.sort();
                    subjects.dedup();
                    stream_config.subjects = subjects;

                    jetstream
                        .update_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
                Err(_) => {
                    jetstream
                        .create_stream(stream_config)
                        .await
                        .map_err(Error::NatsCreateStream)?;
                }
            }

            while let Ok(e) = self.rx.recv().await {
                if e.current_task_id == Some(self.current_task_id - 1) {
                    let event = e.to_publish().map_err(Error::NatsJetStreamEvent)?;

                    jetstream
                        .send_publish(e.subject.clone(), event)
                        .await
                        .map_err(Error::NatsPublish)?;

                    event!(Level::INFO, "event published: {}", e.subject);
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Target>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Target>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
