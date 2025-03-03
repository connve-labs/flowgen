use std::time::Duration;

use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy};
use flowgen_core::client::Client;

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
}

pub struct Publisher {
    pub jetstream: async_nats::jetstream::Context,
}

pub struct Builder {
    config: super::config::Target,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(config: super::config::Target) -> Builder {
        Builder { config }
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        // Connect to Nats Server.
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.config.credentials.into())
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

            // Create or update stream according to config.
            let mut stream_config = Config {
                name: self.config.stream.clone(),
                description: self.config.stream_description,
                max_messages_per_subject: 1,
                subjects: self.config.subjects.clone(),
                discard: DiscardPolicy::Old,
                retention: RetentionPolicy::Limits,
                max_age: Duration::new(max_age, 0),
                ..Default::default()
            };

            let stream = jetstream.get_stream(self.config.stream).await;

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

                    subjects.extend(self.config.subjects);
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

            Ok(Publisher { jetstream })
        } else {
            Err(Error::MissingNatsClient())
        }
    }
}
