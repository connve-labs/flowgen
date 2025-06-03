use async_nats::jetstream;
use chrono::Utc;
use flowgen_core::{connect::client::Client as FlowgenClientTrait, stream::event::Event};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, event, Level};

/// Base subject used for logging successful event processing messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "deltalake.writer";
/// Default alias for the target DeltaTable in MERGE operations.
const DEFAULT_TARGET_ALIAS: &str = "target";
/// Default alias for the source data (in-memory table) in MERGE operations.
const DEFAULT_SOURCE_ALIAS: &str = "source";

/// Errors that can occur during the Delta Lake writing process.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error occurring when joining Tokio tasks.
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error("failed to connect to a NATS Server")]
    NatsClientAuth(#[source] crate::client::Error),
    /// Error originating from the Delta Lake client setup or connection (`super::client`).
    #[error(transparent)]
    Client(#[from] super::client::Error),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attrubute")]
    // Note: "attrubute" typo exists in original code
    MissingRequiredAttribute(String),
    /// The required `path` configuration for the Delta table was not provided.
    #[error("missing required config value path")]
    MissingPath(),
    /// Internal error: The DeltaTable reference was unexpectedly missing.
    #[error("missing required value DeltaTable")]
    MissingDeltaTable(),
    /// Could not extract a filename from the configured Delta table path.
    #[error("no filename in provided path")]
    EmptyFileName(),
    /// An expected string value was empty (e.g., filename conversion).
    #[error("no value in provided str")]
    EmptyStr(),
    #[error(transparent)]
    CacheEntry(#[from] async_nats::jetstream::kv::EntryError),
}

#[derive(Debug, Default)]
pub struct Cache {
    credentials_path: PathBuf,
    store: Option<async_nats::jetstream::kv::Store>,
}

impl flowgen_core::cache::Cache for Cache {
    type Error = Error;

    async fn init(mut self, bucket: &str) -> Result<Self, Self::Error> {
        let client = crate::client::ClientBuilder::new()
            .credentials_path(self.credentials_path.clone())
            .build()
            .map_err(Error::NatsClientAuth)?
            .connect()
            .await
            .map_err(Error::NatsClientAuth)?;

        let jetstream = client.jetstream.unwrap();
        let store = match jetstream.get_key_value(bucket).await {
            Ok(store) => store,
            Err(_) => jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    history: 10,
                    ..Default::default()
                })
                .await
                .unwrap(),
        };

        self.store = Some(store);
        Ok(self)
    }
    async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), Self::Error> {
        if let Some(store) = &self.store {
            store.put(key, value).await.unwrap();
        }
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<bytes::Bytes, Self::Error> {
        let store = self.store.as_ref().ok_or(Error::MissingDeltaTable())?;
        let bytes = store
            .get(key)
            .await
            .map_err(Error::CacheEntry)?
            .ok_or(Error::MissingDeltaTable())?;
        Ok(bytes)
    }
}

#[derive(Default)]
pub struct CacheBuilder {
    credentials_path: Option<PathBuf>,
}

impl CacheBuilder {
    /// Creates a new `CacheBuilder` with default values.
    pub fn new() -> CacheBuilder {
        CacheBuilder {
            ..Default::default()
        }
    }

    /// Sets the broadcast channel receiver for incoming events.
    ///
    /// # Arguments
    /// * `receiver` - The `Receiver<Event>` end of the broadcast channel.
    pub fn credentials_path(mut self, credentials_path: PathBuf) -> Self {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Builds the `Cache` instance.
    ///
    /// Consumes the builder and returns a `Writer` if all required fields (`config`, `rx`)
    /// have been set.
    ///
    /// # Returns
    /// * `Ok(Writer)` if construction is successful.
    /// * `Err(Error::MissingRequiredAttribute)` if `config` or `rx` was not provided.
    pub fn build(self) -> Result<Cache, Error> {
        Ok(Cache {
            credentials_path: self
                .credentials_path
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
            store: None,
        })
    }
}
