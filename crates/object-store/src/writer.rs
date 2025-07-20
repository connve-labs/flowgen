use apache_avro::from_avro_datum;
use bytes::Bytes;
use chrono::Utc;
use flowgen_core::connect::client::Client;
use flowgen_core::stream::event::Event;
use object_store::PutPayload;
use std::{fs::File, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

/// Default subject prefix for logging messages.
const DEFAULT_MESSAGE_SUBJECT: &str = "object_store.writer";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStoreClient(#[from] super::client::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("could not initialize object store context")]
    NoObjectStoreContext(),
}

/// Handles processing of individual events by writing them to object storage.
struct EventHandler {
    client: Arc<Mutex<super::client::Client>>,
}

impl EventHandler {
    /// Processes an event and writes it to the configured object store.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let timestamp = Utc::now().timestamp_micros();
        let filename = match event.id {
            Some(id) => id,
            None => {
                format!("{timestamp}")
            }
        };

        let mut client_guard = self.client.lock().await;
        let context = client_guard
            .context
            .as_mut()
            .ok_or_else(Error::NoObjectStoreContext)?;

        match &event.data {
            flowgen_core::stream::event::EventData::ArrowRecordBatch(data) => {
                let file = File::create(&filename).map_err(Error::IO)?;
                arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(file)
                    .write(data)
                    .map_err(Error::Arrow)?;
            }
            flowgen_core::stream::event::EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema).map_err(Error::Avro)?;
                let value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(Error::Avro)?;

                let mut buffer = Vec::new();
                {
                    let mut writer = apache_avro::Writer::new(&schema, &mut buffer);
                    writer.append(value).map_err(Error::Avro)?;
                    writer.flush().map_err(Error::Avro)?;
                }
                let date = format!("{}", Utc::now().format("%Y-%m-%d"));
                let path = object_store::path::Path::from(format!(
                    "{}/{}/{}.avro",
                    context.path, date, filename
                ));
                let payload = PutPayload::from_bytes(Bytes::from(buffer));
                context
                    .object_store
                    .put(&path, payload)
                    .await
                    .map_err(Error::ObjectStore)?;
            }
        }

        let subject = format!("{DEFAULT_MESSAGE_SUBJECT}.{filename}");
        event!(Level::INFO, "event processed: {}", subject);
        Ok(())
    }
}

/// Object store writer that processes events from a broadcast receiver.
pub struct Writer {
    config: Arc<super::config::Writer>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;

    async fn run(mut self) -> Result<(), Self::Error> {
        let mut client_builder = super::client::ClientBuilder::new().path(self.config.path.clone());

        if let Some(options) = &self.config.options {
            client_builder = client_builder.options(options.clone());
        }
        if let Some(credentials) = &self.config.credentials {
            client_builder = client_builder.credentials(credentials.to_path_buf());
        }

        let client = Arc::new(Mutex::new(
            client_builder
                .build()
                .map_err(Error::ObjectStoreClient)?
                .connect()
                .await
                .map_err(Error::ObjectStoreClient)?,
        ));

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let client = Arc::clone(&client);

                let event_handler = EventHandler { client };
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

/// Builder pattern for constructing Writer instances.
#[derive(Default)]
pub struct WriterBuilder {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
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

    pub async fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
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
