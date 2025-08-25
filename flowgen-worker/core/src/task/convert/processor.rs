use super::super::super::event::{Event, EventBuilder, EventData};
use crate::event::AvroData;
use chrono::Utc;
use serde_avro_fast::ser;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "convert";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] super::super::super::event::Error),
    #[error(transparent)]
    SerdeAvro(#[from] serde_avro_fast::ser::SerError),
    #[error(transparent)]
    SerdeSchema(#[from] serde_avro_fast::schema::SchemaError),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

fn transform_keys(value: &mut Value) {
    if let Value::Object(map) = value {
        let mut new_map = Map::new();

        // Collect keys that need to be renamed
        let keys_to_rename: Vec<String> = map.keys().filter(|k| k.contains("-")).cloned().collect();

        // Remove old keys and insert with new names
        for old_key in keys_to_rename {
            if let Some(val) = map.remove(&old_key) {
                let new_key = old_key.replace("-", "_");
                new_map.insert(new_key, val);
            }
        }

        // Add the new keys to the original map
        for (key, val) in new_map {
            map.insert(key, val);
        }
    }
}

/// Handles processing of https call outs.
struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events
    tx: Sender<Event>,
    /// Task identifier for event tracking
    current_task_id: usize,
    serializer: Option<Arc<AvroSerializerOptions>>,
}

struct AvroSerializerOptions {
    schema_string: String,
    serializer_config: Mutex<ser::SerializerConfig<'static>>,
}

impl EventHandler {
    /// Processes an event and converts to selected target format.
    async fn handle(self, event: Event) -> Result<(), Error> {
        let data = match event.data {
            EventData::Json(mut data) => match &self.serializer {
                Some(serializer_opts) => {
                    transform_keys(&mut data);

                    let mut serializer_config = serializer_opts.serializer_config.lock().await;
                    let raw_bytes: Vec<u8> =
                        serde_avro_fast::to_datum_vec(&data, &mut serializer_config)?;

                    EventData::Avro(AvroData {
                        schema: serializer_opts.schema_string.clone(),
                        raw_bytes,
                    })
                }
                None => EventData::Json(data),
            },
            // Conversion to other types are currently not supported and not configurable.
            _ => todo!(),
        };

        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!("{}.{}", label.to_lowercase(), timestamp),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Send processor output as event.
        let e = EventBuilder::new()
            .data(data)
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;

        self.tx.send(e)?;
        event!(Level::INFO, "Event processed: {}", subject);
        Ok(())
    }
}

pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl super::super::runner::Runner for Processor {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        let serializer = match self.config.target_format {
            super::config::TargetFormat::Avro => {
                let schema_string = self
                    .config
                    .as_ref()
                    .schema
                    .clone()
                    .ok_or_else(|| Error::MissingRequiredAttribute("schema".to_string()))?;

                let schema: serde_avro_fast::Schema = schema_string.parse()?;

                // Leak the schema to get a 'static reference
                // This is intentional and safe in this context since the schema
                // is effectively program-lifetime data.
                let leaked_schema: &'static serde_avro_fast::Schema = Box::leak(Box::new(schema));

                let serializer_config = ser::SerializerConfig::new(leaked_schema);

                Some(Arc::new(AvroSerializerOptions {
                    schema_string,
                    serializer_config: Mutex::new(serializer_config),
                }))
            }
        };

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let config = Arc::clone(&self.config);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;
                let serializer = serializer.clone();

                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                    serializer,
                };

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

#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Processor>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
