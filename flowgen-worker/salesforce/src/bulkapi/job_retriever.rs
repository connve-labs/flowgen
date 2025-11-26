use arrow::csv::reader::Format;
use flowgen_core::event::{Event, EventBuilder, EventData, SenderExt};
use oauth2::TokenResponse;
use serde::Deserialize;
use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{error, Instrument};

/// Message subject prefix for bulk API retrieve operations.
const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce_query_job_retrieve";

/// Errors for Salesforce bulk job retrieval operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO operation failed: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to send event message: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("HTTP request failed: {source}")]
    Reqwest {
        #[source]
        source: reqwest::Error,
    },
    #[error(transparent)]
    SalesforceAuth(#[from] salesforce_core::client::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error("missing salesforce access token")]
    NoSalesforceAuthToken(),
    #[error("Arrow data processing failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("missing salesforce instance URL")]
    NoSalesforceInstanceURL(),
    #[error("JSON serialization/deserialization failed: {source}")]
    ParseSchema {
        #[source]
        source: apache_avro::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Salesforce job metadata API response.
#[derive(Debug, Deserialize)]
struct JobResponse {
    /// Salesforce object type (e.g., "Account", "Contact").
    object: String,
}

/// Processor for retrieving Salesforce bulk job results.
pub struct JobRetriever {
    config: Arc<super::config::JobRetriever>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    task_id: usize,
    task_type: &'static str,
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Event handler for processing individual job retrieval requests.
pub struct EventHandler {
    /// HTTP client for Salesforce API requests.
    client: Arc<reqwest::Client>,
    /// Channel sender for emitting processed data.
    tx: Sender<Event>,
    /// Task identifier for event correlation.
    current_task_id: usize,
    /// Processor configuration.
    config: Arc<super::config::JobRetriever>,
    /// SFDC client.
    sfdc_client: salesforce_core::client::Client,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes job retrieval: extract job info, download CSV, convert to Arrow, emit events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        // Process only Avro events with job completion data.
        if let EventData::Json(value) = &event.data {
            let instance_url = self
                .sfdc_client
                .instance_url
                .clone()
                .ok_or_else(Error::NoSalesforceInstanceURL)?;

            // Create HTTP request to download CSV results.
            let mut client = self.client.get(format!(
                "{}{}",
                instance_url,
                value.get("ResultUrl").unwrap()
            ));

            let token_result = self
                .sfdc_client
                .token_result
                .clone()
                .ok_or_else(Error::NoSalesforceAuthToken)?;

            client = client.bearer_auth(token_result.access_token().secret());

            // Download CSV result data.
            let resp = client
                .send()
                .await
                .map_err(|e| Error::Reqwest { source: e })?
                .bytes()
                .await
                .map_err(|e| Error::Reqwest { source: e })?;

            // Write CSV to temporary file.
            let file_path = "output.csv";
            let mut file = File::create(file_path).unwrap();
            file.write_all(&resp).map_err(|e| Error::IO { source: e })?;

            // Reopen file for reading and schema inference.
            let mut file = File::open(file_path).map_err(|e| Error::IO { source: e })?;

            // Infer CSV schema from first 100 rows.
            let (schema, _) = Format::default()
                .with_header(true)
                .infer_schema(&file, Some(100))
                .map_err(|e| Error::Arrow { source: e })?;

            // Reset file pointer to beginning.
            file.rewind().map_err(|e| Error::IO { source: e })?;

            // Create Arrow CSV reader.
            let csv = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(true)
                .with_batch_size(100)
                .build(&file)
                .map_err(|e| Error::Arrow { source: e })?;

            // Request job metadata to get object type.
            let mut client = self.client.get(format!(
                "{}{}{}{}",
                instance_url,
                crate::bulkapi::config::DEFAULT_URI_PATH,
                self.config.job_type.as_str().to_owned() + "/",
                value.get("job_id").unwrap()
            ));

            client = client.bearer_auth(token_result.access_token().secret());

            // Retrieve job metadata.
            let resp = client
                .send()
                .await
                .map_err(|e| Error::Reqwest { source: e })?
                .text()
                .await
                .map_err(|e| Error::Reqwest { source: e })?;

            let job_metadata: JobResponse =
                serde_json::from_str(&resp).map_err(|e| Error::SerdeJson { source: e })?;

            // Process each Arrow record batch and emit as events.
            for data in csv {
                let e = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(
                        data.map_err(|e| Error::Arrow { source: e })?,
                    ))
                    .subject(job_metadata.object.to_lowercase())
                    .task_id(self.current_task_id)
                    .task_type(self.task_type)
                    .build()?;
                self.tx
                    .send_with_logging(e)
                    .map_err(|e| Error::SendMessage { source: e })?;
            }
        }
        Ok(())
    }
}

/// Builder for constructing JobRetriever instances.
#[derive(Default)]
pub struct JobRetrieverBuilder {
    config: Option<Arc<super::config::JobRetriever>>,
    tx: Option<Sender<Event>>,
    rx: Option<Receiver<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for JobRetriever {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes HTTPS client and creates event handler.
    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self.config.as_ref();
        // Initialize secure HTTP client (HTTPS only).
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client,
            sfdc_client,
            task_type: self.task_type,
        };
        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields( task_id = self.task_id))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        // Initialize runner task.
        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        // Process incoming events, filtering by task ID.
        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

impl JobRetrieverBuilder {
    /// Creates a new ProcessorBuilder with defaults.
    pub fn new() -> JobRetrieverBuilder {
        JobRetrieverBuilder {
            ..Default::default()
        }
    }

    /// Sets the job retriever configuration.
    pub fn config(mut self, config: Arc<super::config::JobRetriever>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the task identifier.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Sets the task context.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Builds JobRetriever after validating required fields.
    pub async fn build(self) -> Result<JobRetriever, Error> {
        Ok(JobRetriever {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}
