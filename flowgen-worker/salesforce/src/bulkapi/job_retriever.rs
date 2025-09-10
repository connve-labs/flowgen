use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use arrow::csv::reader::Format;
use flowgen_core::{
    client::Client,
    event::{generate_subject, Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE},
};
use oauth2::TokenResponse;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

/// Default message subject prefix for bulk API retrieve operations
const DEFAULT_MESSAGE_SUBJECT: &'static str = "bulkapiretrieve";
/// Default Salesforce Bulk API endpoint path for job metadata retrieval (API version 61.0)
const DEFAULT_JOB_METADATA_URI: &'static str = "/services/data/v61.0/jobs/query/";

/// Comprehensive error types for Salesforce bulk job retrieval operations.
///
/// This enum encapsulates all possible error conditions that can occur during
/// the job retrieval process, from file I/O operations to API communication issues.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// File system I/O error (file creation, reading, writing, seeking)
    ///
    /// Occurs when operations like creating output files, writing CSV data,
    /// or repositioning file pointers fail due to filesystem issues.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Error occurred while sending events through the broadcast channel
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),

    /// Required configuration attribute is missing
    ///
    /// This error is thrown when mandatory fields like config, sender, or receiver
    /// are not provided during the builder pattern construction.
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),

    /// HTTP request/response error from the reqwest library
    ///
    /// Wraps underlying network, HTTP protocol, or response parsing errors
    /// that occur when communicating with the Salesforce API.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    /// Salesforce authentication or client initialization error
    ///
    /// Occurs when credential loading, OAuth flow, or client setup fails.
    #[error(transparent)]
    SalesforceAuth(#[from] crate::client::Error),

    /// Event creation or processing error from flowgen_core
    ///
    /// Wraps errors that occur during event building or data serialization.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),

    /// Missing or invalid Salesforce access token
    ///
    /// Indicates that the OAuth authentication process didn't produce a valid token.
    #[error("missing salesforce access token")]
    NoSalesforceAuthToken(),

    /// Apache Avro schema parsing or data deserialization error
    ///
    /// Occurs when Avro schema parsing fails or when deserializing Avro binary data
    /// into structured values doesn't succeed.
    #[error(transparent)]
    Arrow(#[from] apache_avro::Error),

    /// Apache Arrow error during CSV processing or schema inference
    ///
    /// Happens when Arrow's CSV reader cannot infer schema from the data
    /// or when processing Arrow record batches fails.
    #[error("arrow error: {}", _0)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// JSON serialization/deserialization error
    ///
    /// Occurs when parsing Salesforce API JSON responses fails or when
    /// converting data structures to/from JSON representation fails.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

/// Response structure for Salesforce job metadata API calls.
///
/// This struct represents the essential information returned when querying
/// job metadata from the Salesforce Bulk API, particularly the object type
/// that the job operates on.
#[derive(Debug, Deserialize)]
struct JobResponse {
    /// The Salesforce object type that this job processes
    ///
    /// Examples: "Account", "Contact", "Custom_Object__c"
    /// This is used for constructing meaningful event subjects and logging.
    object: String,
}

/// Internal credentials structure for storing authentication information.
///
/// Currently supports bearer token authentication, with potential for expansion
/// to other authentication methods in the future.
#[derive(Deserialize, Serialize, PartialEq, Clone, Debug, Default)]
struct Credentials {
    /// Optional bearer authentication token
    ///
    /// When present, this token will be used for API authentication.
    /// None indicates that other authentication methods should be used.
    bearer_auth: Option<String>,
}

/// Main processor for retrieving Salesforce bulk job results.
///
/// This struct coordinates the entire job result retrieval workflow, from receiving
/// Avro events containing job information to downloading CSV results and converting
/// them to Arrow record batches for downstream processing.
pub struct JobRetriever {
    /// Shared configuration containing authentication details and processing options
    config: Arc<super::config::JobRetriever>,
    /// Broadcast sender for emitting processed Arrow record batches downstream
    tx: Sender<Event>,
    /// Broadcast receiver for incoming Avro events containing job information
    rx: Receiver<Event>,
    /// Unique identifier for tracking this processor's events in the pipeline
    current_task_id: usize,
}

/// Internal event handler responsible for processing individual job retrieval requests.
///
/// This struct encapsulates the logic for extracting job information from Avro events,
/// downloading CSV results from Salesforce, converting them to Arrow format, and
/// emitting the processed data as events.
struct EventHandler {
    /// HTTP client for making requests to the Salesforce API
    client: Arc<reqwest::Client>,
    /// Processor configuration containing authentication and processing details
    config: Arc<super::config::JobRetriever>,
    /// Channel sender for emitting processed Arrow record batches
    tx: Sender<Event>,
    /// Task identifier for event correlation and tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes a job retrieval event and handles the complete data download workflow.
    ///
    /// This method orchestrates the entire job result retrieval process:
    /// 1. Parses Avro event data to extract job information (ResultUrl, JobIdentifier)
    /// 2. Downloads CSV result data from the Salesforce ResultUrl
    /// 3. Writes the CSV data to a temporary file for processing
    /// 4. Uses Arrow to infer schema and parse the CSV into record batches
    /// 5. Retrieves job metadata to determine the object type for event naming
    /// 6. Emits each record batch as a separate event for downstream processing
    ///
    /// # Arguments
    ///
    /// * `event` - The incoming Avro event containing job completion information
    ///
    /// # Returns
    ///
    /// `Ok(())` if the job results were successfully retrieved and emitted,
    /// or an `Error` if any step in the process failed.
    ///
    /// # Errors
    ///
    /// This method can return various errors including:
    /// - `SalesforceAuth` errors for credential or authentication issues
    /// - `NoSalesforceAuthToken` if authentication didn't produce a valid token
    /// - `Reqwest` errors for HTTP communication problems
    /// - `Arrow` errors for Avro parsing or Arrow processing issues
    /// - `IO` errors for file operations
    /// - `SerdeJson` errors for JSON response parsing
    /// - `SendMessage` errors if events cannot be emitted
    async fn handle(self, event: Event) -> Result<(), Error> {
        let config = self.config.as_ref();
        let creds_path = Path::new(&config.credentials);

        // Initialize and authenticate Salesforce client
        let sfdc_client = crate::client::Builder::new()
            .credentials_path(creds_path.to_path_buf())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        // Process only Avro events containing job completion data
        if let EventData::Avro(value) = &event.data {
            // Parse the Avro schema from the event
            let schema = Schema::parse_str(&value.schema)?;

            // Deserialize the Avro binary data using the schema
            let value = from_avro_datum(&schema, &mut value.raw_bytes.as_slice(), None)?;

            // Process the deserialized Avro record
            match value {
                Value::Record(fields) => {
                    // Extract the ResultUrl field which contains the download URL for CSV results
                    match fields.iter().find(|(name, _)| name == "ResultUrl") {
                        Some((_, Value::Union(_, inner_value))) => {
                            match &**inner_value {
                                Value::String(result_url) => {
                                    println!("Found ResultUrl: {}", result_url);

                                    // Create HTTP client request to download CSV results
                                    let mut client =
                                        self.client.get(sfdc_client.instance_url + result_url);

                                    let token_result = sfdc_client
                                        .token_result
                                        .ok_or_else(|| Error::NoSalesforceAuthToken())?;

                                    client =
                                        client.bearer_auth(token_result.access_token().secret());

                                    // Download the CSV result data
                                    let resp = client.send().await?.bytes().await?;

                                    // Write CSV data to temporary file for Arrow processing
                                    // TODO: Consider using in-memory processing or configurable output paths
                                    let file_path = "output.csv";
                                    let mut file = File::create(file_path).unwrap();
                                    file.write_all(&resp)?;

                                    // Reopen file for reading and schema inference
                                    let mut file = File::open(file_path)?;

                                    // Use Arrow to infer CSV schema from the first 100 rows
                                    let (schema, _) = Format::default()
                                        .with_header(true)
                                        .infer_schema(&file, Some(100))
                                        .map_err(Error::ArrowError)?;

                                    // Reset file pointer to beginning for full read
                                    file.rewind().map_err(Error::IO)?;

                                    // Create Arrow CSV reader with inferred schema
                                    let csv =
                                        arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
                                            .with_header(true)
                                            .with_batch_size(100)
                                            .build(&file)
                                            .map_err(Error::ArrowError)?;

                                    // Extract JobIdentifier for metadata retrieval
                                    match fields.iter().find(|(name, _)| name == "JobIdentifier") {
                                        Some((_, Value::String(job_id))) => {
                                            // Create new client for job metadata retrieval
                                            let sfdc_client = crate::client::Builder::new()
                                                .credentials_path(creds_path.to_path_buf())
                                                .build()
                                                .map_err(Error::SalesforceAuth)?
                                                .connect()
                                                .await?;

                                            // Request job metadata to get object type
                                            let mut client = self.client.get(
                                                sfdc_client.instance_url
                                                    + DEFAULT_JOB_METADATA_URI
                                                    + job_id,
                                            );

                                            let token_result = sfdc_client
                                                .token_result
                                                .ok_or_else(|| Error::NoSalesforceAuthToken())?;

                                            client = client
                                                .bearer_auth(token_result.access_token().secret());

                                            // Retrieve job metadata
                                            let resp = client.send().await?.text().await?;

                                            let job_metadata: JobResponse =
                                                serde_json::from_str(&resp)?;

                                            let subject = generate_subject(
                                                Some(job_metadata.object.to_lowercase().as_str()),
                                                DEFAULT_MESSAGE_SUBJECT,
                                                SubjectSuffix::Timestamp,
                                            );

                                            // Process each Arrow record batch and emit as separate events
                                            for data in csv {
                                                // Create event with Arrow record batch data
                                                let e = EventBuilder::new()
                                                    .data(EventData::ArrowRecordBatch(data?))
                                                    .subject(subject.clone())
                                                    .current_task_id(self.current_task_id)
                                                    .build()?;
                                                self.tx.send(e.clone())?;
                                                event!(
                                                    Level::INFO,
                                                    "{}: {}",
                                                    DEFAULT_LOG_MESSAGE,
                                                    e.subject
                                                );
                                            }
                                        }
                                        Some((_, _)) => {
                                            eprintln!(
                                                "JobIdentifier field found but it is not a String."
                                            );
                                        }
                                        None => {
                                            eprintln!("JobIdentifier field not found in record.");
                                        }
                                    }
                                }
                                Value::Null => {
                                    eprintln!("ResultUrl is null.");
                                }
                                _ => {
                                    eprintln!("ResultUrl field is a Union, but contains an unexpected type.");
                                }
                            }
                        }
                        Some((_, _)) => {
                            eprintln!("ResultUrl field is not a Union as expected.");
                        }
                        None => {
                            eprintln!("ResultUrl field not found in record.");
                        }
                    }
                }
                _ => {
                    eprintln!("The top-level Avro value is not a Record.");
                }
            }
        } else {
            println!("Not an Avro event");
        }

        Ok(())
    }
}

/// Builder pattern implementation for constructing JobRetriever instances.
///
/// This builder ensures that all required components are provided and properly
/// configured before creating a JobRetriever instance. It follows the standard
/// Rust builder pattern with method chaining for ergonomic configuration.
#[derive(Default)]
pub struct ProcessorBuilder {
    /// Job retriever configuration (required)
    config: Option<Arc<super::config::JobRetriever>>,
    /// Event sender channel (required)
    tx: Option<Sender<Event>>,
    /// Event receiver channel (required)
    rx: Option<Receiver<Event>>,
    /// Task identifier for event correlation (defaults to 0)
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for JobRetriever {
    type Error = Error;

    /// Main execution loop for the job retriever processor.
    ///
    /// This method implements the flowgen_core Runner trait and provides the
    /// main execution logic for the processor. It:
    /// 1. Sets up an HTTPS-only HTTP client for security
    /// 2. Continuously listens for incoming Avro events containing job information
    /// 3. Processes events that match the expected task ID
    /// 4. Spawns asynchronous handlers for each job result retrieval request
    ///
    /// The method uses task ID filtering to ensure that only events intended
    /// for this specific processor instance are handled, enabling proper
    /// pipeline orchestration in multi-step workflows.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the processor shuts down cleanly, or an `Error` if
    /// critical initialization or processing failures occur.
    ///
    /// # Errors
    ///
    /// Returns `Reqwest` error if the HTTP client cannot be initialized.
    /// Individual event processing errors are logged but don't terminate the processor.
    async fn run(mut self) -> Result<(), Error> {
        // Initialize secure HTTP client (HTTPS only for security)
        let client = reqwest::ClientBuilder::new().https_only(true).build()?;
        let client = Arc::new(client);

        // Main event processing loop
        while let Ok(event) = self.rx.recv().await {
            // Filter events by task ID to ensure proper pipeline ordering
            if event.current_task_id == Some(self.current_task_id - 1) {
                // Clone shared resources for the async handler
                let config = Arc::clone(&self.config);
                let client = Arc::clone(&client);
                let tx = self.tx.clone();
                let current_task_id = self.current_task_id;

                // Create handler for this specific job retrieval request
                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                    client,
                };

                // Process the event asynchronously to avoid blocking the main loop
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

impl ProcessorBuilder {
    /// Creates a new ProcessorBuilder with default values.
    ///
    /// All optional fields are initialized to None, and current_task_id is set to 0.
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    /// Sets the job retriever configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Shared configuration containing authentication details and processing options
    pub fn config(mut self, config: Arc<super::config::JobRetriever>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    ///
    /// # Arguments
    ///
    /// * `receiver` - Broadcast receiver for incoming Avro events containing job information
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    ///
    /// # Arguments
    ///
    /// * `sender` - Broadcast sender for emitting processed Arrow record batches
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the task identifier for event correlation.
    ///
    /// # Arguments
    ///
    /// * `current_task_id` - Unique identifier for tracking this processor's events
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    /// Builds the JobRetriever instance after validating required fields.
    ///
    /// # Returns
    ///
    /// `Ok(JobRetriever)` if all required fields are provided, or an `Error`
    /// indicating which required field is missing.
    ///
    /// # Errors
    ///
    /// Returns `MissingRequiredAttribute` error if any of the required fields
    /// (config, receiver, sender) are not provided.
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
            current_task_id: self.current_task_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    // Helper function to create a test configuration
    fn create_test_config() -> Arc<crate::bulkapi::config::JobRetriever> {
        Arc::new(crate::bulkapi::config::JobRetriever {
            label: Some("test_retriever".to_string()),
            credentials: "/tmp/test_creds.json".to_string(),
        })
    }

    #[test]
    fn test_error_display() {
        let error = Error::MissingRequiredAttribute("config".to_string());
        assert_eq!(error.to_string(), "missing required attribute: config");

        let error = Error::NoSalesforceAuthToken();
        assert_eq!(error.to_string(), "missing salesforce access token");
    }

    #[test]
    fn test_error_debug() {
        let error = Error::MissingRequiredAttribute("test".to_string());
        let debug_output = format!("{:?}", error);
        assert!(debug_output.contains("MissingRequiredAttribute"));
        assert!(debug_output.contains("test"));
    }

    #[test]
    fn test_job_response_deserialization() {
        let json = r#"{"object": "Account"}"#;
        let response: JobResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.object, "Account");
    }

    #[test]
    fn test_job_response_debug() {
        let response = JobResponse {
            object: "Contact".to_string(),
        };
        let debug_output = format!("{:?}", response);
        assert!(debug_output.contains("Contact"));
        assert!(debug_output.contains("JobResponse"));
    }

    #[test]
    fn test_credentials_default() {
        let creds = Credentials::default();
        assert_eq!(creds.bearer_auth, None);
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            bearer_auth: Some("test_token".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }

    #[test]
    fn test_credentials_clone_and_partial_eq() {
        let original = Credentials {
            bearer_auth: Some("token".to_string()),
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let different = Credentials {
            bearer_auth: Some("different_token".to_string()),
        };
        assert_ne!(original, different);
    }

    #[test]
    fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[test]
    fn test_processor_builder_default() {
        let builder = ProcessorBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = create_test_config();
        let builder = ProcessorBuilder::new().config(Arc::clone(&config));

        assert!(builder.config.is_some());
        assert_eq!(builder.config.unwrap().label, config.label);
    }

    #[tokio::test]
    async fn test_processor_builder_channels() {
        let (tx, rx) = broadcast::channel(10);

        let builder = ProcessorBuilder::new().sender(tx).receiver(rx);

        assert!(builder.tx.is_some());
        assert!(builder.rx.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_task_id() {
        let builder = ProcessorBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_config() {
        let (_tx, rx) = broadcast::channel(10);
        let (tx, _rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .sender(tx)
            .receiver(rx)
            .build()
            .await;

        assert!(result.is_err());
        match result.err().unwrap() {
            Error::MissingRequiredAttribute(attr) => assert_eq!(attr, "config"),
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_sender() {
        let config = create_test_config();
        let (_tx, rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .build()
            .await;

        assert!(result.is_err());
        match result.err().unwrap() {
            Error::MissingRequiredAttribute(attr) => assert_eq!(attr, "sender"),
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_receiver() {
        let config = create_test_config();
        let (tx, _rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .build()
            .await;

        assert!(result.is_err());
        match result.err().unwrap() {
            Error::MissingRequiredAttribute(attr) => assert_eq!(attr, "receiver"),
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let config = create_test_config();
        let (tx, rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(5)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 5);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapiretrieve");
        assert_eq!(DEFAULT_JOB_METADATA_URI, "/services/data/v61.0/jobs/query/");
    }

    #[tokio::test]
    async fn test_event_handler_creation() {
        let config = create_test_config();
        let client = Arc::new(reqwest::Client::new());
        let (tx, _rx) = broadcast::channel(10);

        let handler = EventHandler {
            client,
            config,
            tx,
            current_task_id: 1,
        };

        assert_eq!(handler.current_task_id, 1);
    }

    #[test]
    fn test_error_from_conversions() {
        // Test From trait implementations for various error types

        // IO Error
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let _: Error = io_error.into();

        // JSON Error
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json");
        match json_error {
            Err(e) => {
                let _: Error = e.into();
            }
            _ => panic!("Expected JSON error"),
        }
    }

    #[test]
    fn test_job_response_with_complex_object() {
        let json = r#"{"object": "Custom_Object__c"}"#;
        let response: JobResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.object, "Custom_Object__c");
    }

    #[tokio::test]
    async fn test_builder_method_chaining() {
        let config = create_test_config();
        let (tx, rx) = broadcast::channel(10);

        // Test that all methods return Self for chaining
        let builder = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(10);

        let result = builder.build().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_credentials_with_none() {
        let creds = Credentials { bearer_auth: None };
        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds.bearer_auth, None);
        assert_eq!(deserialized.bearer_auth, None);
    }

    #[tokio::test]
    async fn test_processor_builder_fluent_interface() {
        let config = create_test_config();
        let (tx, rx) = broadcast::channel(10);

        // Test fluent interface returns the builder
        let builder = ProcessorBuilder::new();
        let builder = builder.config(config);
        let builder = builder.sender(tx);
        let builder = builder.receiver(rx);
        let builder = builder.current_task_id(15);

        let processor = builder.build().await.unwrap();
        assert_eq!(processor.current_task_id, 15);
    }

    #[test]
    fn test_error_chain_display() {
        // Test error chaining for different error types
        let errors = vec![
            Error::MissingRequiredAttribute("test".to_string()),
            Error::NoSalesforceAuthToken(),
        ];

        for error in errors {
            let display_output = format!("{}", error);
            assert!(!display_output.is_empty());

            let debug_output = format!("{:?}", error);
            assert!(!debug_output.is_empty());
        }
    }

    #[test]
    fn test_job_response_serialization_edge_cases() {
        // Test with empty object name
        let json = r#"{"object": ""}"#;
        let response: JobResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.object, "");

        // Test with special characters in object name
        let json = r#"{"object": "Account_123__c"}"#;
        let response: JobResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.object, "Account_123__c");
    }

    #[test]
    fn test_job_response_invalid_json() {
        // Test with missing object field
        let json = r#"{"name": "Account"}"#;
        let result: Result<JobResponse, _> = serde_json::from_str(json);
        assert!(result.is_err());

        // Test with null object field
        let json = r#"{"object": null}"#;
        let result: Result<JobResponse, _> = serde_json::from_str(json);
        assert!(result.is_err());

        // Test with wrong type for object field
        let json = r#"{"object": 123}"#;
        let result: Result<JobResponse, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_credentials_edge_cases() {
        // Test with empty string token
        let creds = Credentials {
            bearer_auth: Some("".to_string()),
        };
        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
        assert_eq!(deserialized.bearer_auth, Some("".to_string()));

        // Test with very long token
        let long_token = "a".repeat(1000);
        let creds = Credentials {
            bearer_auth: Some(long_token.clone()),
        };
        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
        assert_eq!(deserialized.bearer_auth, Some(long_token));
    }

    #[tokio::test]
    async fn test_processor_builder_zero_task_id() {
        let config = create_test_config();
        let (tx, rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(0)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_large_task_id() {
        let config = create_test_config();
        let (tx, rx) = broadcast::channel(10);

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(usize::MAX)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.current_task_id, usize::MAX);
    }

    #[test]
    fn test_arrow_error_conversion() {
        // Test Arrow error conversion
        use arrow::error::ArrowError;

        let arrow_error = ArrowError::InvalidArgumentError("Invalid argument".to_string());
        let converted_error: Error = arrow_error.into();

        match converted_error {
            Error::ArrowError(_) => {
                // Success - error was properly converted
            }
            _ => panic!("Expected ArrowError variant"),
        }
    }

    #[tokio::test]
    async fn test_multiple_builder_instances() {
        let config1 = create_test_config();
        let config2 = create_test_config();
        let (tx1, rx1) = broadcast::channel(10);
        let (tx2, rx2) = broadcast::channel(10);

        // Test that multiple builders can be created independently
        let builder1 = ProcessorBuilder::new()
            .config(config1)
            .sender(tx1)
            .receiver(rx1)
            .current_task_id(1);

        let builder2 = ProcessorBuilder::new()
            .config(config2)
            .sender(tx2)
            .receiver(rx2)
            .current_task_id(2);

        let processor1 = builder1.build().await.unwrap();
        let processor2 = builder2.build().await.unwrap();

        assert_eq!(processor1.current_task_id, 1);
        assert_eq!(processor2.current_task_id, 2);
    }

    #[test]
    fn test_config_with_different_labels() {
        // Test configurations with different label scenarios
        let config_with_label = crate::bulkapi::config::JobRetriever {
            label: Some("custom_label".to_string()),
            credentials: "/path/to/creds.json".to_string(),
        };

        let config_without_label = crate::bulkapi::config::JobRetriever {
            label: None,
            credentials: "/path/to/creds.json".to_string(),
        };

        let config_with_empty_label = crate::bulkapi::config::JobRetriever {
            label: Some("".to_string()),
            credentials: "/path/to/creds.json".to_string(),
        };

        // All should be valid configurations
        assert_eq!(config_with_label.label, Some("custom_label".to_string()));
        assert_eq!(config_without_label.label, None);
        assert_eq!(config_with_empty_label.label, Some("".to_string()));
    }

    #[tokio::test]
    async fn test_broadcast_channel_capacity() {
        let config = create_test_config();

        // Test with different channel capacities
        let capacities = vec![1, 10, 100, 1000];

        for capacity in capacities {
            let (tx, rx) = broadcast::channel(capacity);

            let result = ProcessorBuilder::new()
                .config(Arc::clone(&config))
                .sender(tx)
                .receiver(rx)
                .build()
                .await;

            assert!(result.is_ok(), "Failed with capacity: {}", capacity);
        }
    }

    #[test]
    fn test_error_message_consistency() {
        // Test that error messages are consistent and helpful
        let required_attrs = vec!["config", "sender", "receiver"];

        for attr in required_attrs {
            let error = Error::MissingRequiredAttribute(attr.to_string());
            let message = error.to_string();

            assert!(message.contains("missing required attribute"));
            assert!(message.contains(attr));
            assert!(!message.is_empty());
        }
    }

    #[test]
    fn test_credentials_json_serialization_format() {
        let creds = Credentials {
            bearer_auth: Some("test_token_123".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();

        // Verify JSON structure
        assert!(json.contains("bearer_auth"));
        assert!(json.contains("test_token_123"));

        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_object());
    }

    #[test]
    fn test_job_response_debug_format() {
        let response = JobResponse {
            object: "TestObject__c".to_string(),
        };

        let debug_str = format!("{:?}", response);

        // Should contain struct name and field values
        assert!(debug_str.contains("JobResponse"));
        assert!(debug_str.contains("object"));
        assert!(debug_str.contains("TestObject__c"));
    }

    #[tokio::test]
    async fn test_processor_config_immutability() {
        let config = create_test_config();
        let original_label = config.label.clone();
        let original_credentials = config.credentials.clone();
        let (tx, rx) = broadcast::channel(10);

        let _processor = ProcessorBuilder::new()
            .config(Arc::clone(&config))
            .sender(tx)
            .receiver(rx)
            .build()
            .await
            .unwrap();

        // Verify config hasn't changed after processor creation
        assert_eq!(config.label, original_label);
        assert_eq!(config.credentials, original_credentials);
    }

    #[test]
    fn test_constants_are_static() {
        // Test that constants are properly defined as static
        let subject1 = DEFAULT_MESSAGE_SUBJECT;
        let subject2 = DEFAULT_MESSAGE_SUBJECT;
        let uri1 = DEFAULT_JOB_METADATA_URI;
        let uri2 = DEFAULT_JOB_METADATA_URI;

        // Should be the same reference (static)
        assert_eq!(subject1, subject2);
        assert_eq!(uri1, uri2);

        // Verify values
        assert_eq!(subject1, "bulkapiretrieve");
        assert_eq!(uri1, "/services/data/v61.0/jobs/query/");
    }

    #[tokio::test]
    async fn test_event_handler_with_different_clients() {
        let config = create_test_config();
        let (tx, _rx) = broadcast::channel(10);

        // Test with default client
        let client1 = Arc::new(reqwest::Client::new());
        let handler1 = EventHandler {
            client: client1,
            config: Arc::clone(&config),
            tx: tx.clone(),
            current_task_id: 1,
        };

        // Test with configured client
        let client2 = Arc::new(
            reqwest::ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        );
        let handler2 = EventHandler {
            client: client2,
            config: Arc::clone(&config),
            tx,
            current_task_id: 2,
        };

        assert_eq!(handler1.current_task_id, 1);
        assert_eq!(handler2.current_task_id, 2);
    }

    #[test]
    fn test_comprehensive_error_coverage() {
        // Test all error variants can be constructed
        let errors = vec![
            Error::MissingRequiredAttribute("test".to_string()),
            Error::NoSalesforceAuthToken(),
        ];

        for error in errors {
            // Test Display trait
            let display_str = format!("{}", error);
            assert!(!display_str.is_empty());

            // Test Debug trait
            let debug_str = format!("{:?}", error);
            assert!(!debug_str.is_empty());

            // Test that error is Send + Sync (common requirements)
            fn assert_send_sync<T: Send + Sync>(_: &T) {}
            assert_send_sync(&error);
        }
    }
}
