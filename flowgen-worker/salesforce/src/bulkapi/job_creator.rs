use chrono::Utc;
use flowgen_core::{
    client::Client,
    event::{Event, EventBuilder, EventData},
};
use oauth2::TokenResponse;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{path::Path, sync::Arc};
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::{event, Level};

/// Default message subject prefix for bulk API create operations
const DEFAULT_MESSAGE_SUBJECT: &'static str = "bulkapicreate";
/// Default Salesforce Bulk API endpoint path for query jobs (API version 61.0)
const DEFAULT_URI_PATH: &'static str = "/services/data/v61.0/jobs/query";

/// Comprehensive error types for Salesforce bulk job creation operations.
/// 
/// This enum encapsulates all possible error conditions that can occur during
/// the job creation process, from authentication failures to API communication issues.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error occurred while sending events through the broadcast channel
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    
    /// Required configuration attribute is missing
    /// 
    /// This error is thrown when mandatory fields like config, sender, or receiver
    /// are not provided during the builder pattern construction.
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    
    /// No data available in expected array format
    /// 
    /// Indicates that an operation expected array data but received empty or null data.
    #[error("no array data available")]
    EmptyArray(),
    
    /// HTTP request/response error from the reqwest library
    /// 
    /// Wraps underlying network, HTTP protocol, or response parsing errors.
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

/// Main processor for creating Salesforce bulk API jobs.
/// 
/// This struct coordinates the entire job creation workflow, from receiving
/// trigger events to authenticating with Salesforce and submitting job requests.
/// It implements the flowgen_core Runner trait for integration with the task system.
pub struct JobCreator {
    /// Shared configuration containing job parameters and authentication details
    config: Arc<super::config::JobCreator>,
    /// Broadcast sender for emitting processed events downstream
    tx: Sender<Event>,
    /// Broadcast receiver for incoming trigger events
    rx: Receiver<Event>,
    /// Unique identifier for tracking this processor's events in the pipeline
    current_task_id: usize,
}

/// Internal event handler responsible for processing individual job creation requests.
/// 
/// This struct encapsulates the logic for authenticating with Salesforce,
/// constructing the appropriate API payload, and making the HTTP request to create a bulk job.
struct EventHandler {
    /// HTTP client for making requests to the Salesforce API
    client: Arc<reqwest::Client>,
    /// Processor configuration containing job parameters and credentials
    config: Arc<super::config::JobCreator>,
    /// Channel sender for emitting the job creation response
    tx: Sender<Event>,
    /// Task identifier for event correlation and tracking
    current_task_id: usize,
}

impl EventHandler {
    /// Processes a job creation request and handles the complete workflow.
    /// 
    /// This method orchestrates the entire job creation process:
    /// 1. Loads and validates Salesforce credentials
    /// 2. Authenticates with Salesforce to obtain access token
    /// 3. Constructs the appropriate API payload based on operation type
    /// 4. Makes the HTTP request to create the bulk job
    /// 5. Processes the response and emits it as an event
    /// 
    /// # Returns
    /// 
    /// `Ok(())` if the job was successfully created and the response was emitted,
    /// or an `Error` if any step in the process failed.
    /// 
    /// # Errors
    /// 
    /// This method can return various errors including:
    /// - `SalesforceAuth` errors for credential or authentication issues
    /// - `NoSalesforceAuthToken` if authentication didn't produce a valid token
    /// - `Reqwest` errors for HTTP communication problems
    /// - `Event` errors for response processing issues
    /// - `SendMessage` errors if the response cannot be emitted
    async fn handle(self) -> Result<(), Error> {
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

        // Extract access token from authentication result
        let token_result = sfdc_client
            .token_result
            .ok_or_else(|| Error::NoSalesforceAuthToken())?;

        // Construct API payload based on the requested operation type
        let payload = match self.config.operation {
            super::config::Operation::Query | super::config::Operation::QueryAll => {
                // Create payload for query or queryall job.
                // These operations require a SOQL query and output format specifications.
                json!({
                    "operation": self.config.operation,
                    "query": self.config.query,
                    "contentType": self.config.content_type,
                    "columnDelimiter": self.config.column_delimiter,
                    "lineEnding": self.config.line_ending,
                })
            }
            _ => {
                // TODO: Implement payload construction for data manipulation operations
                // (Insert, Update, Upsert, Delete, HardDelete)
                // These operations will require object name and potentially external ID fields
                todo!("Implement other operations like Insert, Update, Upsert, Delete, HardDelete");
            }
        };

        // Configure HTTP client with Salesforce endpoint and authentication
        let mut client = self
            .client
            .post(sfdc_client.instance_url + DEFAULT_URI_PATH);

        client = client.bearer_auth(token_result.access_token().secret());
        client = client.json(&payload);

        // Execute API request and retrieve response
        let resp = client.send().await?.text().await?;

        // Prepare the response data for event emission
        let data = json!(resp);

        // Generate unique event subject for tracking and routing
        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                label.to_lowercase(),
                timestamp
            ),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Create and emit the response event
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;
        self.tx.send(e)?;
        event!(Level::INFO, "Event processed: {}", subject);

        Ok(())
    }
}

impl flowgen_core::task::runner::Runner for JobCreator {
    type Error = Error;
    
    /// Main execution loop for the job creator processor.
    /// 
    /// This method implements the flowgen_core Runner trait and provides the
    /// main execution logic for the processor. It:
    /// 1. Sets up an HTTPS-only HTTP client for security
    /// 2. Continuously listens for incoming events
    /// 3. Processes events that match the expected task ID
    /// 4. Spawns asynchronous handlers for each job creation request
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

                // Create handler for this specific job creation request
                let event_handler = EventHandler {
                    config,
                    current_task_id,
                    tx,
                    client,
                };
                
                // Process the event asynchronously to avoid blocking the main loop
                tokio::spawn(async move {
                    if let Err(err) = event_handler.handle().await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
        Ok(())
    }
}

/// Builder pattern implementation for constructing JobCreator instances.
/// 
/// This builder ensures that all required components are provided and properly
/// configured before creating a JobCreator instance. It follows the standard
/// Rust builder pattern with method chaining for ergonomic configuration.
#[derive(Default)]
pub struct ProcessorBuilder {
    /// Job configuration (required)
    config: Option<Arc<super::config::JobCreator>>,
    /// Event sender channel (required)
    tx: Option<Sender<Event>>,
    /// Event receiver channel (required)
    rx: Option<Receiver<Event>>,
    /// Task identifier for event correlation (defaults to 0)
    current_task_id: usize,
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

    /// Sets the job configuration.
    /// 
    /// # Arguments
    /// 
    /// * `config` - Shared configuration containing job parameters, credentials, and operation details
    pub fn config(mut self, config: Arc<super::config::JobCreator>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event receiver channel.
    /// 
    /// # Arguments
    /// 
    /// * `receiver` - Broadcast receiver for incoming trigger events
    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    /// Sets the event sender channel.
    /// 
    /// # Arguments
    /// 
    /// * `sender` - Broadcast sender for emitting processed events
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

    /// Builds the JobCreator instance after validating required fields.
    /// 
    /// # Returns
    /// 
    /// `Ok(JobCreator)` if all required fields are provided, or an `Error`
    /// indicating which required field is missing.
    /// 
    /// # Errors
    /// 
    /// Returns `MissingRequiredAttribute` error if any of the required fields
    /// (config, receiver, sender) are not provided.
    pub async fn build(self) -> Result<JobCreator, Error> {
        Ok(JobCreator {
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
    fn create_test_config() -> Arc<crate::bulkapi::config::JobCreator> {
        Arc::new(crate::bulkapi::config::JobCreator {
            label: Some("test_job".to_string()),
            credentials: "/tmp/test_creds.json".to_string(),
            query: Some("SELECT Id FROM Account".to_string()),
            object: Some("Account".to_string()),
            operation: crate::bulkapi::config::Operation::Query,
            content_type: Some(crate::bulkapi::config::ContentType::Csv),
            column_delimiter: Some(crate::bulkapi::config::ColumnDelimiter::Comma),
            line_ending: Some(crate::bulkapi::config::LineEnding::Crlf),
            assignment_rule_id: None,
            external_id_field_name: None,
        })
    }

    #[test]
    fn test_error_display() {
        let error = Error::MissingRequiredAttribute("config".to_string());
        assert_eq!(error.to_string(), "missing required attribute: config");

        let error = Error::EmptyArray();
        assert_eq!(error.to_string(), "no array data available");

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
    fn test_credentials_clone() {
        let original = Credentials {
            bearer_auth: Some("token".to_string()),
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
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
        let _tx_clone = tx.clone();
        
        let builder = ProcessorBuilder::new()
            .sender(tx)
            .receiver(rx);
        
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
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "bulkapicreate");
        assert_eq!(DEFAULT_URI_PATH, "/services/data/v61.0/jobs/query");
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

    #[test]
    fn test_credentials_partial_eq() {
        let creds1 = Credentials {
            bearer_auth: Some("token1".to_string()),
        };
        let creds2 = Credentials {
            bearer_auth: Some("token1".to_string()),
        };
        let creds3 = Credentials {
            bearer_auth: Some("token2".to_string()),
        };
        
        assert_eq!(creds1, creds2);
        assert_ne!(creds1, creds3);
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
    fn test_error_chain() {
        // Test error chaining and transparency
        let _inner_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        
        // Create a chain of errors to test error propagation
        let (tx, _rx) = broadcast::channel::<Event>(1);
        drop(tx); // Close the sender to cause a send error
        
        // The error types should properly chain and display their sources
    }
}