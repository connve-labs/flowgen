use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Publisher for creating Salesforce bulk jobs.:
/// ```json
/// {
///     "salesforce_query": {
///    "label": "salesforce_query_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "query",
///         "query": "Select Id from Account",
///         "content_type": "CSV",
///        "column_delimiter": "CARET",
///         "line_ending": "CRLF"
///     }
///  }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    /// Optional human-readable label for identifying this subscriber configuration.
    pub label: Option<String>,
    /// Reference to credential store entry containing Salesforce authentication details.
    pub credentials: String,
    /// Operation name related to Salesforce bulk job.
    pub operation: String,
    /// SOQL query to create the bulk job.
    pub query: String,
    /// Output file format for the bulk job.
    pub content_type: Option<String>,
    /// Column delimeter for output file for the bulk job.
    pub column_delimiter: Option<String>,
    /// Line ending for output file for the bulk job.
    pub line_ending: String,
}

impl ConfigExt for Publisher {}
