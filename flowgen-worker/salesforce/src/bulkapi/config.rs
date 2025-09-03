use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};

/// Processor for creating salesforce account query job.
/// ```json
/// {
///     "salesforce_bulkapi_job_creator": {
///    "label": "salesforce_query_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "query",
///         "job": "Select Id from Account",
///         "content_type": "CSV",
///         "column_delimiter": "COMMA",
///         "line_ending": "CRLF"
///     }
///  }
/// ```
///
///
/// /// Processor for creating salesforce account query all job.
/// ```json
/// {
///     "salesforce_bulkapi_job_creator": {
///    "label": "salesforce_query_all_job",
///         "credentials": "/etc/sfdc_dev.json",
///         "operation": "queryAll",
///         "job": "Select Id from Account",
///         "content_type": "CSV",
///         "column_delimiter": "COMMA",
///         "line_ending": "CRLF"
///     }
///  }
/// ```


/// Configuration for retrieving existing Salesforce bulk jobs.
/// 
/// This struct is used to configure a job retrieval operation that can fetch
/// information about previously created Salesforce bulk jobs, including their
/// status, results, and metadata.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobRetriever {
    /// Optional human-readable label for identifying this job retriever configuration.
    /// 
    /// Useful for distinguishing between multiple job retrieval configurations
    /// in logging, monitoring, and configuration management contexts.
    pub label: Option<String>,
    
    /// Reference to credential store entry containing Salesforce authentication details.
    /// 
    /// This should be a key or path to securely stored Salesforce credentials
    /// (username, password, security token, or OAuth tokens) required for
    /// authenticating with the Salesforce Bulk API.
    pub credentials: String,
}

/// Configuration for creating new Salesforce bulk jobs.
/// 
/// This struct encapsulates all the parameters needed to create and configure
/// a Salesforce Bulk API job for various operations like querying, inserting,
/// updating, or deleting records in bulk.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct JobCreator {
    /// Optional human-readable label for identifying this job creator configuration.
    /// 
    /// Helpful for distinguishing between different job configurations in logs,
    /// monitoring dashboards, and configuration management systems.
    pub label: Option<String>,
    
    /// Reference to credential store entry containing Salesforce authentication details.
    /// 
    /// Points to securely stored Salesforce credentials needed for Bulk API authentication.
    /// Should not contain actual credentials for security reasons.
    pub credentials: String,
    
    /// SOQL query string for query and queryAll operations.
    /// 
    /// Required for query-based operations. Should contain a valid SOQL query
    /// that will be executed against the Salesforce database.
    /// Example: "SELECT Id, Name FROM Account WHERE CreatedDate = TODAY"
    pub query: Option<String>,
    
    /// Salesforce object API name for data manipulation operations.
    /// 
    /// Required for insert, update, delete, and upsert operations.
    /// Specifies the Salesforce object type to operate on.
    /// Examples: "Account", "Contact", "Custom_Object__c"
    pub object: Option<String>,
    
    /// The type of bulk operation to perform.
    /// 
    /// Determines whether this job will query data, insert records,
    /// update existing records, etc. Each operation has different
    /// requirements for other configuration fields.
    pub operation: Operation,
    
    /// Output file format for bulk job results.
    /// 
    /// Specifies how the job results should be formatted when retrieved.
    /// Currently only CSV format is supported by most Salesforce bulk operations.
    pub content_type: Option<ContentType>,
    
    /// Column separator character for CSV output files.
    /// 
    /// Defines how columns are separated in the result CSV files.
    /// Different delimiters may be needed for compatibility with
    /// downstream systems or to handle data containing commas.
    pub column_delimiter: Option<ColumnDelimiter>,
    
    /// Line termination style for output files.
    /// 
    /// Specifies whether to use Unix-style (LF) or Windows-style (CRLF)
    /// line endings in the output files. Important for cross-platform
    /// compatibility of generated files.
    pub line_ending: Option<LineEnding>,
    
    /// The ID of an assignment rule to run for Case or Lead objects.
    /// 
    /// When creating or updating Case or Lead records, this field can specify
    /// an assignment rule to automatically assign the records to appropriate
    /// users or queues. The rule can be active or inactive.
    /// 
    /// The ID can be retrieved using Salesforce SOAP or REST APIs to query
    /// the AssignmentRule object.
    pub assignment_rule_id: Option<String>,
    
    /// The external ID field name for upsert operations.
    /// 
    /// Required only for upsert operations. Specifies which field should be used
    /// as the external identifier to determine whether to insert a new record
    /// or update an existing one.
    /// 
    /// Example: "External_ID__c" or "Email" for Contact objects
    pub external_id_field_name: Option<String>,
}

/// Enumeration of supported Salesforce Bulk API operations.
/// 
/// Each operation type has different requirements and behaviors:
/// - Query operations retrieve data and require a SOQL query
/// - Data manipulation operations require an object name and appropriate data
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Standard query operation that returns only non-deleted records.
    /// 
    /// Requires a valid SOQL query in the `query` field. Results include
    /// only records that are currently active (not in the recycle bin).
    #[default]
    #[serde(rename = "query")]
    Query,
    
    /// Query all operation that includes deleted and archived records.
    /// 
    /// Similar to Query but also returns soft-deleted records from the
    /// recycle bin. Useful for data archival and complete data exports.
    #[serde(rename = "queryAll")]
    QueryAll,
    
    /// Insert operation for creating new records.
    /// 
    /// Creates new records in Salesforce. Requires `object` field to specify
    /// the target Salesforce object type.
    #[serde(rename = "insert")]
    Insert,
    
    /// Soft delete operation that moves records to the recycle bin.
    /// 
    /// Marks records as deleted but allows them to be restored later.
    /// Records can be recovered from the recycle bin within the retention period.
    #[serde(rename = "delete")]
    Delete,
    
    /// Hard delete operation that permanently removes records.
    /// 
    /// Permanently deletes records without moving them to the recycle bin.
    /// This operation cannot be undone and requires special permissions.
    #[serde(rename = "hardDelete")]
    HardDelete,
    
    /// Update operation for modifying existing records.
    /// 
    /// Updates existing records identified by their Salesforce ID.
    /// Requires records to include the Id field for identification.
    #[serde(rename = "update")]
    Update,
    
    /// Upsert operation that inserts or updates based on external ID.
    /// 
    /// Creates new records or updates existing ones based on an external
    /// identifier field. Requires `external_id_field_name` to be specified.
    #[serde(rename = "upsert")]
    Upsert,
}

/// Supported content types for bulk job output files.
/// 
/// Currently limited to CSV format, which is the most widely supported
/// format for Salesforce bulk operations and downstream data processing.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ContentType {
    /// Comma-Separated Values format.
    /// 
    /// Standard tabular data format with rows and columns. Most compatible
    /// with spreadsheet applications and data processing tools.
    #[default]
    #[serde(rename = "CSV")]
    Csv, // Currently only supports CSV.
}

/// Available column delimiter options for CSV output formatting.
/// 
/// Different delimiters may be required based on the data content or
/// compatibility requirements with downstream systems.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum ColumnDelimiter {
    /// Standard comma delimiter (most common CSV format).
    /// 
    /// Default choice for CSV files. May cause issues if data contains commas,
    /// but most CSV parsers handle quoted fields correctly.
    #[default]
    #[serde(rename = "COMMA")]
    Comma,
    
    /// Tab character as delimiter.
    /// 
    /// Useful when data contains many commas or for tab-separated value (TSV) files.
    /// Often preferred for data interchange between systems.
    #[serde(rename = "TAB")]
    Tab,
    
    /// Semicolon delimiter.
    /// 
    /// Common in European locales where comma is used as decimal separator.
    /// Useful for international data processing requirements.
    #[serde(rename = "SEMICOLON")]
    Semicolon,
    
    /// Pipe character (|) as delimiter.
    /// 
    /// Less likely to appear in actual data content, making parsing more reliable.
    /// Often used in data warehousing and ETL processes.
    #[serde(rename = "PIPE")]
    Pipe,
    
    /// Caret character (^) as delimiter.
    /// 
    /// Rarely appears in business data, providing reliable field separation.
    /// Sometimes used in legacy systems or specialized data formats.
    #[serde(rename = "CARET")]
    Caret,
    
    /// Backquote character (`) as delimiter.
    /// 
    /// Another rare character that can serve as a reliable delimiter
    /// when standard options might conflict with data content.
    #[serde(rename = "BACKQUOTE")]
    Backquote,
}

/// Line ending styles for output file formatting.
/// 
/// Different operating systems use different conventions for line endings.
/// Choosing the correct style ensures proper file handling across platforms.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum LineEnding {
    /// Unix/Linux style line ending (Line Feed only).
    /// 
    /// Single LF character (\n). Standard on Unix-like systems including
    /// Linux and macOS. More compact than CRLF.
    #[default]
    #[serde(rename = "LF")]
    Lf,
    
    /// Windows style line ending (Carriage Return + Line Feed).
    /// 
    /// Two-character sequence (\r\n). Required for proper display
    /// in Windows text editors and some legacy systems.
    #[serde(rename = "CRLF")]
    Crlf,
}

// Implement configuration extension traits for both structs
// This likely provides additional configuration-related functionality
// from the flowgen_core framework.
impl ConfigExt for JobCreator {}
impl ConfigExt for JobRetriever {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_job_retriever_default() {
        let retriever = JobRetriever::default();
        assert_eq!(retriever.label, None);
        assert_eq!(retriever.credentials, String::new());
    }

    #[test]
    fn test_job_retriever_creation() {
        let retriever = JobRetriever {
            label: Some("test_retriever".to_string()),
            credentials: "test_creds".to_string(),
        };
        assert_eq!(retriever.label, Some("test_retriever".to_string()));
        assert_eq!(retriever.credentials, "test_creds");
    }

    #[test]
    fn test_job_retriever_clone() {
        let original = JobRetriever {
            label: Some("original".to_string()),
            credentials: "creds".to_string(),
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_job_retriever_partial_eq() {
        let retriever1 = JobRetriever {
            label: Some("test".to_string()),
            credentials: "creds".to_string(),
        };
        let retriever2 = JobRetriever {
            label: Some("test".to_string()),
            credentials: "creds".to_string(),
        };
        let retriever3 = JobRetriever {
            label: Some("different".to_string()),
            credentials: "creds".to_string(),
        };
        
        assert_eq!(retriever1, retriever2);
        assert_ne!(retriever1, retriever3);
    }

    #[test]
    fn test_job_retriever_debug() {
        let retriever = JobRetriever {
            label: Some("debug_test".to_string()),
            credentials: "debug_creds".to_string(),
        };
        let debug_output = format!("{:?}", retriever);
        assert!(debug_output.contains("debug_test"));
        assert!(debug_output.contains("debug_creds"));
    }

    #[test]
    fn test_job_creator_default() {
        let creator = JobCreator::default();
        assert_eq!(creator.label, None);
        assert_eq!(creator.credentials, String::new());
        assert_eq!(creator.query, None);
        assert_eq!(creator.object, None);
        assert_eq!(creator.operation, Operation::Query);
        assert_eq!(creator.content_type, None);
        assert_eq!(creator.column_delimiter, None);
        assert_eq!(creator.line_ending, None);
        assert_eq!(creator.assignment_rule_id, None);
        assert_eq!(creator.external_id_field_name, None);
    }

    #[test]
    fn test_job_creator_full_configuration() {
        let creator = JobCreator {
            label: Some("full_config".to_string()),
            credentials:("full_creds".to_string()),
            query: Some("SELECT Id FROM Account".to_string()),
            object: Some("Account".to_string()),
            operation: Operation::Query,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Comma),
            line_ending: Some(LineEnding::Crlf),
            assignment_rule_id: Some("rule123".to_string()),
            external_id_field_name: Some("External_ID__c".to_string()),
        };

        assert_eq!(creator.label, Some("full_config".to_string()));
        assert_eq!(creator.credentials, "full_creds");
        assert_eq!(creator.query, Some("SELECT Id FROM Account".to_string()));
        assert_eq!(creator.object, Some("Account".to_string()));
        assert_eq!(creator.operation, Operation::Query);
        assert_eq!(creator.content_type, Some(ContentType::Csv));
        assert_eq!(creator.column_delimiter, Some(ColumnDelimiter::Comma));
        assert_eq!(creator.line_ending, Some(LineEnding::Crlf));
        assert_eq!(creator.assignment_rule_id, Some("rule123".to_string()));
        assert_eq!(creator.external_id_field_name, Some("External_ID__c".to_string()));
    }

    #[test]
    fn test_operation_variants() {
        assert_eq!(Operation::default(), Operation::Query);
        
        let operations = vec![
            Operation::Query,
            Operation::QueryAll,
            Operation::Insert,
            Operation::Delete,
            Operation::HardDelete,
            Operation::Update,
            Operation::Upsert,
        ];

        for op in operations {
            // Test cloning
            let cloned = op.clone();
            assert_eq!(op, cloned);
            
            // Test debug formatting
            let debug_str = format!("{:?}", op);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_content_type_variants() {
        assert_eq!(ContentType::default(), ContentType::Csv);
        
        let csv = ContentType::Csv;
        let cloned = csv.clone();
        assert_eq!(csv, cloned);
        
        let debug_str = format!("{:?}", csv);
        assert!(debug_str.contains("Csv"));
    }

    #[test]
    fn test_column_delimiter_variants() {
        assert_eq!(ColumnDelimiter::default(), ColumnDelimiter::Comma);
        
        let delimiters = vec![
            ColumnDelimiter::Comma,
            ColumnDelimiter::Tab,
            ColumnDelimiter::Semicolon,
            ColumnDelimiter::Pipe,
            ColumnDelimiter::Caret,
            ColumnDelimiter::Backquote,
        ];

        for delimiter in delimiters {
            let cloned = delimiter.clone();
            assert_eq!(delimiter, cloned);
            
            let debug_str = format!("{:?}", delimiter);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_line_ending_variants() {
        assert_eq!(LineEnding::default(), LineEnding::Lf);
        
        let endings = vec![LineEnding::Lf, LineEnding::Crlf];

        for ending in endings {
            let cloned = ending.clone();
            assert_eq!(ending, cloned);
            
            let debug_str = format!("{:?}", ending);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_job_retriever_serialization() {
        let retriever = JobRetriever {
            label: Some("serialization_test".to_string()),
            credentials: "serial_creds".to_string(),
        };

        // Test serialization
        let json = serde_json::to_string(&retriever).unwrap();
        assert!(json.contains("serialization_test"));
        assert!(json.contains("serial_creds"));

        // Test deserialization
        let deserialized: JobRetriever = serde_json::from_str(&json).unwrap();
        assert_eq!(retriever, deserialized);
    }

    #[test]
    fn test_job_creator_serialization() {
        let creator = JobCreator {
            label: Some("json_test".to_string()),
            credentials: "json_creds".to_string(),
            query: Some("SELECT Id FROM Contact".to_string()),
            object: Some("Contact".to_string()),
            operation: Operation::Insert,
            content_type: Some(ContentType::Csv),
            column_delimiter: Some(ColumnDelimiter::Tab),
            line_ending: Some(LineEnding::Lf),
            assignment_rule_id: Some("assign_rule".to_string()),
            external_id_field_name: Some("Email".to_string()),
        };

        let json = serde_json::to_string(&creator).unwrap();
        let deserialized: JobCreator = serde_json::from_str(&json).unwrap();
        assert_eq!(creator, deserialized);
    }

    #[test]
    fn test_operation_serialization() {
        let test_cases = vec![
            (Operation::Query, "\"query\""),
            (Operation::QueryAll, "\"queryAll\""),
            (Operation::Insert, "\"insert\""),
            (Operation::Delete, "\"delete\""),
            (Operation::HardDelete, "\"hardDelete\""),
            (Operation::Update, "\"update\""),
            (Operation::Upsert, "\"upsert\""),
        ];

        for (operation, expected_json) in test_cases {
            let json = serde_json::to_string(&operation).unwrap();
            assert_eq!(json, expected_json);
            
            let deserialized: Operation = serde_json::from_str(&json).unwrap();
            assert_eq!(operation, deserialized);
        }
    }

    #[test]
    fn test_content_type_serialization() {
        let csv = ContentType::Csv;
        let json = serde_json::to_string(&csv).unwrap();
        assert_eq!(json, "\"CSV\"");
        
        let deserialized: ContentType = serde_json::from_str(&json).unwrap();
        assert_eq!(csv, deserialized);
    }

    #[test]
    fn test_column_delimiter_serialization() {
        let test_cases = vec![
            (ColumnDelimiter::Comma, "\"COMMA\""),
            (ColumnDelimiter::Tab, "\"TAB\""),
            (ColumnDelimiter::Semicolon, "\"SEMICOLON\""),
            (ColumnDelimiter::Pipe, "\"PIPE\""),
            (ColumnDelimiter::Caret, "\"CARET\""),
            (ColumnDelimiter::Backquote, "\"BACKQUOTE\""),
        ];

        for (delimiter, expected_json) in test_cases {
            let json = serde_json::to_string(&delimiter).unwrap();
            assert_eq!(json, expected_json);
            
            let deserialized: ColumnDelimiter = serde_json::from_str(&json).unwrap();
            assert_eq!(delimiter, deserialized);
        }
    }

    #[test]
    fn test_line_ending_serialization() {
        let test_cases = vec![
            (LineEnding::Lf, "\"LF\""),
            (LineEnding::Crlf, "\"CRLF\""),
        ];

        for (ending, expected_json) in test_cases {
            let json = serde_json::to_string(&ending).unwrap();
            assert_eq!(json, expected_json);
            
            let deserialized: LineEnding = serde_json::from_str(&json).unwrap();
            assert_eq!(ending, deserialized);
        }
    }

    #[test]
    fn test_job_retriever_with_none_label() {
        let retriever = JobRetriever {
            label: None,
            credentials: "no_label_creds".to_string(),
        };

        let json = serde_json::to_string(&retriever).unwrap();
        let deserialized: JobRetriever = serde_json::from_str(&json).unwrap();
        assert_eq!(retriever, deserialized);
        assert_eq!(deserialized.label, None);
    }

    #[test]
    fn test_job_creator_minimal_config() {
        let creator = JobCreator {
            label: None,
            credentials: "minimal_creds".to_string(),
            query: None,
            object: None,
            operation: Operation::Query,
            content_type: None,
            column_delimiter: None,
            line_ending: None,
            assignment_rule_id: None,
            external_id_field_name: None,
        };

        let json = serde_json::to_string(&creator).unwrap();
        let deserialized: JobCreator = serde_json::from_str(&json).unwrap();
        assert_eq!(creator, deserialized);
    }

    #[test]
    fn test_complex_json_deserialization() {
        let json_str = r#"{
            "label": "complex_test",
            "credentials": "/path/to/creds.json",
            "query": "SELECT Id, Name FROM Account WHERE CreatedDate = TODAY",
            "object": "Account",
            "operation": "queryAll",
            "content_type": "CSV",
            "column_delimiter": "PIPE",
            "line_ending": "CRLF",
            "assignment_rule_id": "03d000000000001",
            "external_id_field_name": "External_System_ID__c"
        }"#;

        let creator: JobCreator = serde_json::from_str(json_str).unwrap();
        assert_eq!(creator.label, Some("complex_test".to_string()));
        assert_eq!(creator.operation, Operation::QueryAll);
        assert_eq!(creator.column_delimiter, Some(ColumnDelimiter::Pipe));
        assert_eq!(creator.line_ending, Some(LineEnding::Crlf));
    }

    #[test]
    fn test_config_ext_implementation() {
        // Test that ConfigExt is properly implemented
        // This test assumes ConfigExt has some basic functionality
        let _creator: Box<dyn ConfigExt> = Box::new(JobCreator::default());
        let _retriever: Box<dyn ConfigExt> = Box::new(JobRetriever::default());
        
        // If ConfigExt has specific methods, test them here
        // For now, just verify the trait is implemented
    }

    #[test]
    fn test_enum_exhaustive_matching() {
        // Test that all enum variants can be matched
        fn match_operation(op: Operation) -> &'static str {
            match op {
                Operation::Query => "query",
                Operation::QueryAll => "queryAll",
                Operation::Insert => "insert",
                Operation::Delete => "delete",
                Operation::HardDelete => "hardDelete",
                Operation::Update => "update",
                Operation::Upsert => "upsert",
            }
        }

        fn match_content_type(ct: ContentType) -> &'static str {
            match ct {
                ContentType::Csv => "csv",
            }
        }

        fn match_delimiter(del: ColumnDelimiter) -> &'static str {
            match del {
                ColumnDelimiter::Comma => "comma",
                ColumnDelimiter::Tab => "tab",
                ColumnDelimiter::Semicolon => "semicolon",
                ColumnDelimiter::Pipe => "pipe",
                ColumnDelimiter::Caret => "caret",
                ColumnDelimiter::Backquote => "backquote",
            }
        }

        fn match_line_ending(le: LineEnding) -> &'static str {
            match le {
                LineEnding::Lf => "lf",
                LineEnding::Crlf => "crlf",
            }
        }

        // Test all variants
        assert_eq!(match_operation(Operation::Query), "query");
        assert_eq!(match_content_type(ContentType::Csv), "csv");
        assert_eq!(match_delimiter(ColumnDelimiter::Comma), "comma");
        assert_eq!(match_line_ending(LineEnding::Lf), "lf");
    }

    #[test]
    fn test_error_handling_invalid_json() {
        // Test deserialization with invalid operation
        let invalid_operation_json = r#"{"credentials": "test", "operation": "invalid_op"}"#;
        let result: Result<JobCreator, _> = serde_json::from_str(invalid_operation_json);
        assert!(result.is_err());

        // Test deserialization with invalid content type
        let invalid_content_type_json = r#"{"credentials": "test", "content_type": "XML"}"#;
        let result: Result<JobCreator, _> = serde_json::from_str(invalid_content_type_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that default instances don't allocate unnecessary memory
        let default_creator = JobCreator::default();
        let default_retriever = JobRetriever::default();
        
        // Verify optional fields are None and strings are empty
        assert!(default_creator.label.is_none());
        assert!(default_creator.query.is_none());
        assert!(default_creator.object.is_none());
        assert!(default_creator.content_type.is_none());
        assert!(default_retriever.label.is_none());
        assert!(default_creator.credentials.is_empty());
        assert!(default_retriever.credentials.is_empty());
    }
}