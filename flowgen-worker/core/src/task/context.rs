//! Task execution context providing metadata and runtime configuration.
//!
//! Contains task and flow identification, runtime feature flags, and other shared
//! context that tasks need for proper execution, logging, and coordination.

use serde_json::{Map, Value};

/// Errors that can occur during TaskContext operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// Flow identification and metadata.
#[derive(Clone, Debug)]
pub struct FlowOptions {
    /// Flow name.
    pub name: String,
    /// Optional labels for flow metadata.
    pub labels: Option<Map<String, Value>>,
}

/// Context information for task execution shared across all tasks.
#[derive(Clone)]
pub struct TaskContext {
    /// Flow identification and metadata.
    pub flow: FlowOptions,
    /// Optional host client for coordination (e.g., lease management).
    pub host: Option<HostClient>,
}

/// Host client for distributed coordination.
#[derive(Clone)]
pub struct HostClient {
    /// Arc-wrapped host implementation for shared access.
    pub client: std::sync::Arc<dyn crate::host::Host>,
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("flow", &self.flow)
            .field("host", &self.host.is_some())
            .finish()
    }
}

impl std::fmt::Debug for HostClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostClient")
            .field("client", &"<dyn Host>")
            .finish()
    }
}

/// Builder for constructing TaskContext instances.
#[derive(Default)]
pub struct TaskContextBuilder {
    /// Unique flow name.
    flow_name: Option<String>,
    /// Optional labels for flow metadata.
    flow_labels: Option<Map<String, Value>>,
    /// Optional host client for coordination.
    host: Option<HostClient>,
}

impl TaskContextBuilder {
    /// Creates a new TaskContextBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the unique flow name.
    ///
    /// # Arguments
    /// * `id` - The unique identifier for this flow
    pub fn flow_name(mut self, name: String) -> Self {
        self.flow_name = Some(name);
        self
    }

    /// Sets the optional flow labels for metadata.
    ///
    /// # Arguments
    /// * `labels` - Optional labels map for metadata and logging
    pub fn flow_labels(mut self, labels: Option<Map<String, Value>>) -> Self {
        self.flow_labels = labels;
        self
    }

    /// Sets the host client for coordination.
    ///
    /// # Arguments
    /// * `host` - Optional host client for distributed coordination
    pub fn host(mut self, host: Option<HostClient>) -> Self {
        self.host = host;
        self
    }

    /// Builds the TaskContext instance.
    ///
    /// # Errors
    /// Returns `Error::MissingRequiredAttribute` if required fields are not set.
    pub fn build(self) -> Result<TaskContext, Error> {
        Ok(TaskContext {
            flow: FlowOptions {
                name: self
                    .flow_name
                    .ok_or_else(|| Error::MissingRequiredAttribute("flow_name".to_string()))?,
                labels: self.flow_labels,
            },
            host: self.host,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_context_builder_new() {
        let builder = TaskContextBuilder::new();
        assert!(builder.flow_name.is_none());
        assert!(builder.flow_labels.is_none());
    }

    #[test]
    fn test_task_context_builder_build_success() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test Flow".to_string()));
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let context = TaskContextBuilder::new()
            .flow_name("test-flow".to_string())
            .flow_labels(Some(labels.clone()))
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "test-flow");
        assert_eq!(context.flow.labels, Some(labels));
    }

    #[test]
    fn test_task_context_builder_missing_flow_name() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test".to_string()));

        let result = TaskContextBuilder::new().flow_labels(Some(labels)).build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: flow_name"));
    }

    #[test]
    fn test_task_context_builder_defaults() {
        let context = TaskContextBuilder::new()
            .flow_name("default-test".to_string())
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "default-test");
        assert!(context.flow.labels.is_none());
    }

    #[test]
    fn test_task_context_builder_chain() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Chained Builder Test".to_string()),
        );
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let context = TaskContextBuilder::new()
            .flow_name("chain-test".to_string())
            .flow_labels(Some(labels.clone()))
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "chain-test");
        assert_eq!(context.flow.labels, Some(labels));
    }

    #[test]
    fn test_task_context_clone() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );

        let context = TaskContextBuilder::new()
            .flow_name("clone-test".to_string())
            .flow_labels(Some(labels.clone()))
            .build()
            .unwrap();

        let cloned = context.clone();
        assert_eq!(context.flow.name, cloned.flow.name);
        assert_eq!(context.flow.labels, cloned.flow.labels);
    }
}
