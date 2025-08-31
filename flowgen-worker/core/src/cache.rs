//! Caching abstractions for persistent storage across workflow executions.
//!
//! Provides traits and configuration for key-value caching systems used by
//! tasks that need to maintain state between runs, such as replay identifiers.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Configuration options for cache operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Optional key override for cache insertion operations.
    pub insert_key: Option<String>,
    /// Optional key override for cache retrieval operations.
    pub retrieve_key: Option<String>,
}

/// Trait for asynchronous key-value cache implementations.
///
/// Provides a unified interface for different caching backends like NATS JetStream,
/// Redis, or other persistent storage systems used for workflow state management.
pub trait Cache: Debug + Send + Sync + 'static {
    /// Error type for cache operations.
    type Error: Debug + Send + Sync + 'static;

    /// Initializes the cache with a specific bucket or namespace.
    ///
    /// # Arguments
    /// * `bucket` - The bucket or namespace identifier for this cache instance
    fn init(
        self,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;

    /// Stores a value in the cache with the given key.
    ///
    /// # Arguments
    /// * `key` - The key to store the value under
    /// * `value` - The binary data to store
    fn put(
        &self,
        key: &str,
        value: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;

    /// Retrieves a value from the cache by key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve the value for
    ///
    /// # Returns
    /// The cached binary data or an error if the key is not found
    fn get(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<bytes::Bytes, Self::Error>> + Send
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Mock cache implementation for testing.
    #[derive(Debug)]
    struct MockCache {
        data: HashMap<String, bytes::Bytes>,
        should_error: bool,
    }

    #[derive(Debug)]
    struct MockError;

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Mock cache error")
        }
    }

    impl std::error::Error for MockError {}

    impl Cache for MockCache {
        type Error = MockError;

        async fn init(self, _bucket: &str) -> Result<Self, Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                Ok(self)
            }
        }

        async fn put(&self, _key: &str, _value: bytes::Bytes) -> Result<(), Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                Ok(())
            }
        }

        async fn get(&self, key: &str) -> Result<bytes::Bytes, Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                Ok(self.data.get(key).cloned().unwrap_or_default())
            }
        }
    }

    #[tokio::test]
    async fn test_cache_init_success() {
        let cache = MockCache {
            data: HashMap::new(),
            should_error: false,
        };

        let result = cache.init("test_bucket").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cache_init_error() {
        let cache = MockCache {
            data: HashMap::new(),
            should_error: true,
        };

        let result = cache.init("test_bucket").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_put_success() {
        let cache = MockCache {
            data: HashMap::new(),
            should_error: false,
        };

        let result = cache
            .put("test_key", bytes::Bytes::from("test_value"))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cache_get_success() {
        let mut data = HashMap::new();
        data.insert(
            "existing_key".to_string(),
            bytes::Bytes::from("existing_value"),
        );

        let cache = MockCache {
            data,
            should_error: false,
        };

        let result = cache.get("existing_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), bytes::Bytes::from("existing_value"));
    }

    #[test]
    fn test_cache_options_default() {
        let options = CacheOptions {
            insert_key: None,
            retrieve_key: None,
        };

        assert!(options.insert_key.is_none());
        assert!(options.retrieve_key.is_none());
    }

    #[test]
    fn test_cache_options_serialization() {
        let options = CacheOptions {
            insert_key: Some("insert".to_string()),
            retrieve_key: Some("retrieve".to_string()),
        };

        let serialized = serde_json::to_string(&options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();

        assert_eq!(options, deserialized);
    }
}
