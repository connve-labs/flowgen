//! Host coordination and lease management.
//!
//! Defines the abstraction for distributed coordination operations
//! such as lease management across different hosting platforms.

use async_trait::async_trait;

pub mod k8s;

/// Errors that can occur during host operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to create lease.
    #[error("Failed to create lease: {0}")]
    CreateLease(String),
    /// Failed to delete lease.
    #[error("Failed to delete lease: {0}")]
    DeleteLease(String),
    /// Failed to renew lease.
    #[error("Failed to renew lease: {0}")]
    RenewLease(String),
}

/// Trait for host coordination operations.
#[async_trait]
pub trait Host: Send + Sync {
    /// Creates a new lease with the given name.
    async fn create_lease(&self, name: &str) -> Result<(), Error>;

    /// Deletes an existing lease.
    async fn delete_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error>;

    /// Renews an existing lease.
    async fn renew_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error>;
}
