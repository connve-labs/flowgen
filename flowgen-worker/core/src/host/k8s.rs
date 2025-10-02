//! Kubernetes implementation of host coordination.
//!
//! Provides Kubernetes-based lease management using the coordination.k8s.io API.

use crate::host::{Error, Host};
use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams, PostParams},
    Client,
};
use std::sync::Arc;
use tracing::{debug, error, info};

/// Default namespace for Kubernetes resources.
const DEFAULT_NAMESPACE: &str = "default";

/// Default lease duration in seconds.
const DEFAULT_LEASE_DURATION_SECS: i32 = 60;

/// Kubernetes host coordinator for lease management.
#[derive(Clone)]
pub struct K8sHost {
    /// Kubernetes client.
    client: Arc<Client>,
    /// Namespace for Kubernetes resources.
    namespace: String,
    /// Lease duration in seconds.
    lease_duration_secs: i32,
}

#[async_trait]
impl Host for K8sHost {
    async fn create_lease(&self, name: &str) -> Result<(), Error> {
        let namespace = &self.namespace;

        let api: Api<Lease> = Api::namespaced((*self.client).clone(), namespace);

        let lease = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": name,
                "namespace": namespace,
            },
            "spec": {
                "holderIdentity": "flowgen",
                "leaseDurationSeconds": self.lease_duration_secs,
                "acquireTime": MicroTime(chrono::Utc::now()),
                "renewTime": MicroTime(chrono::Utc::now()),
            }
        });

        match api
            .create(
                &PostParams::default(),
                &serde_json::from_value(lease.clone())
                    .map_err(|e| Error::CreateLease(format!("Failed to deserialize lease: {e}")))?,
            )
            .await
        {
            Ok(_) => {
                info!("Created lease: {} in namespace: {}", name, namespace);
                Ok(())
            }
            Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
                debug!(
                    "Lease already exists: {} in namespace: {}, attempting to patch",
                    name, namespace
                );

                // Lease already exists, patch it.
                api.patch(name, &PatchParams::default(), &Patch::Merge(lease))
                    .await
                    .map_err(|e| Error::CreateLease(format!("Failed to patch lease: {e}")))?;

                info!(
                    "Patched existing lease: {} in namespace: {}",
                    name, namespace
                );
                Ok(())
            }
            Err(e) => {
                error!("Failed to create lease: {}", e);
                Err(Error::CreateLease(e.to_string()))
            }
        }
    }

    async fn delete_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let api: Api<Lease> = Api::namespaced((*self.client).clone(), namespace);

        api.delete(name, &DeleteParams::default())
            .await
            .map_err(|e| Error::DeleteLease(e.to_string()))?;

        info!("Deleted lease: {} in namespace: {}", name, namespace);
        Ok(())
    }

    async fn renew_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error> {
        let namespace = namespace.unwrap_or(&self.namespace);
        let api: Api<Lease> = Api::namespaced((*self.client).clone(), namespace);

        let patch = serde_json::json!({
            "spec": {
                "renewTime": MicroTime(chrono::Utc::now()),
            }
        });

        api.patch(name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(|e| Error::RenewLease(e.to_string()))?;

        debug!("Renewed lease: {} in namespace: {}", name, namespace);
        Ok(())
    }
}

/// Builder for K8sHost.
pub struct K8sHostBuilder {
    client: Option<Client>,
    namespace: String,
    lease_duration_secs: i32,
}

impl Default for K8sHostBuilder {
    fn default() -> Self {
        Self {
            client: None,
            namespace: DEFAULT_NAMESPACE.to_string(),
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
        }
    }
}

impl K8sHostBuilder {
    /// Creates a new K8sHostBuilder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the Kubernetes client.
    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Sets the namespace for Kubernetes resources.
    pub fn namespace(mut self, namespace: String) -> Self {
        self.namespace = namespace;
        self
    }

    /// Sets the lease duration in seconds.
    pub fn lease_duration_secs(mut self, duration: i32) -> Self {
        self.lease_duration_secs = duration;
        self
    }

    /// Builds the K8sHost instance.
    pub async fn build(self) -> Result<K8sHost, kube::Error> {
        let client = if let Some(client) = self.client {
            client
        } else {
            Client::try_default().await?
        };

        Ok(K8sHost {
            client: Arc::new(client),
            namespace: self.namespace,
            lease_duration_secs: self.lease_duration_secs,
        })
    }
}
