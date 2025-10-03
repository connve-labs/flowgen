use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::debug;

/// Task manager errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to send event: {0}")]
    SendError(#[source] mpsc::error::SendError<TaskRegistration>),
}
/// Leader election options for tasks requiring coordination.
#[derive(Debug, Clone)]
pub struct LeaderElectionOptions {
    // Future: task-specific overrides.
}

/// Task registration event.
#[derive(Debug, Clone)]
struct TaskRegistration {
    task_id: String,
    leader_election_options: Option<LeaderElectionOptions>,
}

/// Internal task state managed by TaskManager.
#[derive(Debug)]
struct TaskState {
    task_id: String,
    lease_handle: Option<JoinHandle<()>>,
}

/// Centralized task lifecycle manager.
/// Handles task registration, coordination, and resource management.
pub struct TaskManager {
    tx: Arc<Mutex<Option<UnboundedSender<TaskRegistration>>>>,
    host: Option<Arc<dyn crate::host::Host>>,
}

impl TaskManager {
    /// Starts the task manager event loop.
    pub async fn start(self) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<TaskRegistration>();

        // Store the sender.
        *self.tx.lock().await = Some(tx);

        // Event processing loop.
        tokio::spawn(async move {
            while let Some(registration) = rx.recv().await {
                // Process task registration.
                debug!("Received task registration: {:?}", registration.task_id);
                // Future: handle leader election, lease management, etc.
            }
        });

        self
    }

    /// Registers a task with the manager.
    pub async fn register(
        &self,
        task_id: String,
        leader_election_options: Option<LeaderElectionOptions>,
    ) -> Result<(), Error> {
        if let Some(tx) = self.tx.lock().await.as_ref() {
            tx.send(TaskRegistration {
                task_id,
                leader_election_options,
            })
            .map_err(Error::SendError)?;
        }
        Ok(())
    }
}

/// Builder for TaskManager.
#[derive(Default)]
pub struct TaskManagerBuilder {
    host: Option<std::sync::Arc<dyn crate::host::Host>>,
}

impl TaskManagerBuilder {
    /// Creates a new TaskManager builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the host client for leader election.
    pub fn host(mut self, host: std::sync::Arc<dyn crate::host::Host>) -> Self {
        self.host = Some(host);
        self
    }

    /// Builds the TaskManager configuration.
    pub fn build(self) -> TaskManager {
        TaskManager {
            tx: Arc::new(Mutex::new(None)),
            host: self.host,
        }
    }
}
