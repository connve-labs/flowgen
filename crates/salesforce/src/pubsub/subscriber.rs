use super::eventbus::v1::{FetchRequest, FetchResponse, TopicInfo, TopicRequest};
use flowgen_core::client::Client;
use std::sync::Arc;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
    #[error("There was an error executing async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
    #[error("There was an error with sending message over channel.")]
    TokioSendMessage(#[source] tokio::sync::mpsc::error::SendError<Message>),
}

pub struct Subscriber {
    pub async_task_list: Vec<JoinHandle<Result<(), Error>>>,
    pub pubsub: Arc<Mutex<super::context::Context>>,
    pub rx: Receiver<Message>,
    pub tx: Sender<Message>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub fetch_response: FetchResponse,
    pub topic_info: TopicInfo,
}

pub struct Builder {
    service: flowgen_core::service::Service,
    config: super::config::Source,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(service: flowgen_core::service::Service, config: super::config::Source) -> Builder {
        Builder { service, config }
    }

    /// Builds a new FlowgenSalesforcePubsub Subscriber.
    pub async fn build(self) -> Result<Subscriber, Error> {
        // Connect to Salesforce.
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        // Get PubSub context.
        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        let mut async_task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let pubsub = Arc::new(Mutex::new(pubsub));
        let (tx, rx) = tokio::sync::mpsc::channel(200);

        for topic in self.config.topic_list.iter() {
            let pubsub: Arc<Mutex<super::context::Context>> = Arc::clone(&pubsub);
            let tx = tx.clone();
            let topic = topic.clone();

            let subscribe_task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let topic_info: TopicInfo = pubsub
                    .lock()
                    .await
                    .get_topic(TopicRequest {
                        topic_name: topic.clone(),
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                let mut stream = pubsub
                    .lock()
                    .await
                    .subscribe(FetchRequest {
                        topic_name: topic,
                        num_requested: 200,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::FlowgenSalesforcePubSub)?
                    .into_inner();

                while let Some(received) = stream.next().await {
                    match received {
                        Ok(fr) => {
                            let m = Message {
                                fetch_response: fr,
                                topic_info: topic_info.clone(),
                            };
                            tx.send(m).await.map_err(Error::TokioSendMessage)?;
                        }
                        Err(e) => {
                            return Err(Error::FlowgenSalesforcePubSub(
                                super::context::Error::RPCFailed(e),
                            ));
                        }
                    }
                }
                Ok(())
            });
            async_task_list.push(subscribe_task);
        }

        let s = Subscriber {
            async_task_list,
            pubsub,
            rx,
            tx,
        };

        Ok(s)
    }
}
