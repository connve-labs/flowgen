use std::fs;
use std::path::PathBuf;

/// Default NATS Server host.
const DEFAULT_NATS_HOST: &str = "localhost:4222";

/// Used to store NATS Client credentials.
#[derive(serde::Deserialize)]
struct Credentials {
    /// nKey public key string.
    nkey: Option<String>,
    /// Optional host value, if not passed localhost:4222 will be used.
    host: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("missing required attributes")]
    MissingRequiredAttribute(String),
    #[error("cannot open the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("error parsing credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("error parsing provided url")]
    ParseUrl(#[source] url::ParseError),
    #[error("error connecting to NATS")]
    NatsConnect(#[source] async_nats::ConnectError),
    #[error("credentials are not provided")]
    CredentialsNotProvided(),
}

#[derive(Debug)]
pub struct Client {
    credentials_path: PathBuf,
    pub jetstream: Option<async_nats::jetstream::Context>,
}

impl flowgen_core::connect::client::Client for Client {
    type Error = Error;
    /// Connect to the NATS Server with provided options.
    async fn connect(mut self) -> Result<Self, Error> {
        let credentials_string = fs::read_to_string(&self.credentials_path)
            .map_err(|e| Error::OpenFile(e, self.credentials_path.to_owned()))?;

        let credentials: Credentials =
            serde_json::from_str(&credentials_string).map_err(Error::ParseCredentials)?;

        let mut connect_options = async_nats::ConnectOptions::new();
        if let Some(configured_nkey) = credentials.nkey {
            connect_options = async_nats::ConnectOptions::with_nkey(configured_nkey);
        }

        let mut host = DEFAULT_NATS_HOST.to_string();
        if let Some(configured_host) = credentials.host {
            host = configured_host.clone();
        }

        let nats_client = connect_options
            .connect(host)
            .await
            .map_err(Error::NatsConnect)?;

        let jetstream = async_nats::jetstream::new(nats_client);

        self.jetstream = Some(jetstream);
        Ok(self)
    }
}

#[derive(Default)]
/// Configuration options of the client.
pub struct ClientBuilder {
    credentials_path: Option<PathBuf>,
}

impl ClientBuilder {
    /// Creates a new instance of a client builder and allows various configurartion of the client.
    pub fn new() -> Self {
        ClientBuilder::default()
    }
    /// Pass credentials file as path to the file.
    pub fn credentials_path(&mut self, credentials_path: PathBuf) -> &mut ClientBuilder {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Generates a new NATS Client or return error in case provided credentials are not valid.
    pub fn build(&self) -> Result<Client, Error> {
        Ok(Client {
            credentials_path: self
                .credentials_path
                .clone()
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials_path".to_string()))?,

            jetstream: None,
        })
    }
}
