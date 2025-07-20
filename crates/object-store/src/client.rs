use std::{collections::HashMap, path::PathBuf};

use object_store::{parse_url_opts, path::Path, ObjectStore};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("no path provided")]
    EmptyPath(),
}

pub struct Context {
    pub object_store: Box<dyn ObjectStore>,
    pub path: Path,
}

pub struct Client {
    path: PathBuf,
    credentials: Option<PathBuf>,
    options: Option<HashMap<String, String>>,
    pub context: Option<Context>,
}

impl flowgen_core::connect::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Client, Error> {
        let path = self.path.to_str().ok_or_else(Error::EmptyPath)?;
        let url = Url::parse(path).map_err(Error::ParseUrl)?;
        let mut parse_opts = match &self.options {
            Some(options) => options.clone(),
            None => HashMap::new(),
        };

        if let Some(credentials) = &self.credentials {
            parse_opts.insert(
                "google_service_account".to_string(),
                credentials.to_string_lossy().to_string(),
            );
        }

        let (object_store, path) = parse_url_opts(&url, parse_opts).map_err(Error::ObjectStore)?;
        let context = Context { object_store, path };
        self.context = Some(context);
        Ok(self)
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    path: Option<PathBuf>,
    credentials: Option<PathBuf>,
    pub options: Option<HashMap<String, String>>,
}

impl ClientBuilder {
    pub fn new() -> ClientBuilder {
        ClientBuilder {
            ..Default::default()
        }
    }

    pub fn credentials(mut self, credentials: PathBuf) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = Some(options);
        self
    }
    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            path: self
                .path
                .ok_or_else(|| Error::MissingRequiredAttribute("path".to_string()))?,
            credentials: self.credentials,
            options: self.options,
            context: None,
        })
    }
}
