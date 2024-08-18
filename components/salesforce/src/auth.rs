use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct Client {
    pub client_id: String,
    pub client_secret: String,
    pub auth_url: String,
    pub token_url: String,
}

impl Client {
    pub fn new() -> ClientBuilder {
        ClientBuilder::default()
    }
    pub fn connect(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    credentials_path: String,
}

impl ClientBuilder {
    pub fn with_credentials_path(mut self, credentials_path: String) -> ClientBuilder {
        self.credentials_path = credentials_path;
        self
    }

    pub fn build(self) -> Result<Client, Box<dyn std::error::Error>> {
        let creds = fs::read_to_string(&self.credentials_path)?;
        let client: Client = serde_json::from_str(&creds)?;
        Ok(client)
    }
}
