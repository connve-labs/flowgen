use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Client {
    pub client_id: String,
    pub client_secret: String,
    pub auth_url: String,
    pub token_url: String,
}

impl Client {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

#[derive(Default)]
pub struct Builder {
    credentials_path: String,
}

impl Builder {
    pub fn with_credetentials_path(&mut self, credentials_path: String) -> &mut Self {
        self.credentials_path = credentials_path;
        self
    }

    pub fn build(&self) -> Client {
        let creds = fs::read_to_string(&self.credentials_path)
            .expect("Should have been able to read the file");
        let client: Client = serde_json::from_str(&creds).unwrap();
        client
    }
}
