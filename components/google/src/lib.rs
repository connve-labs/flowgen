#[path = ""]
pub mod storage {
    #[path = "google.storage.v2.rs"]
    pub mod v2;
    pub const ENDPOINT: &'static str = "https://storage.googleapis.com";
}

#[path = ""]
pub mod iam {
    #[path = "google.iam.v1.rs"]
    pub mod v1;
}

#[path = "google.r#type.rs"]
pub mod r#type;
