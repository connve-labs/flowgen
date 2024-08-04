#[path = ""]
pub mod eventbus {
    #[path = "eventbus.v1.rs"]
    pub mod v1;
    pub const GLOBAL_ENDPOINT: &str = "https://api.pubsub.salesforce.com";
    pub const DE_ENDPOINT: &str = "https://api.deu.pubsub.salesforce.com";
}
pub mod auth;
