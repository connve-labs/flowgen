pub mod config {
    pub mod input;
}
pub mod stream {
    pub mod event;
    pub mod publisher;
}
pub mod convert {
    pub mod recordbatch;
    pub mod render;
    pub mod serde;
}
pub mod connect {
    pub mod client;
    pub mod service;
}
pub mod task {
    pub mod enumerate {
        pub mod config;
        pub mod processor;
    }
    pub mod generate {
        pub mod config;
        pub mod subscriber;
    }
}
