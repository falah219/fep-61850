//! Kafka: producer, consumer, handler event, publisher (pola `kafka/` FEP IEC 104).
//! Format JSON identik dengan FEP IEC 104 (`kafka/models.rs`).

pub mod consumer;
pub mod event_handler;
pub mod handlers;
pub mod models;
pub mod producer;
pub mod publisher;

pub use models::DataMessage;
pub use producer::KafkaProducer;

pub const TOPIC_RTU_DATAS: &str = "RTU_DATAS";
pub const TOPIC_DATAPOINTS: &str = "DATAPOINTS";
pub const TOPIC_RTU_STATUS: &str = "RTU_STATUS";
pub const TOPIC_RTU_LOGGING: &str = "RTU_LOGGING";
pub const TOPIC_MANUAL_SET_RESPONSE: &str = "MANUAL_SET_EVENT_RESPONSE";
