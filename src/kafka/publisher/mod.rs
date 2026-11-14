//! Publish data titik ke RTU_DATAS + DATAPOINTS (pola `kafka/publisher/` FEP IEC 104).

mod message_builder;

pub use message_builder::Publisher;
