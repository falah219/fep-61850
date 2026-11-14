//! Handler per event Kafka (pola `kafka/handlers/` FEP IEC 104).

pub(crate) mod capture_data_change;
pub(crate) mod manual_set;
pub(crate) mod master_datapoint;
pub(crate) mod master_rtu_event;
pub(crate) mod rtu_event;
