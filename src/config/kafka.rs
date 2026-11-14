//! Konfigurasi Kafka (producer & consumer), sama dengan `config/kafka.rs` FEP IEC 104.

use std::env;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub sasl_username: String,
    pub sasl_password: String,
    pub client_id: String,
    /// HARUS beda dengan FEP IEC 104 supaya kedua FEP menerima semua pesan.
    pub group_id: String,
    pub auto_offset_reset: String,
}

impl KafkaConfig {
    pub fn from_env() -> Result<Self, String> {
        let need = |k: &str| env::var(k).map_err(|_| format!("env {k} wajib diisi"));
        let security_protocol = env::var("SECURITY_PROTOCOL").unwrap_or_else(|_| "PLAINTEXT".into());
        let sasl = security_protocol.contains("SASL");
        Ok(Self {
            bootstrap_servers: need("BOOTSTRAP_SERVER")?,
            sasl_mechanism: if sasl { need("SASL_MECANISM")? } else { String::new() },
            sasl_username: if sasl { need("SASL_USERNAME")? } else { String::new() },
            sasl_password: if sasl { need("SASL_PASSWORD")? } else { String::new() },
            security_protocol,
            client_id: env::var("CLIENT_ID").unwrap_or_else(|_| "iec61850_client".into()),
            group_id: env::var("GROUP_ID").unwrap_or_else(|_| "IEC61850_GROUP".into()),
            auto_offset_reset: env::var("AUTO_OFFSET_RESET").unwrap_or_else(|_| "latest".into()),
        })
    }

    fn security(&self, c: &mut Vec<(&'static str, String)>) {
        if self.security_protocol.contains("SASL") {
            c.push(("sasl.mechanism", self.sasl_mechanism.clone()));
            c.push(("sasl.username", self.sasl_username.clone()));
            c.push(("sasl.password", self.sasl_password.clone()));
        }
        if self.security_protocol.contains("SSL") {
            c.push(("ssl.endpoint.identification.algorithm", "none".into()));
            c.push(("enable.ssl.certificate.verification", "false".into()));
        }
    }

    /// Sama dengan `KafkaConfig::to_consumer_config` di FEP IEC 104.
    pub fn to_consumer_config(&self) -> Vec<(&'static str, String)> {
        let mut c = vec![
            ("bootstrap.servers", self.bootstrap_servers.clone()),
            ("security.protocol", self.security_protocol.clone()),
            ("group.id", self.group_id.clone()),
            ("client.id", self.client_id.clone()),
            ("enable.auto.commit", "false".into()),
            ("auto.offset.reset", self.auto_offset_reset.clone()),
            ("session.timeout.ms", "45000".into()),
            ("heartbeat.interval.ms", "5000".into()),
            ("max.poll.interval.ms", "900000".into()),
            ("isolation.level", "read_committed".into()),
            ("fetch.min.bytes", "1".into()),
            ("fetch.max.bytes", "52428800".into()),
            ("fetch.wait.max.ms", "100".into()),
            ("socket.keepalive.enable", "true".into()),
            ("max.partition.fetch.bytes", "1048576".into()),
            ("enable.partition.eof", "false".into()),
            ("queued.min.messages", "1".into()),
            ("queued.max.messages.kbytes", "65536".into()),
        ];
        self.security(&mut c);
        c
    }

    /// Sama dengan `KafkaConfig::to_producer_config` di FEP IEC 104.
    pub fn to_producer_config(&self) -> Vec<(&'static str, String)> {
        let mut c = vec![
            ("bootstrap.servers", self.bootstrap_servers.clone()),
            ("security.protocol", self.security_protocol.clone()),
            ("client.id", self.client_id.clone()),
            ("message.timeout.ms", "30000".into()),
            ("socket.timeout.ms", "60000".into()),
            ("request.timeout.ms", "30000".into()),
            ("metadata.max.age.ms", "180000".into()),
            ("queue.buffering.max.messages", "500000".into()),
            ("queue.buffering.max.kbytes", "1048576".into()),
            ("batch.num.messages", "10000".into()),
            ("linger.ms", "5".into()),
            ("compression.type", "none".into()),
            ("acks", "1".into()),
        ];
        self.security(&mut c);
        c
    }
}
