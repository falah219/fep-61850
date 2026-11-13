//! Konfigurasi dari env. Nama variabel sama dengan FEP IEC 104.

use std::env;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub sasl_username: String,
    pub sasl_password: String,
    pub client_id: String,
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
        })
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
        if self.security_protocol.contains("SASL") {
            c.push(("sasl.mechanism", self.sasl_mechanism.clone()));
            c.push(("sasl.username", self.sasl_username.clone()));
            c.push(("sasl.password", self.sasl_password.clone()));
        }
        if self.security_protocol.contains("SSL") {
            c.push(("ssl.endpoint.identification.algorithm", "none".into()));
            c.push(("enable.ssl.certificate.verification", "false".into()));
        }
        c
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl DatabaseConfig {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            host: env::var("POSTGRE_HOST").unwrap_or_else(|_| "localhost".into()),
            port: env::var("POSTGRE_PORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()
                .map_err(|_| "POSTGRE_PORT tidak valid".to_string())?,
            user: env::var("DB_USERNAME").unwrap_or_else(|_| "postgres".into()),
            password: env::var("DB_PASSWORD").unwrap_or_default(),
            dbname: env::var("DB_DATABASE").unwrap_or_else(|_| "ICONICS_DEV".into()),
        })
    }
}
