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

/// Parameter runtime dari env (nama & default sama dengan FEP IEC 104).
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub max_concurrent_connects: usize,
    pub status_heartbeat_interval_secs: u64,
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub ping_attempts: u16,
    pub ping_retry_delay_ms: u64,
    pub ping_resend_every: u32,
    pub ping_concurrency: usize,
    /// Reload per id_rtu dari MASTER_DATAPOINT_EVENT / CDC ditunda & digabung selama ini.
    pub reload_debounce_ms: u64,
    /// Batas tunggu semua thread IED berhenti saat shutdown, sebelum flush Kafka.
    pub shutdown_wait_secs: u64,
    pub subscribe_topics: Vec<String>,
}

const DEFAULT_TOPICS: &str = "RTU_EVENTS,RTU_COMMAND_CONFIG_REQUEST,MASTER_DATAPOINT_EVENT,MASTER_DATA_OBJECT_EVENT,CAPTURE_DATA_CHANGE,MASTER_RTU_EVENTS,MANUAL_SET_EVENT";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl RuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            max_concurrent_connects: env_or("MAX_CONCURRENT_CONNECTS", 200).max(1),
            status_heartbeat_interval_secs: env_or("STATUS_HEARTBEAT_INTERVAL_SECS", 10).max(1),
            ping_interval_ms: env_or("PING_INTERVAL_MS", 5000).max(100),
            ping_timeout_ms: env_or("PING_TIMEOUT_MS", 2000),
            ping_attempts: env_or("PING_ATTEMPTS", 3).max(1),
            ping_retry_delay_ms: env_or("PING_RETRY_DELAY_MS", 200),
            ping_resend_every: env_or("PING_RESEND_EVERY", 6),
            ping_concurrency: env_or("PING_CONCURRENCY", 500).max(1),
            reload_debounce_ms: env_or("RELOAD_DEBOUNCE_MS", 2000),
            shutdown_wait_secs: env_or("SHUTDOWN_WAIT_SECS", 5),
            subscribe_topics: env::var("SUBSCRIBE_TOPICS")
                .unwrap_or_else(|_| DEFAULT_TOPICS.into())
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(String::from)
                .collect(),
        }
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
