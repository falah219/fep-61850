//! Konfigurasi Postgres, sama dengan `config/database.rs` FEP IEC 104.

use std::env;

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
