//! FEP IEC 61850: muat konfigurasi dari Postgres, satu thread per IED (report / polling),
//! publish ke Kafka dengan format yang sama seperti FEP IEC 104, dan dengarkan event Kafka.

use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

use fep_iec61850::config::{DatabaseConfig, KafkaConfig, RuntimeConfig};
use fep_iec61850::handlers;
use fep_iec61850::kafka::{consumer, KafkaProducer};
use fep_iec61850::manager::Manager;
use fep_iec61850::publisher::Publisher;
use fep_iec61850::repository as repo;
use fep_iec61850::resolver::{FormulaResolver, GeneralActionResolver, GroupResolver, TagResolver};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Err(e) = run().await {
        error!("{e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    info!("FEP IEC 61850 (MMS client) mulai");
    let kafka_cfg = KafkaConfig::from_env()?;
    let runtime = RuntimeConfig::from_env();
    let pool = repo::create_pool(&DatabaseConfig::from_env()?)?;

    let tags = TagResolver::new();
    let formulas = FormulaResolver::new();
    let groups = GroupResolver::new();
    let general_actions = GeneralActionResolver::new();
    {
        let c = pool.get().await.map_err(|e| format!("gagal konek DB: {e}"))?;
        tags.load(repo::load_tags(&c).await?);
        tags.load_manual_set(repo::load_manual_sets(&c).await?);
        formulas.load(repo::load_formulas(&c).await?);
        groups.load(repo::load_groups(&c).await?);
        general_actions.load(repo::load_general_actions(&c).await?);
    }
    info!(
        "resolver termuat: {} tag, {} formula, {} group, {} general action",
        tags.get_count(),
        formulas.get_count(),
        groups.get_count(),
        general_actions.get_count()
    );

    let publisher = Arc::new(Publisher {
        producer: KafkaProducer::new(&kafka_cfg)?,
        tags,
        formulas,
        groups,
        general_actions,
    });

    let mut events = consumer::start(&kafka_cfg, &runtime.subscribe_topics)?;
    let mgr = Arc::new(Manager::new(pool, Arc::clone(&publisher), runtime));
    let started = mgr.start_all().await?;
    info!("{started} IED dijalankan");

    tokio::spawn(Arc::clone(&mgr).heartbeat_loop());
    tokio::spawn(Arc::clone(&mgr).ping_loop());
    let event_mgr = Arc::clone(&mgr);
    let event_task = tokio::spawn(async move {
        while let Some(ev) = events.recv().await {
            handlers::handle(&event_mgr, ev).await;
        }
    });

    publisher.producer.send_system_status(1);
    info!("FEP berjalan, menunggu sinyal berhenti");
    shutdown_signal().await;

    info!("berhenti...");
    event_task.abort();
    // Tunggu semua thread IED berhenti (maks SHUTDOWN_WAIT_SECS) + RTU_STATUS=0 semua IED, baru flush.
    let stopped = mgr.stop_all(Duration::from_secs(mgr.runtime.shutdown_wait_secs)).await;
    info!("{} IED dihentikan, RTU_STATUS=0 dikirim", stopped.len());
    publisher.producer.send_system_status(0);
    publisher.producer.flush(Duration::from_secs(5));
    Ok(())
}

/// Ctrl+C atau SIGTERM (Kubernetes).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
