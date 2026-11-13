//! FEP IEC 61850 (fase 7): muat konfigurasi dari Postgres, akuisisi tiap IED, publish ke
//! Kafka RTU_DATAS + DATAPOINTS dengan format yang sama seperti FEP IEC 104.
//!
//! Belum ada (fase 8): consumer RTU_EVENTS dkk., RTU_STATUS/RTU_LOGGING, ping, health check,
//! batas koneksi serentak.

use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use tracing::{error, info, warn};

use fep_iec61850::config::{DatabaseConfig, KafkaConfig};
use fep_iec61850::iec61850::acquisition::{run_ied, RtuCtx};
use fep_iec61850::kafka::KafkaProducer;
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
    let db = DatabaseConfig::from_env()?;
    let mut pg = tokio_postgres::Config::new();
    pg.host(&db.host).port(db.port).user(&db.user).password(&db.password).dbname(&db.dbname);
    let (client, conn) = pg.connect(NoTls).await.map_err(|e| format!("gagal konek DB {}:{}: {e}", db.host, db.port))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("koneksi DB error: {e}");
        }
    });

    let rtus = repo::load_rtus(&client).await?;
    let rcbs = repo::load_rcbs(&client).await?;
    let points = repo::load_points(&client).await?;

    let tags = TagResolver::new();
    tags.load(repo::load_tags(&client).await?);
    tags.load_manual_set(repo::load_manual_sets(&client).await?);
    let formulas = FormulaResolver::new();
    formulas.load(repo::load_formulas(&client).await?);
    let groups = GroupResolver::new();
    groups.load(repo::load_groups(&client).await?);
    let general_actions = GeneralActionResolver::new();
    general_actions.load(repo::load_general_actions(&client).await?);
    info!(
        "termuat: {} IED, {} RCB, {} titik, {} tag, {} formula, {} group, {} general action",
        rtus.len(),
        rcbs.len(),
        points.len(),
        tags.get_count(),
        formulas.get_count(),
        groups.get_count(),
        general_actions.get_count()
    );

    let publisher = Arc::new(Publisher {
        producer: KafkaProducer::new(&KafkaConfig::from_env()?)?,
        tags,
        formulas,
        groups,
        general_actions,
    });

    for rtu in rtus {
        let id = rtu.id_rtu;
        let rtu_points: Vec<_> = points.rtu_points(id).cloned().collect();
        if rtu_points.is_empty() {
            warn!(id_rtu = id, "tidak ada titik termapping, IED tetap dikoneksikan");
        }
        let rtu_rcbs = rcbs
            .iter()
            .filter(|(r, _, _)| *r == id)
            .map(|(_, rcb, s)| (rcb.clone(), s.clone()))
            .collect();
        let ctx = Arc::new(RtuCtx::new(rtu, rtu_rcbs, rtu_points, Arc::clone(&publisher)));
        // Satu thread OS per IED: API libIEC61850 blocking, tidak boleh jalan di task async.
        std::thread::Builder::new()
            .name(format!("ied-{id}"))
            .spawn(move || run_ied(ctx))
            .map_err(|e| format!("gagal membuat thread IED {id}: {e}"))?;
    }

    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    info!("berhenti, flush Kafka...");
    publisher.producer.flush(Duration::from_secs(5));
    Ok(())
}
