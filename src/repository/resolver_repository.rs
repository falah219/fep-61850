//! Query data resolver (tag, manual set, formula, group, general action): sama dengan FEP IEC 104,
//! tetapi protocol = 'IEC61850'.

use tokio_postgres::Client;

use super::{db_err, DbResult};

pub async fn load_tags(c: &Client) -> DbResult<Vec<(i32, i32, String)>> {
    let rows = c
        .query(
            "SELECT id_rtu, ioa, tag FROM master_datapoints
             WHERE is_deleted = FALSE AND protocol = 'IEC61850' AND tag IS NOT NULL AND tag != ''",
            &[],
        )
        .await
        .map_err(db_err("query tag"))?;
    Ok(rows
        .iter()
        .filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?, r.try_get(2).ok()?)))
        .collect())
}

pub async fn load_manual_sets(c: &Client) -> DbResult<Vec<String>> {
    let rows = c
        .query(
            "SELECT md.tag FROM manual_sets ms JOIN master_datapoints md ON md.id = ms.id_master_datapoint
             WHERE ms.is_manual_set = TRUE AND ms.is_deleted = FALSE AND md.is_deleted = FALSE
               AND md.protocol = 'IEC61850' AND md.tag IS NOT NULL",
            &[],
        )
        .await
        .map_err(db_err("query manual set"))?;
    Ok(rows.iter().filter_map(|r| r.try_get(0).ok()).collect())
}

pub async fn load_formulas(c: &Client) -> DbResult<Vec<(String, String, f64, f64, f64, f64)>> {
    let rows = c
        .query(
            "SELECT md.tag, COALESCE(f.scalling_type, 'No Scale'), COALESCE(f.scale, 1.0), COALESCE(f.offset, 0.0),
                    COALESCE(f.span_low, 0.0), COALESCE(f.span_high, 100.0)
             FROM master_datapoints md JOIN formulas f ON md.id = f.id_datapoint
             WHERE md.is_deleted = FALSE AND f.is_deleted = FALSE AND md.protocol = 'IEC61850'
               AND md.tag IS NOT NULL AND md.tag != ''",
            &[],
        )
        .await
        .map_err(db_err("query formula"))?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            Some((
                r.try_get(0).ok()?,
                r.try_get(1).ok()?,
                r.try_get(2).ok()?,
                r.try_get(3).ok()?,
                r.try_get(4).ok()?,
                r.try_get(5).ok()?,
            ))
        })
        .collect())
}

pub async fn load_groups(c: &Client) -> DbResult<Vec<(String, String)>> {
    let rows = c
        .query(
            "SELECT initial, id_group::TEXT FROM master_data_objects
             WHERE is_deleted = FALSE AND initial IS NOT NULL AND id_group IS NOT NULL",
            &[],
        )
        .await
        .map_err(db_err("query group"))?;
    Ok(rows.iter().filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?))).collect())
}

pub async fn load_general_actions(c: &Client) -> DbResult<Vec<(String, i32)>> {
    let rows = c
        .query(
            "SELECT name_action, code FROM master_general_actions
             WHERE protocol = 'IEC61850' AND is_deleted = FALSE AND name_action IS NOT NULL AND name_action != ''",
            &[],
        )
        .await
        .map_err(db_err("query general action"))?;
    Ok(rows.iter().filter_map(|r| Some((r.try_get(0).ok()?, r.try_get(1).ok()?))).collect())
}

/// id_rtu pemilik sebuah master_datapoints.id (uuid dalam bentuk teks).
pub async fn rtu_of_datapoint(c: &Client, id_master_datapoint: &str) -> DbResult<Option<i32>> {
    let row = c
        .query_opt("SELECT id_rtu FROM master_datapoints WHERE id::TEXT = $1", &[&id_master_datapoint])
        .await
        .map_err(db_err("query id_rtu datapoint"))?;
    Ok(row.and_then(|r| r.try_get(0).ok()))
}
