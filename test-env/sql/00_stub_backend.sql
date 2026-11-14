-- Stub objek milik backend untuk test env (BUKAN untuk DB bersama / produksi).
-- Jalankan sebelum migrations/0001_iec61850.up.sql.
--
-- notify_table_change() asli dibuat iconics_configuration_cdc_listener (pg_notify 'table_change').
-- Di test env cukup fungsi kosong supaya trigger di migrasi 0001 bisa dibuat.
CREATE OR REPLACE FUNCTION notify_table_change() RETURNS trigger AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
