-- #####################################################################################
-- ##                                                                                 ##
-- ##   HANYA UNTUK DB UJI KOSONG (container PostgreSQL sementara).                   ##
-- ##   JANGAN DIJALANKAN KE DB BERSAMA / KANTOR / PRODUKSI.                          ##
-- ##                                                                                 ##
-- ##   Tabel & fungsi di bawah adalah TIRUAN MINIMAL milik backend                   ##
-- ##   (iconics-configuration-master, iconics_configuration_cdc_listener).           ##
-- ##   Di DB asli objek ini sudah ada dengan kolom lengkap; fungsi di sini KOSONG    ##
-- ##   dan akan menimpa (CREATE OR REPLACE) fungsi CDC / telco yang asli.            ##
-- ##                                                                                 ##
-- #####################################################################################
--
-- Urutan: 00_stub_backend.sql -> migrations/0001_iec61850.up.sql -> 0002 -> seed_*.sql
-- Isi: tabel yang dirujuk FK di 0001 + tabel yang dibaca FEP (resolver) + fungsi trigger.

BEGIN;

-- FK iec61850s.id_station
CREATE TABLE IF NOT EXISTS master_stations (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255),
    is_deleted  BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- FK iec61850s.id_loc_rtu
CREATE TABLE IF NOT EXISTS data_gi (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- FK master_datapoint_iec61850s / iec61850_command_formulas + index unik parsial di 0001
CREATE TABLE IF NOT EXISTS master_datapoints (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    id_rtu          INTEGER,
    protocol        VARCHAR(255),
    id_data_object  UUID,
    id_station      UUID,
    ioa             INTEGER,
    tag             VARCHAR(255),
    description     VARCHAR(255),
    is_deleted      BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Resolver group (initial = CDC)
CREATE TABLE IF NOT EXISTS master_data_objects (
    id           UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol     VARCHAR(255),
    code         INTEGER,
    initial      VARCHAR(255),
    information  VARCHAR(255),
    id_group     UUID,
    is_deleted   BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Resolver general action
CREATE TABLE IF NOT EXISTS master_general_actions (
    id           UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol     VARCHAR(255),
    name_action  VARCHAR(255),
    code         INTEGER,
    is_deleted   BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Resolver formula (kolom "offset" kata kunci SQL -> dikutip)
CREATE TABLE IF NOT EXISTS formulas (
    id             UUID              PRIMARY KEY DEFAULT gen_random_uuid(),
    id_datapoint   UUID,
    scalling_type  VARCHAR(255),
    scale          DOUBLE PRECISION,
    "offset"       DOUBLE PRECISION,
    span_low       DOUBLE PRECISION,
    span_high      DOUBLE PRECISION,
    is_deleted     BOOLEAN           NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ       NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ       NOT NULL DEFAULT now()
);

-- Resolver manual set
CREATE TABLE IF NOT EXISTS manual_sets (
    id                   UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    id_master_datapoint  UUID,
    is_manual_set        BOOLEAN       NOT NULL DEFAULT FALSE,
    is_deleted           BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Trigger CDC di 0001. Asli: iconics_configuration_cdc_listener (pg_notify 'table_change').
CREATE OR REPLACE FUNCTION notify_table_change() RETURNS trigger AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger telco di 0002. Asli: migrate-telco-status.sql backend (isi telco_statuses).
CREATE OR REPLACE FUNCTION sync_telco_status() RETURNS trigger AS $$
BEGIN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMIT;
