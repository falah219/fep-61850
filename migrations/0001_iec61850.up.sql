-- =====================================================================
-- USULAN skema FEP IEC 61850 (docs/IEC61850_PLAN.md bagian 3).
-- HARUS DI-REVIEW TIM sebelum dipakai di lingkungan bersama.
-- Diuji di PostgreSQL 16 kosong bersama stub backend (test-env/sql/00_stub_backend.sql -> 0001).
--
-- Konvensi diambil dari model Sequelize iec104s (iconics-configuration-master):
--   PK `id` uuid, `id_rtu` integer, `protocol`, `longitude_x`/`latitude_y` varchar,
--   `status_rtu` varchar ('green'/'red'), `channel_state` boolean, `id_station` -> master_stations,
--   `id_loc_rtu` -> data_gi, `rtu_online_at`/`rtu_offline_at` varchar, timestamp underscored.
--
-- Keputusan (dikonfirmasi):
--   - Belum ada tabel backend untuk form "Add Client" IEC 61850 -> tabel baru iec61850s.
--   - reconnect_retry_count = 0 berarti reconnect tanpa batas.
--   - Kolom di luar form GRITA (acquisition_mode, poll_interval_ms,
--     general_interrogation_interval, request_timeout_ms) tetap jadi kolom DB dengan default.
--
-- Masih terbuka:
--   1. Cara simpan password ACSE belum ditentukan -> hanya kolom password_ref (referensi, BUKAN password).
--   2. Lokasi file SCL (DB / object storage / volume) -> hanya simpan nama file.
--   3. AE/AP Invoke ID: libIEC61850 v1.6.2.1 TIDAK punya setter -> disimpan, tidak dikirim ke IED.
--   4. id_rtu dibuat UNIQUE (target FK iec61850_report_controls). RTU yang di-soft-delete tetap
--      memakai id_rtu-nya.
--   5. Index unik parsial baru di master_datapoints (tabel bersama).
--   6. Tabel baru perlu ikut dipantau CDC (CAPTURE_DATA_CHANGE) dan telco_statuses
--      (lihat 0002_iec61850_telco_status.sql).
-- =====================================================================

BEGIN;

-- 3.1 Koneksi IED (form "Add Client" IEC 61850 di GRITA, padanan iec104s)
CREATE TABLE iec61850s (
    id                              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    id_rtu                          INTEGER      NOT NULL UNIQUE,                       -- RTU ID
    protocol                        VARCHAR(255) NOT NULL DEFAULT 'IEC61850',
    id_station                      UUID         REFERENCES master_stations (id),       -- Station Name
    id_loc_rtu                      UUID         REFERENCES data_gi (id),
    loc_name_rtu                    VARCHAR(255),
    longitude_x                     VARCHAR(255),                                       -- RTU Location
    latitude_y                      VARCHAR(255),                                       -- RTU Location
    state_rtu                       BOOLEAN      NOT NULL DEFAULT TRUE,                 -- RTU State
    ip_address                      VARCHAR(255) NOT NULL,                              -- Server IP Address
    port                            INTEGER      NOT NULL DEFAULT 102                   -- Server Port Number
                                    CHECK (port BETWEEN 1 AND 65535),
    local_ip_address                VARCHAR(255) NOT NULL DEFAULT '0.0.0.0',            -- Client IP Address

    -- Parameter ACSE / OSI
    ae_invoke_id                    INTEGER      NOT NULL DEFAULT 10,                   -- AE Invoke ID
    ae_qualifier                    INTEGER      NOT NULL DEFAULT 12,                   -- AE Qualifier
    ap_invoke_id                    INTEGER      NOT NULL DEFAULT 10,                   -- AP Invoke ID
    ap_title                        VARCHAR(64)  NOT NULL DEFAULT '1.1.999.1'           -- Application ID
                                    CHECK (ap_title ~ '^[0-9]+(\.[0-9]+)+$'),
    p_selector                      VARCHAR(32)  NOT NULL DEFAULT '00 00 00 01'         -- Presentation Selector
                                    CHECK (p_selector ~* '^[0-9a-f]{2}( [0-9a-f]{2}){0,15}$'),
    s_selector                      VARCHAR(32)  NOT NULL DEFAULT '00 01'               -- Session Selector
                                    CHECK (s_selector ~* '^[0-9a-f]{2}( [0-9a-f]{2}){0,15}$'),
    t_selector                      VARCHAR(32)  NOT NULL DEFAULT '00 01'               -- Transport Selector
                                    CHECK (t_selector ~* '^[0-9a-f]{2}( [0-9a-f]{2}){0,3}$'),

    goose_interface                 VARCHAR(64),                                        -- GOOSE Adapter (fitur lanjutan)
    scl_file_name                   VARCHAR(255),                                       -- SCL File
    use_scl_file                    BOOLEAN      NOT NULL DEFAULT FALSE,                -- Use SCL File
    ied_name                        VARCHAR(64),                                        -- IED Name (dalam SCL)

    reconnect_time_ms               INTEGER      NOT NULL DEFAULT 5000                  -- Reconnect Time (ms)
                                    CHECK (reconnect_time_ms > 0),
    reconnect_retry_count           INTEGER      NOT NULL DEFAULT 0                     -- Reconnect Re-try Count
                                    CHECK (reconnect_retry_count >= 0),                 -- 0 = tanpa batas
    connect_timeout_ms              INTEGER      NOT NULL DEFAULT 10000                 -- Connect Timeout (ms)
                                    CHECK (connect_timeout_ms > 0),

    use_password                    BOOLEAN      NOT NULL DEFAULT FALSE,                -- Security -> Password
    password_ref                    VARCHAR(255),                                       -- referensi secret, BUKAN password
    use_certificate                 BOOLEAN      NOT NULL DEFAULT FALSE,                -- Security -> Certificate (fitur lanjutan)

    -- Tidak ada di form GRITA (kolom DB + default)
    acquisition_mode                VARCHAR(16)  NOT NULL DEFAULT 'REPORT'
                                    CHECK (acquisition_mode IN ('REPORT', 'POLLING', 'HYBRID')),
    poll_interval_ms                INTEGER      NOT NULL DEFAULT 2000  CHECK (poll_interval_ms > 0),
    -- detik; 0 = hanya GI awal setelah konek (tanpa GI berkala)
    general_interrogation_interval  INTEGER      NOT NULL DEFAULT 300   CHECK (general_interrogation_interval >= 0),
    request_timeout_ms              INTEGER      NOT NULL DEFAULT 5000  CHECK (request_timeout_ms > 0),

    -- Sama seperti iec104s, supaya handler RTU_EVENTS & telco_statuses bisa dipakai ulang
    status_rtu                      VARCHAR(255) NOT NULL DEFAULT 'red',
    channel_number                  INTEGER      NOT NULL DEFAULT 1,
    channel_state                   BOOLEAN      NOT NULL DEFAULT TRUE,
    rtu_online_at                   VARCHAR(255),
    rtu_offline_at                  VARCHAR(255),

    is_deleted                      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at                      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CHECK (NOT use_password OR password_ref IS NOT NULL),
    CHECK (NOT use_scl_file OR scl_file_name IS NOT NULL)
);

-- 3.2 Report Control Block (hanya mode REPORT / HYBRID).
-- Kolom NULL = TIDAK ditulis ke IED (pakai konfigurasi dari engineering tool vendor).
CREATE TABLE iec61850_report_controls (
    id                 BIGSERIAL    PRIMARY KEY,
    id_rtu             INTEGER      NOT NULL REFERENCES iec61850s (id_rtu) ON UPDATE CASCADE ON DELETE CASCADE,
    -- contoh BAY1LD0/LLN0.BR.brcbST01 (BRCB) atau BAY1LD0/LLN0.RP.urcbMX01 (URCB)
    rcb_reference      VARCHAR(255) NOT NULL CHECK (rcb_reference ~ '\.(BR|RP)\.'),
    dataset_reference  VARCHAR(255),
    -- contoh 'dchg,qchg,integrity,gi'
    trg_ops            VARCHAR(64)
                       CHECK (trg_ops ~ '^(dchg|qchg|dupd|integrity|period|gi)(,(dchg|qchg|dupd|integrity|period|gi))*$'),
    intg_pd_ms         INTEGER      CHECK (intg_pd_ms >= 0),
    buf_tm_ms          INTEGER      CHECK (buf_tm_ms >= 0),
    enabled            BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (id_rtu, rcb_reference)
);

-- 3.3 Ekstensi 1:1 master_datapoints untuk titik IEC 61850
CREATE TABLE master_datapoint_iec61850s (
    id_master_datapoint    UUID         PRIMARY KEY REFERENCES master_datapoints (id) ON DELETE CASCADE,
    -- level Data Object, contoh BAY1LD0/XCBR1.Pos
    object_reference       VARCHAR(255) NOT NULL,
    functional_constraint  VARCHAR(2)   NOT NULL CHECK (functional_constraint IN ('ST', 'MX', 'CO', 'SP')),
    cdc                    VARCHAR(8)   NOT NULL
                           CHECK (cdc IN ('SPS','DPS','INS','ENS','ENC','ACT','ACD','MV','CMV','BCR',
                                          'SPC','DPC','INC','APC','BSC')),
    -- NULL = default dari CDC (stVal / general / mag.f / cVal.mag.f / actVal)
    value_attribute        VARCHAR(64),
    -- hanya measurement di mode polling
    deadband               DOUBLE PRECISION CHECK (deadband >= 0),
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 3.3b Setting kontrol per titik, pola iec104_command_formulas (remote control backend GRITA).
-- Pengganti kolom ctl_model/interlock_check/synchro_check di master_datapoint_iec61850s.
-- sboTimeout tidak disimpan (pakai nilai IED); batas tunggu CommandTermination = env OPER_TIMEOUT_MS.
CREATE TABLE iec61850_command_formulas (
    id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    id_master_datapoint  UUID         NOT NULL REFERENCES master_datapoints (id) ON DELETE CASCADE,
    type                 VARCHAR(8)   NOT NULL CHECK (type IN ('SPC', 'DPC', 'INC', 'ENC', 'BSC', 'APC')),
    -- NULL = dibaca otomatis dari IED (ctlModel, FC CF)
    ctl_model            VARCHAR(40)
                         CHECK (ctl_model IN ('status-only', 'direct-with-normal-security', 'sbo-with-normal-security',
                                              'direct-with-enhanced-security', 'sbo-with-enhanced-security')),
    -- disimpan untuk backend; FEP memakai value dari payload COMMAND
    value                VARCHAR(64),
    interlock_check      BOOLEAN      NOT NULL DEFAULT FALSE,
    synchro_check        BOOLEAN      NOT NULL DEFAULT FALSE,
    is_deleted           BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Maks satu formula aktif per titik (FEP join by id_master_datapoint).
CREATE UNIQUE INDEX ux_iec61850_command_formulas_datapoint
    ON iec61850_command_formulas (id_master_datapoint)
    WHERE is_deleted = FALSE;

-- UNIQUE (id_rtu, object_reference, functional_constraint): id_rtu ada di master_datapoints,
-- jadi tidak bisa pakai constraint biasa. Dicek lewat trigger + join.
CREATE FUNCTION iec61850_check_unique_reference() RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM master_datapoint_iec61850s x
        JOIN master_datapoints md  ON md.id  = x.id_master_datapoint
        JOIN master_datapoints cur ON cur.id = NEW.id_master_datapoint
        WHERE x.id_master_datapoint <> NEW.id_master_datapoint
          AND md.id_rtu = cur.id_rtu
          AND md.is_deleted = FALSE
          AND x.object_reference = NEW.object_reference
          AND x.functional_constraint = NEW.functional_constraint
    ) THEN
        RAISE EXCEPTION 'object_reference % [%] sudah dipakai di RTU yang sama',
            NEW.object_reference, NEW.functional_constraint;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_iec61850_unique_reference
    BEFORE INSERT OR UPDATE OF id_master_datapoint, object_reference, functional_constraint
    ON master_datapoint_iec61850s
    FOR EACH ROW EXECUTE FUNCTION iec61850_check_unique_reference();

-- IOA virtual unik per RTU untuk titik IEC 61850 (titik yang sudah di-soft-delete tidak dihitung)
CREATE UNIQUE INDEX ux_master_datapoints_iec61850_rtu_ioa
    ON master_datapoints (id_rtu, ioa)
    WHERE protocol = 'IEC61850' AND is_deleted = FALSE;

-- CDC (iconics_configuration_cdc_listener): notify_table_change() milik backend, harus sudah ada.
-- Nama trigger sama dengan yang dibuat listener (<tabel>_notify_trigger) supaya tidak dobel.
CREATE TRIGGER iec61850s_notify_trigger
    AFTER INSERT OR DELETE OR UPDATE ON iec61850s
    FOR EACH ROW EXECUTE FUNCTION notify_table_change();
CREATE TRIGGER iec61850_report_controls_notify_trigger
    AFTER INSERT OR DELETE OR UPDATE ON iec61850_report_controls
    FOR EACH ROW EXECUTE FUNCTION notify_table_change();
CREATE TRIGGER master_datapoint_iec61850s_notify_trigger
    AFTER INSERT OR DELETE OR UPDATE ON master_datapoint_iec61850s
    FOR EACH ROW EXECUTE FUNCTION notify_table_change();
CREATE TRIGGER iec61850_command_formulas_notify_trigger
    AFTER INSERT OR DELETE OR UPDATE ON iec61850_command_formulas
    FOR EACH ROW EXECUTE FUNCTION notify_table_change();

COMMIT;
