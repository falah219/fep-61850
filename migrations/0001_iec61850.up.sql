-- =====================================================================
-- USULAN skema FEP IEC 61850 (docs/IEC61850_PLAN.md bagian 3).
-- HARUS DI-REVIEW TIM sebelum dipakai di lingkungan bersama.
-- Belum diuji ke Postgres mana pun.
--
-- Hal yang perlu dikonfirmasi saat review:
--   1. iec61850s.id_rtu dijadikan PRIMARY KEY. Apakah id_rtu di iec104s juga unik / PK,
--      atau mengacu ke tabel master RTU lain?
--   2. Tipe state_rtu (boolean), status_rtu & channel_state (text) disamakan dengan
--      cara FEP IEC 104 membacanya; cek tipe asli di iec104s.
--   3. Index unik parsial baru di master_datapoints (tabel bersama).
--   4. Tabel baru perlu ikut dipantau CDC (CAPTURE_DATA_CHANGE).
-- =====================================================================

BEGIN;

-- 3.1 Koneksi IED (padanan iec104s)
CREATE TABLE iec61850s (
    id_rtu                          INTEGER PRIMARY KEY,
    ip_address                      VARCHAR(64)  NOT NULL,
    port                            INTEGER      NOT NULL DEFAULT 102 CHECK (port BETWEEN 1 AND 65535),
    state_rtu                       BOOLEAN,
    status_rtu                      VARCHAR(32),
    channel_number                  INTEGER,
    channel_state                   VARCHAR(32),
    acquisition_mode                VARCHAR(16)  NOT NULL DEFAULT 'REPORT'
                                    CHECK (acquisition_mode IN ('REPORT', 'POLLING', 'HYBRID')),
    poll_interval_ms                INTEGER      DEFAULT 2000  CHECK (poll_interval_ms > 0),
    general_interrogation_interval  INTEGER      DEFAULT 300   CHECK (general_interrogation_interval > 0),
    connect_timeout_ms              INTEGER      DEFAULT 10000 CHECK (connect_timeout_ms > 0),
    request_timeout_ms              INTEGER      DEFAULT 5000  CHECK (request_timeout_ms > 0),
    -- nama secret Kubernetes, BUKAN password asli
    auth_password_ref               VARCHAR(128),
    is_deleted                      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at                      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 3.2 Report Control Block (hanya mode REPORT / HYBRID).
-- Kolom NULL = TIDAK ditulis ke IED (pakai konfigurasi dari engineering tool vendor).
CREATE TABLE iec61850_report_controls (
    id                 BIGSERIAL    PRIMARY KEY,
    id_rtu             INTEGER      NOT NULL REFERENCES iec61850s (id_rtu),
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
    id_master_datapoint    UUID         PRIMARY KEY REFERENCES master_datapoints (id),
    -- level Data Object, contoh BAY1LD0/XCBR1.Pos
    object_reference       VARCHAR(255) NOT NULL,
    functional_constraint  VARCHAR(2)   NOT NULL CHECK (functional_constraint IN ('ST', 'MX', 'CO', 'SP')),
    cdc                    VARCHAR(8)   NOT NULL
                           CHECK (cdc IN ('SPS','DPS','INS','ENS','MV','CMV','BCR','SPC','DPC','INC','APC','BSC')),
    -- NULL = default dari CDC (stVal / mag.f / cVal.mag.f / actVal)
    value_attribute        VARCHAR(64),
    -- hanya titik kontrol; NULL = dibaca otomatis dari IED (ctlModel, FC CF)
    ctl_model              VARCHAR(40)
                           CHECK (ctl_model IN ('status-only', 'direct-with-normal-security', 'sbo-with-normal-security',
                                                'direct-with-enhanced-security', 'sbo-with-enhanced-security')),
    sbo_timeout_ms         INTEGER      CHECK (sbo_timeout_ms > 0),
    interlock_check        BOOLEAN      NOT NULL DEFAULT FALSE,
    synchro_check          BOOLEAN      NOT NULL DEFAULT FALSE,
    -- hanya measurement di mode polling
    deadband               DOUBLE PRECISION CHECK (deadband >= 0),
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

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

COMMIT;
