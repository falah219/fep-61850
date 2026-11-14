-- Data contoh untuk 2 simulator IED (docker-compose.sim.yml). HANYA untuk DB lokal/dev.
--
-- Rentang IOA virtual per jenis titik (usulan):
--   1001..  status      (FC ST)
--   2001..  measurement (FC MX)
--   3001..  kontrol     (FC CO)
--
-- CATATAN: insert ke master_datapoints hanya mengisi kolom yang dipakai FEP. Kolom hierarki
-- (id_station, id_level_voltage, ...) dibiarkan NULL. created_at/updated_at diisi manual karena
-- tabel buatan Sequelize tidak punya default untuk kolom itu.

BEGIN;

INSERT INTO iec61850s (id_rtu, ip_address, port, acquisition_mode, poll_interval_ms, general_interrogation_interval)
VALUES (9001, 'ied-sim-1', 102, 'REPORT',  2000, 300),
       (9002, 'ied-sim-2', 102, 'POLLING', 2000, 300);

INSERT INTO iec61850_report_controls (id_rtu, rcb_reference, trg_ops, intg_pd_ms)
VALUES (9001, 'simpleIOGenericIO/LLN0.BR.EventsBRCB01',   'dchg,qchg,integrity,gi', 10000),
       (9001, 'simpleIOGenericIO/LLN0.BR.Measurements01', 'integrity,gi',           5000);

-- (id_rtu, ioa, tag, object_reference, fc, cdc, deadband)
CREATE TEMP TABLE seed_points (
    id_rtu INTEGER, ioa INTEGER, tag TEXT, object_reference TEXT, fc TEXT, cdc TEXT, deadband DOUBLE PRECISION
) ON COMMIT DROP;

INSERT INTO seed_points
SELECT r.id_rtu, p.ioa, format('SIM%s_%s', r.id_rtu - 9000, p.suffix), p.object_reference, p.fc, p.cdc, p.deadband
FROM (VALUES (9001), (9002)) AS r (id_rtu)
CROSS JOIN (VALUES
    (1001, 'SPCSO1_ST', 'simpleIOGenericIO/GGIO1.SPCSO1', 'ST', 'SPC', NULL::DOUBLE PRECISION),
    (1002, 'SPCSO2_ST', 'simpleIOGenericIO/GGIO1.SPCSO2', 'ST', 'SPC', NULL),
    (1003, 'SPCSO3_ST', 'simpleIOGenericIO/GGIO1.SPCSO3', 'ST', 'SPC', NULL),
    (1004, 'SPCSO4_ST', 'simpleIOGenericIO/GGIO1.SPCSO4', 'ST', 'SPC', NULL),
    (1011, 'IND1',      'simpleIOGenericIO/GGIO1.Ind1',   'ST', 'SPS', NULL),
    (1012, 'IND2',      'simpleIOGenericIO/GGIO1.Ind2',   'ST', 'SPS', NULL),
    (1013, 'IND3',      'simpleIOGenericIO/GGIO1.Ind3',   'ST', 'SPS', NULL),
    (1014, 'IND4',      'simpleIOGenericIO/GGIO1.Ind4',   'ST', 'SPS', NULL),
    (2001, 'ANIN1',     'simpleIOGenericIO/GGIO1.AnIn1',  'MX', 'MV',  0.05),
    (2002, 'ANIN2',     'simpleIOGenericIO/GGIO1.AnIn2',  'MX', 'MV',  0.05),
    (2003, 'ANIN3',     'simpleIOGenericIO/GGIO1.AnIn3',  'MX', 'MV',  0.05),
    (2004, 'ANIN4',     'simpleIOGenericIO/GGIO1.AnIn4',  'MX', 'MV',  0.05),
    (3001, 'SPCSO1_CO', 'simpleIOGenericIO/GGIO1.SPCSO1', 'CO', 'SPC', NULL),
    (3002, 'SPCSO2_CO', 'simpleIOGenericIO/GGIO1.SPCSO2', 'CO', 'SPC', NULL),
    (3003, 'SPCSO3_CO', 'simpleIOGenericIO/GGIO1.SPCSO3', 'CO', 'SPC', NULL),
    (3004, 'SPCSO4_CO', 'simpleIOGenericIO/GGIO1.SPCSO4', 'CO', 'SPC', NULL)
) AS p (ioa, suffix, object_reference, fc, cdc, deadband);

-- Dua statement terpisah (bukan CTE) supaya trigger unik bisa melihat baris master_datapoints.
INSERT INTO master_datapoints (id, id_rtu, ioa, tag, protocol, is_deleted, created_at, updated_at)
SELECT gen_random_uuid(), id_rtu, ioa, tag, 'IEC61850', FALSE, now(), now() FROM seed_points;

INSERT INTO master_datapoint_iec61850s (id_master_datapoint, object_reference, functional_constraint, cdc, deadband)
SELECT md.id, s.object_reference, s.fc, s.cdc, s.deadband
FROM seed_points s
JOIN master_datapoints md
  ON md.id_rtu = s.id_rtu AND md.ioa = s.ioa AND md.protocol = 'IEC61850' AND md.is_deleted = FALSE;

COMMIT;
