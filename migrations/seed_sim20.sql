-- 20 IED untuk uji koneksi serentak (docker-compose.sim20.yml). HANYA DB lokal/dev.
-- Tanpa titik termapping: yang diuji hanya koneksi + RCB aktif.
-- Hapus: DELETE FROM iec61850_report_controls WHERE id_rtu BETWEEN 9101 AND 9120;
--        DELETE FROM iec61850s WHERE id_rtu BETWEEN 9101 AND 9120;
BEGIN;

INSERT INTO iec61850s (id_rtu, ip_address, port, acquisition_mode)
SELECT 9100 + n, 'ied-sim-' || (100 + n), 102, 'REPORT' FROM generate_series(1, 20) AS n;

INSERT INTO iec61850_report_controls (id_rtu, rcb_reference, trg_ops, intg_pd_ms)
SELECT 9100 + n, 'simpleIOGenericIO/LLN0.BR.EventsBRCB01', 'dchg,qchg,integrity,gi', 10000
FROM generate_series(1, 20) AS n;

COMMIT;
