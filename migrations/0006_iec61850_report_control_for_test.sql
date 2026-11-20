BEGIN;
-- IED (padanan iec104s). IP = server simulator, port 102 = SIMIED1
INSERT INTO iec61850s (id_rtu, id_station, ip_address, port, acquisition_mode,
poll_interval_ms, general_interrogation_interval)
SELECT 9201, st.id, '192.168.212.25', 102, 'REPORT', 2000, 300
FROM master_stations st WHERE st.code = 'SIMIEC' AND st.is_deleted = FALSE
ON CONFLICT (id_rtu) DO UPDATE
SET id_station = EXCLUDED.id_station, ip_address = EXCLUDED.ip_address,
port = EXCLUDED.port, acquisition_mode = EXCLUDED.acquisition_mode,
is_deleted = FALSE, updated_at = now();
-- Report Control Block (intg_pd_ms = periode INTEGRITY)
INSERT INTO iec61850_report_controls (id_rtu, rcb_reference, trg_ops, intg_pd_ms) VALUES
(9201, 'SIMIED1LD0/LLN0.BR.brcbST01', 'dchg,qchg,integrity,gi', 60000),
(9201, 'SIMIED1LD0/LLN0.BR.brcbMX01', 'dchg,qchg,integrity,gi', 10000)
ON CONFLICT