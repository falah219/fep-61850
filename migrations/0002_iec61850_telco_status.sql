-- USULAN. Sinkronkan iec61850s ke telco_statuses, sama seperti iec104s/dnp3s/... di
-- iconics-configuration-master/migrate-telco-status.sql. HARUS DI-REVIEW TIM.
--
-- Syarat: tabel telco_statuses dan function sync_telco_status() sudah ada.
--
-- PENTING: sync_telco_status() menentukan status online dari daftar nama tabel. Tanpa perubahan,
-- iec61850s jatuh ke cabang ELSE -> status selalu false. Tim backend perlu menambahkan 'iec61850s'
-- ke baris berikut di function itu (di repo backend, bukan di sini, supaya tidak ada dua versi):
--
--     ELSIF TG_TABLE_NAME IN ('iec101s', 'iec104s', 'dnp3s', 'opc_uas', 'iec61850s') THEN

BEGIN;

DROP TRIGGER IF EXISTS trigger_sync_telco_iec61850 ON iec61850s;
CREATE TRIGGER trigger_sync_telco_iec61850
AFTER INSERT OR DELETE OR UPDATE OF id_rtu, protocol, longitude_x, latitude_y, status_rtu, id_station, id_loc_rtu, rtu_online_at, rtu_offline_at, is_deleted ON iec61850s
FOR EACH ROW EXECUTE FUNCTION sync_telco_status();

COMMIT;
