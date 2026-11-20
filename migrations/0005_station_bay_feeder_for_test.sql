BEGIN;
-- Station simulator
INSERT INTO master_stations (id, id_area, name_station, code, category, created_at, updated_at)
SELECT gen_random_uuid(), '3d4d55cd-1e1f-40d8-823c-dcf4f335e7b1',
'GI SIMULATOR IEC61850', 'SIMIEC', 'GH', now(), now()
WHERE NOT EXISTS (SELECT 1 FROM master_stations WHERE code = 'SIMIEC' AND is_deleted = FALSE);
-- Bay feeder
INSERT INTO master_group_bays (id, id_station, bay_feeder, category, created_at, updated_at)
SELECT gen_random_uuid(), st.id, v.nama, 'Bay/Feeder', now(), now()
FROM master_stations st
CROSS JOIN (VALUES ('SIMBAY 1'), ('SIMBAY 2')) v(nama)
WHERE st.code = 'SIMIEC' AND st.is_deleted = FALSE
AND NOT EXISTS (SELECT 1 FROM master_group_bays g
WHERE g.id_station = st.id AND g.bay_feeder = v.nama AND g.is_deleted = FALSE);
-- Cek: 1 station, 2 bay
SELECT s.code, g.bay_feeder FROM master_group_bays g
JOIN master_stations s ON s.id = g.id_station WHERE s.code = 'SIMIEC';
-- COMMIT;