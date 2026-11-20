BEGIN;
DROP TABLE IF EXISTS ga_map;
CREATE TEMP TABLE ga_map (name_action TEXT, ref104 TEXT);
INSERT INTO ga_map VALUES
('DATA_CHANGE', 'SPONTANEOUS'),
('QUALITY_CHANGE', 'SPONTANEOUS'),
('DATA_UPDATE', 'SPONTANEOUS'),
('GENERAL_INTERROGATION', 'INTERROGATED_BY_STATION'),
('INTEGRITY', 'PERIODIC'),
('POLLING', 'PERIODIC');

-- Salin baris IEC 104 (kolom code ikut tersalin: 3 / 20 / 1)
INSERT INTO master_general_actions
SELECT (jsonb_populate_record(NULL::master_general_actions, to_jsonb(t) || jsonb_build_object(
    'id', gen_random_uuid(), 'protocol', 'IEC61850', 'name_action', g.name_action,
    'created_at', now(), 'updated_at', now()))).*
FROM ga_map g
CROSS JOIN LATERAL (
    SELECT * FROM master_general_actions x
    WHERE x.protocol = 'IEC104' AND x.name_action = g.ref104 AND x.is_deleted = FALSE
    LIMIT 1) t
WHERE NOT EXISTS (SELECT 1 FROM master_general_actions y
    WHERE y.protocol = 'IEC61850' AND y.name_action = g.name_action
    AND y.is_deleted = FALSE);
-- Cek: harus 6 baris, code 3 / 20 / 1
SELECT name_action, code FROM master_general_actions
WHERE protocol = 'IEC61850' AND is_deleted = FALSE ORDER BY code, name_action;
COMMIT;  -- COMMIT; atau ROLLBACK;