BEGIN;
-- CDC IEC 61850 -> tipe IEC 104 yang dipakai sebagai template (id_group ikut tersalin)
DROP TABLE IF EXISTS cdc_map;
CREATE TEMP TABLE cdc_map (cdc TEXT, t1 TEXT, t2 TEXT);
INSERT INTO cdc_map VALUES
('SPS','M_SP_TB_1','M_SP_NA_1'), ('DPS','M_DP_TB_1','M_DP_NA_1'),
('INS','M_ME_TE_1','M_ME_NB_1'), ('ENS','M_ME_TE_1','M_ME_NB_1'),
('ACT','M_SP_TB_1','M_SP_NA_1'), ('ACD','M_SP_TB_1','M_SP_NA_1'),
('MV', 'M_ME_TF_1','M_ME_NC_1'), ('CMV','M_ME_TF_1','M_ME_NC_1'),
('BCR','M_IT_TB_1','M_IT_NA_1'), ('SPC','C_SC_NA_1','C_SC_TA_1'),
('DPC','C_DC_NA_1','C_DC_TA_1'), ('INC','C_SE_NB_1','C_SE_TB_1'),
('ENC','C_SE_NB_1','C_SE_TB_1'), ('APC','C_SE_NC_1','C_SE_TC_1'),
('BSC','C_RC_NA_1','C_RC_TA_1');
-- Salin baris IEC 104 sebagai template, ganti id / protocol / initial / code
INSERT INTO master_data_objects
SELECT (jsonb_populate_record(NULL::master_data_objects, to_jsonb(t) || jsonb_build_object(
'id', gen_random_uuid(), 'protocol', 'IEC61850', 'initial', m.cdc,
'code', NULL, 'created_at', now(), 'updated_at', now()))).*
FROM cdc_map m
CROSS JOIN LATERAL (
SELECT * FROM master_data_objects x
WHERE x.protocol = 'IEC104' AND x.initial IN (m.t1, m.t2) AND x.is_deleted = FALSE
ORDER BY (x.initial = m.t1) DESC
LIMIT 1) t
WHERE NOT EXISTS (SELECT 1 FROM master_data_objects y
WHERE y.protocol = 'IEC61850' AND y.initial = m.cdc AND y.is_deleted = FALSE);
-- Cek: harus 15 baris, id_group terisi
SELECT initial, id_group FROM master_data_objects
WHERE protocol = 'IEC61850' AND is_deleted = FALSE ORDER BY initial;
COMMIT-- COMMIT; atau ROLLBACK;