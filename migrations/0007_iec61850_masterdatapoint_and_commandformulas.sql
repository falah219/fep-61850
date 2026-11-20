-- 1. master_datapoints
INSERT INTO master_datapoints
  (id, id_rtu, protocol, ioa, tag, description,
   id_data_object, id_station, id_level_voltage, id_group_bay, id_component,
   created_at, updated_at)
VALUES
  (gen_random_uuid(), 9201, 'IEC61850', 1001, '9201_SIMIEC_20_SIMBAY-1_CB_1001', 'Circuit Breaker',
   (SELECT id FROM master_data_objects WHERE protocol = 'IEC61850' AND initial = 'DPS' AND is_deleted = FALSE LIMIT 1),
   (SELECT id FROM master_stations WHERE code = 'SIMIEC' AND is_deleted = FALSE),
   'fe5496f1-d9a2-4077-88f6-c3f024d3ba5e',   -- 20 kV
   (SELECT g.id FROM master_group_bays g JOIN master_stations s ON s.id = g.id_station
     WHERE s.code = 'SIMIEC' AND g.bay_feeder = 'SIMBAY 1' AND g.is_deleted = FALSE),
   '37d57fc6-e88f-46ab-9c01-4275501c13bf',   -- component CB
   now(), now());

-- 2. master_datapoint_iec61850s (dicari lewat id_rtu + ioa)
INSERT INTO master_datapoint_iec61850s (id_master_datapoint, object_reference, functional_constraint, cdc)
VALUES ((SELECT id FROM master_datapoints WHERE id_rtu = 9201 AND ioa = 1001 AND protocol = 'IEC61850' AND is_deleted = FALSE),
        'SIMIED1LD0/XCBR1.Pos', 'ST', 'DPS');

INSERT INTO master_datapoint_iec61850s (id_master_datapoint, object_reference, functional_constraint, cdc)
VALUES ((SELECT id FROM master_datapoints WHERE id_rtu = 9201 AND ioa = 1001 AND protocol = 'IEC61850' AND is_deleted = FALSE),
        'SIMIED1LD0/XCBR1.Pos', 'ST', 'DPS');

-- 3. iec61850_command_formulas
INSERT INTO iec61850_command_formulas (id_master_datapoint, type, value)
VALUES ((SELECT id FROM master_datapoints WHERE id_rtu = 9201 AND ioa = 6001 AND protocol = 'IEC61850' AND is_deleted = FALSE),
        'DPC', 'true');