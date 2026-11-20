BEGIN;
ALTER TABLE iec61850_report_controls
    DROP CONSTRAINT IF EXISTS iec61850_report_controls_id_rtu_fkey,
    ADD CONSTRAINT iec61850_report_controls_id_rtu_fkey
        FOREIGN KEY (id_rtu) REFERENCES iec61850s (id_rtu)
        ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE master_datapoint_iec61850s
    DROP CONSTRAINT IF EXISTS master_datapoint_iec61850s_id_master_datapoint_fkey,
    ADD CONSTRAINT master_datapoint_iec61850s_id_master_datapoint_fkey
        FOREIGN KEY (id_master_datapoint) REFERENCES master_datapoints (id) ON DELETE CASCADE;

ALTER TABLE iec61850_command_formulas
    DROP CONSTRAINT IF EXISTS iec61850_command_formulas_id_master_datapoint_fkey,
        ADD CONSTRAINT iec61850_command_formulas_id_master_datapoint_fkey
        FOREIGN KEY (id_master_datapoint) REFERENCES master_datapoints (id) ON DELETE CASCADE;
COMMIT;