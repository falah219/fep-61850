-- Rollback 0001_iec61850.up.sql
BEGIN;
DROP INDEX IF EXISTS ux_master_datapoints_iec61850_rtu_ioa;
DROP TABLE IF EXISTS iec61850_command_formulas;
DROP TABLE IF EXISTS master_datapoint_iec61850s;
DROP FUNCTION IF EXISTS iec61850_check_unique_reference();
DROP TABLE IF EXISTS iec61850_report_controls;
DROP TABLE IF EXISTS iec61850s;
COMMIT;
