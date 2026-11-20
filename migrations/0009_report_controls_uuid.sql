-- iec61850_report_controls.id BIGSERIAL -> UUID (sama dengan backend). Idempoten: sudah uuid -> tidak ada aksi.
-- FEP tidak membaca kolom id (load_rcbs ORDER BY created_at, rcb_reference).
-- UNIQUE (id_rtu, rcb_reference), CHECK, FK, dan trigger tidak memakai id, jadi tidak tersentuh.
DO $$
DECLARE
    pk TEXT;
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'iec61850_report_controls' AND column_name = 'id') = 'bigint' THEN
        ALTER TABLE iec61850_report_controls ADD COLUMN id_new UUID NOT NULL DEFAULT gen_random_uuid();
        SELECT conname INTO pk FROM pg_constraint
        WHERE conrelid = 'iec61850_report_controls'::regclass AND contype = 'p';
        IF pk IS NOT NULL THEN
            EXECUTE format('ALTER TABLE iec61850_report_controls DROP CONSTRAINT %I', pk);
        END IF;
        ALTER TABLE iec61850_report_controls DROP COLUMN id; -- sequence BIGSERIAL ikut terhapus
        ALTER TABLE iec61850_report_controls RENAME COLUMN id_new TO id;
        ALTER TABLE iec61850_report_controls ADD PRIMARY KEY (id);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint c
                   WHERE c.conrelid = 'iec61850_report_controls'::regclass AND c.contype = 'u'
                     AND (SELECT array_agg(a.attname::TEXT ORDER BY a.attname) FROM pg_attribute a
                          WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)) = ARRAY['id_rtu', 'rcb_reference']) THEN
        RAISE EXCEPTION 'iec61850_report_controls: UNIQUE (id_rtu, rcb_reference) hilang';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'iec61850_report_controls'::regclass
                     AND tgname = 'iec61850_report_controls_notify_trigger') THEN
        RAISE EXCEPTION 'iec61850_report_controls: trigger iec61850_report_controls_notify_trigger hilang';
    END IF;
END $$;
