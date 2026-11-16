-- USULAN (review tim). Override orCat (IEC 61850-7-3 orCategory 0..8) per titik kontrol.
-- NULL = pakai env CONTROL_OR_CAT FEP (default 3 = remote-control).
-- FEP membaca kolom ini dengan query terpisah: kalau migrasi ini belum dijalankan, FEP tetap jalan tanpa override.

ALTER TABLE iec61850_command_formulas
    ADD COLUMN IF NOT EXISTS or_cat SMALLINT NULL CHECK (or_cat BETWEEN 0 AND 8);

COMMENT ON COLUMN iec61850_command_formulas.or_cat IS
    'orCat command titik ini (0 not-supported, 1 bay-control, 2 station-control, 3 remote-control, 4 automatic-bay, 5 automatic-station, 6 automatic-remote, 7 maintenance, 8 process). NULL = env CONTROL_OR_CAT';
