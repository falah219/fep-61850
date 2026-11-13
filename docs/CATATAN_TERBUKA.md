# Catatan terbuka FEP IEC 61850

Hal yang **belum diputuskan / belum dikonfirmasi tim**. Perbarui file ini setiap ada jawaban.

## Perlu konfirmasi tim

| # | Topik | Kondisi sekarang | Lokasi kode |
|---|---|---|---|
| 1 | **Nilai DPC (breaker) di payload COMMAND** | Pakai standar DCS IEC 104: `value 2 = CLOSE (true)`, `value 1 = OPEN (false)`. Dokumen `COMMAND_TEST_PAYLOADS.md` IEC 104 menulis sebaliknya (1=CLOSE). | `ctl_value` di [src/command.rs](../src/command.rs) |
| 2 | **APC / BSC** (setpoint analog, tap changer) | Belum didukung untuk command (ctlVal berupa struktur). Tambahkan jika ada kebutuhan. | `ctl_value` di [src/command.rs](../src/command.rs) |
| 3 | **`OSI_PARAMS_SIDE`: "Application ID", "AE Qualifier", P/S/T selector di form GRITA milik IED tujuan (remote) atau FEP (local)?** | Sudah bisa dipakai lewat env `OSI_PARAMS_SIDE` = `none` \| `local` \| `remote`. **Default `none` (perilaku lama: default libIEC61850) menunggu konfirmasi tim.** Selector divalidasi (hex 2 digit dipisah spasi, P/S maks 16 byte, T maks 4 byte); tidak valid -> default + ERROR di `RTU_LOGGING`. **Perlu diuji ke IED asli**: simulator libIEC61850 tidak memeriksa nilai ini, jadi uji simulator tidak membuktikan apa-apa. | `apply_osi` di [src/iec61850/client.rs](../src/iec61850/client.rs) |
| 4 | **AE/AP Invoke ID** | libIEC61850 v1.6.2.1 tidak punya setter. Kolom disimpan, tidak dikirim ke IED. | — |
| 5 | **Penyimpanan password ACSE** | Belum ditentukan (enkripsi kolom / Kubernetes secret). Hanya kolom `password_ref`; autentikasi belum diimplementasi. | migrasi `0001` |
| 6 | **Lokasi file SCL** (DB / object storage / volume) | Hanya nama file (`scl_file_name`). Tool import (fase 10) membaca file lokal. | — |
| 7 | **Cara CDC tim** (Debezium / lainnya) | Handler `CAPTURE_DATA_CHANGE` mengikuti format IEC 104 (`table`, `schema`, `operation`, `data`). Tabel `iec61850s`, `iec61850_report_controls`, `master_datapoint_iec61850s` perlu ikut dipantau. | [src/handlers.rs](../src/handlers.rs) |
| 8 | **`sync_telco_status()`** | Tim backend perlu menambahkan `'iec61850s'` ke daftar tabel, kalau tidak status di `telco_statuses` selalu false. | [migrations/0002_iec61850_telco_status.sql](../migrations/0002_iec61850_telco_status.sql) |
| 9 | **Review migrasi** `0001` (index unik parsial di `master_datapoints`, `id_rtu` UNIQUE di `iec61850s`) | Belum di-review / diuji ke Postgres. | [migrations/](../migrations/) |
| 10 | **Field tambahan `object_reference` di DataMessage** | Tidak ditambahkan (menunggu konfirmasi consumer). | [src/kafka/mod.rs](../src/kafka/mod.rs) |
| 11 | **Lisensi libIEC61850** (GPLv3 / komersial, sama seperti lib60870 di FEP IEC 104) | FEP me-link statis. Pastikan tim sudah punya lisensi komersial MZ Automation atau menerima GPLv3. | [docker/Dockerfile.dev](../docker/Dockerfile.dev) |
| 12 | **BRCB EntryID & ResvTms di IED asli** | Dirancang sesuai IEC 61850-7-2 dan diuji ke simulator libIEC61850. Perlu dicek ke IED vendor: (a) apakah EntryID dari sesi sebelumnya diterima, (b) apakah `ResvTms` didukung (IED Ed1 biasanya tidak; elemen dilewati kalau tidak ada). EntryID hanya di memori: hilang saat reload RTU / restart FEP (event selama restart FEP bisa hilang). | `resume_from_entry_id` di [src/iec61850/acquisition.rs](../src/iec61850/acquisition.rs) |
| 13 | **`oper_timeout_ms` vs batas 30 detik handler COMMAND** | Kolom baru (NULL = env `OPER_TIMEOUT_MS`, default 10000), CHECK `< 30000` karena handler COMMAND menunggu hasil maks 30 detik. `sbo_timeout_ms` tidak lagi dipakai untuk CommandTermination (tidak dipakai sama sekali di FEP). | migrasi `0001`, `oper_wait` di [src/iec61850/acquisition.rs](../src/iec61850/acquisition.rs) |

## Keputusan yang sudah dikonfirmasi

- Tabel baru `iec61850s` (belum ada tabel backend untuk form "Add Client").
- `reconnect_retry_count = 0` → reconnect tanpa batas.
- Kolom non-form (`acquisition_mode`, `poll_interval_ms`, `general_interrogation_interval`, `request_timeout_ms`) tetap kolom DB + default.
- Nilai DPS di Kafka: 0..3 apa adanya.
- `group` di-lookup dari `master_data_objects.initial` = CDC.
- `general_action` dari `master_general_actions` dengan `protocol='IEC61850'`.
- Grup quality mengikuti urutan IEC 104 (INVALID > SUBSTITUTED > QUESTIONABLE > GOOD).
- `t` IEC 61850 sudah UTC, tidak dikoreksi `TIMEZONE`.
- Cancel: action `CANCEL_RTU`. `orIdent` dari field `user`.
- `general_interrogation_interval >= 0`; 0 = GI hanya saat konek (CHECK langsung di migrasi `0001`, belum dipakai).
- Waktu: atribut `t` -> waktu entri report (`time_desc` `RTU`) -> jam server (`SERVER`).
- Shutdown: tunggu thread IED maks `SHUTDOWN_WAIT_SECS` (5), lalu `RTU_STATUS=0` untuk semua IED, baru flush Kafka.

## Belum diuji (butuh Postgres + Kafka dev)

Fase 6–9: migrasi + seed, `cargo run` (publish `RTU_DATAS`/`DATAPOINTS`), status online/offline, event Kafka, command.
Langkah uji ada di [README.md](../README.md) per fase.
