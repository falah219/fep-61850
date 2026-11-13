# Catatan terbuka FEP IEC 61850

Hal yang **belum diputuskan / belum dikonfirmasi tim**. Perbarui file ini setiap ada jawaban.

## Perlu konfirmasi tim

| # | Topik | Kondisi sekarang | Lokasi kode |
|---|---|---|---|
| 1 | **Nilai DPC (breaker) di payload COMMAND** | Pakai standar DCS IEC 104: `value 2 = CLOSE (true)`, `value 1 = OPEN (false)`. Dokumen `COMMAND_TEST_PAYLOADS.md` IEC 104 menulis sebaliknya (1=CLOSE). | `ctl_value` di [src/command.rs](../src/command.rs) |
| 2 | **APC / BSC** (setpoint analog, tap changer) | Belum didukung untuk command (ctlVal berupa struktur). Tambahkan jika ada kebutuhan. | `ctl_value` di [src/command.rs](../src/command.rs) |
| 3 | **"Application ID" & "AE Qualifier" di form GRITA** | Belum dipakai. Perlu tahu: milik IED tujuan (remote) atau FEP (local)? | `Ied::connect` di [src/iec61850/client.rs](../src/iec61850/client.rs) |
| 4 | **AE/AP Invoke ID** | libIEC61850 v1.6.2.1 tidak punya setter. Kolom disimpan, tidak dikirim ke IED. | — |
| 5 | **Penyimpanan password ACSE** | Belum ditentukan (enkripsi kolom / Kubernetes secret). Hanya kolom `password_ref`; autentikasi belum diimplementasi. | migrasi `0001` |
| 6 | **Lokasi file SCL** (DB / object storage / volume) | Hanya nama file (`scl_file_name`). Tool import (fase 10) membaca file lokal. | — |
| 7 | **Cara CDC tim** (Debezium / lainnya) | Handler `CAPTURE_DATA_CHANGE` mengikuti format IEC 104 (`table`, `schema`, `operation`, `data`). Tabel `iec61850s`, `iec61850_report_controls`, `master_datapoint_iec61850s` perlu ikut dipantau. | [src/handlers.rs](../src/handlers.rs) |
| 8 | **`sync_telco_status()`** | Tim backend perlu menambahkan `'iec61850s'` ke daftar tabel, kalau tidak status di `telco_statuses` selalu false. | [migrations/0002_iec61850_telco_status.sql](../migrations/0002_iec61850_telco_status.sql) |
| 9 | **Review migrasi** `0001` (index unik parsial di `master_datapoints`, `id_rtu` UNIQUE di `iec61850s`) | Belum di-review / diuji ke Postgres. | [migrations/](../migrations/) |
| 10 | **Field tambahan `object_reference` di DataMessage** | Tidak ditambahkan (menunggu konfirmasi consumer). | [src/kafka/mod.rs](../src/kafka/mod.rs) |
| 11 | **Lisensi libIEC61850** (GPLv3 / komersial, sama seperti lib60870 di FEP IEC 104) | FEP me-link statis. Pastikan tim sudah punya lisensi komersial MZ Automation atau menerima GPLv3. | [docker/Dockerfile.dev](../docker/Dockerfile.dev) |

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

## Belum diuji (butuh Postgres + Kafka dev)

Fase 6–9: migrasi + seed, `cargo run` (publish `RTU_DATAS`/`DATAPOINTS`), status online/offline, event Kafka, command.
Langkah uji ada di [README.md](../README.md) per fase.
