# Catatan terbuka FEP IEC 61850

Hal yang **belum diputuskan / belum dikonfirmasi tim**. Perbarui file ini setiap ada jawaban.

## Perlu konfirmasi tim

| # | Topik | Kondisi sekarang | Lokasi kode |
|---|---|---|---|
| 1 | **Nilai DPC (breaker) di payload COMMAND** | Pakai standar DCS IEC 104: `value 2 = CLOSE (true)`, `value 1 = OPEN (false)`. Dokumen `COMMAND_TEST_PAYLOADS.md` IEC 104 menulis sebaliknya (1=CLOSE). | `ctl_value` di [src/domain/command.rs](../src/domain/command.rs) |
| 2 | **APC / BSC / ENC** (setpoint analog, tap changer) | Sudah didukung: ENC integer, BSC Tcmd (0 stop, 1 lower, 2 higher, bit string 2 bit seperti `Dbpos_toMmsValue`), APC float (libIEC61850 membungkus ke `AnalogueValue`). Perlu diuji ke IED asli (simulator tidak punya BSC/APC). | `ctl_value` di [src/domain/command.rs](../src/domain/command.rs) |
| 3 | **`OSI_PARAMS_SIDE`: "Application ID", "AE Qualifier", P/S/T selector di form GRITA milik IED tujuan (remote) atau FEP (local)?** | Sudah bisa dipakai lewat env `OSI_PARAMS_SIDE` = `none` \| `local` \| `remote`. **Default `none` (perilaku lama: default libIEC61850) menunggu konfirmasi tim.** Selector divalidasi (hex 2 digit dipisah spasi, P/S maks 16 byte, T maks 4 byte); tidak valid -> default + ERROR di `RTU_LOGGING`. **Perlu diuji ke IED asli**: simulator libIEC61850 tidak memeriksa nilai ini, jadi uji simulator tidak membuktikan apa-apa. | `apply_osi` di [src/iec61850/client.rs](../src/iec61850/client.rs) |
| 4 | **AE/AP Invoke ID** | libIEC61850 v1.6.2.1 tidak punya setter. Kolom disimpan, tidak dikirim ke IED. | — |
| 5 | **Penyimpanan password ACSE** | Belum ditentukan (enkripsi kolom / Kubernetes secret). Hanya kolom `password_ref`; autentikasi belum diimplementasi. | migrasi `0001` |
| 6 | **Lokasi file SCL** (DB / object storage / volume) | Hanya nama file (`scl_file_name`). Tool import (fase 10) membaca file lokal. | — |
| 7 | **Cara CDC tim** (Debezium / lainnya) | Handler `CAPTURE_DATA_CHANGE` membaca format cdc_listener (`table`, `schema`, `operation`, `record`; cadangan `data`). Migrasi `0001` memasang trigger `notify_table_change()` (fungsi milik cdc_listener, harus sudah ada) di `iec61850s`, `iec61850_report_controls`, `master_datapoint_iec61850s`, `iec61850_command_formulas`. | [src/kafka/handlers/capture_data_change.rs](../src/kafka/handlers/capture_data_change.rs) |
| 8 | **`sync_telco_status()`** | Tim backend perlu menambahkan `'iec61850s'` ke daftar tabel, kalau tidak status di `telco_statuses` selalu false. | [migrations/0002_iec61850_telco_status.sql](../migrations/0002_iec61850_telco_status.sql) |
| 9 | **Review migrasi** `0001` (index unik parsial di `master_datapoints`, `id_rtu` UNIQUE di `iec61850s`) | Belum di-review / diuji ke Postgres. | [migrations/](../migrations/) |
| 10 | **Field tambahan `object_reference` di DataMessage** | Tidak ditambahkan (menunggu konfirmasi consumer). | [src/kafka/mod.rs](../src/kafka/mod.rs) |
| 11 | **Lisensi libIEC61850** (GPLv3 / komersial, sama seperti lib60870 di FEP IEC 104) | FEP me-link statis. Source v1.6.2.1 disimpan di `native/libiec61850` (dengan `COPYING`). Pastikan tim sudah punya lisensi komersial MZ Automation atau menerima GPLv3. | [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) |
| 12 | **BRCB EntryID & ResvTms di IED asli** | Dirancang sesuai IEC 61850-7-2 dan diuji ke simulator libIEC61850. Perlu dicek ke IED vendor: (a) apakah EntryID dari sesi sebelumnya diterima, (b) apakah `ResvTms` didukung (IED Ed1 biasanya tidak; elemen dilewati kalau tidak ada). EntryID hanya di memori: hilang saat reload RTU / restart FEP (event selama restart FEP bisa hilang). | `resume_from_entry_id` di [src/iec61850/acquisition.rs](../src/iec61850/acquisition.rs) |
| 13 | **Timeout remote control vs IED** | (a) `count_down_select` di `remote_controls` harus **<= sboTimeout IED**, kalau tidak select sudah kedaluwarsa di IED sebelum operator operate. (b) env `OPER_TIMEOUT_MS` (default 10000) harus **< `command_timeout`** `remote_controls`, dan < 30 detik (batas tunggu handler COMMAND). Kolom `sbo_timeout_ms` / `oper_timeout_ms` dihapus; sboTimeout memakai nilai IED. Belum ada validasi otomatis. | `remote_controls` (backend), env `OPER_TIMEOUT_MS` |
| 14 | **`iec61850_command_formulas` tanpa `id_remote_control`** | Kolom mengikuti permintaan (beda dengan `iec104_command_formulas` yang punya FK `id_remote_control`). `type` dan `value` disimpan, tidak dipakai FEP (CDC dari `master_datapoint_iec61850s`, value dari payload). Konfirmasi ke tim backend. | migrasi `0001` |
| 15 | **Event BROWSE: CDC hasil tebakan** | CDC ditebak dari tipe MMS: ENS/ENC terbaca INS/INC, DO tidak dikenali -> `cdc: null`. Perlu diuji ke IED vendor (struktur LN$FC lewat `getVariableSpecification`). Topik baru `IEC61850_BROWSE_RESPONSE` perlu dibuat di Kafka bersama + konsumennya (backend/GRITA). | [src/iec61850/browse.rs](../src/iec61850/browse.rs) |
| 16 | **Event SCL_IMPORT (tabel `iec61850_scl_files` milik backend)** | (a) `content` diasumsikan **TEXT** (kalau BYTEA, query perlu `convert_from(content, 'UTF8')`). (b) Format referensi = format BROWSE yang sebenarnya (`LD/LN.DO`, `LD/LLN0.BR.brcbST01`, `LD/LLN0.dsStatus`, config rev `LD/LLN0.NamPlt.configRev`), **bukan** format MMS `LD/LN$FC$DO` yang disebut di permintaan. (c) Key Kafka respons = `key` payload (sama dengan BROWSE), bukan `id_rtu`. (d) `cdc` = CDC asli dari SCL (ENS/ENC tetap ENS/ENC), beda dengan BROWSE yang menebak INS/INC: pastikan `master_data_objects` punya CDC ini. (e) RCB tanpa field `type` BRCB/URCB terpisah: pakai `buffered` (sama dengan BROWSE). (f) `rpt_id` kosong di SCL -> `null` (IED biasanya memakai referensi RCB `LD/LLN0$BR$<rcb>`). | [src/kafka/handlers/rtu_event/scl_import.rs](../src/kafka/handlers/rtu_event/scl_import.rs), [src/scl.rs](../src/scl.rs) |

## Temuan uji

### BROWSE IED 9302: RCB `MainCSWI1.RP.brcbName01` tidak muncul (Studio 9 RCB, FEP 8)

- **Penyebab (dari kode)**: browse sudah membaca RCB semua LN (bukan hanya LLN0), dan konversi referensi RCB ke MMS
  (`LD/LN.RP.name` -> `LN$RP$name`) dilakukan libIEC61850 untuk LN apa saja. Satu-satunya jalur yang membuang RCB yang
  terdaftar di IED adalah `getRCBValues` gagal (dulu: dicatat di `errors` lalu RCB dilewati). Studio juga gagal enable RCB
  ini (`temporarily-unavailable`), jadi kemungkinan besar IED menjawab data-access-error yang sama saat dibaca
  (`temporary-unavailable`) atau struktur RCB-nya tidak standar (`type-inconsistent`). Teks pastinya ada di `errors`
  respons BROWSE 9302 dengan `ref` = `iec61850SC01_LD0/MainCSWI1.RP.brcbName01` (belum dilihat).
- **Sekarang**: RCB itu tetap masuk `rcbs[]` dengan `read_error` (+ log WARN `[BROWSE] RCB ... tidak bisa dibaca`).
- **Perlu dicek**: isi `read_error` di respons BROWSE 9302 berikutnya. Kalau `temporary-unavailable`, RCB ini memang tidak
  bisa dipakai FEP (sama dengan Studio).

### Koneksi mati yang tetap terbaca CONNECTED

- **Gejala**: banyak IED konek serentak (FEP start / reconnect massal). Sebagian IED tetap terlihat
  online (`RTU_STATUS=1`), tapi RCB-nya tidak pernah aktif dan report tidak datang.
- **Penyebab (dugaan)**: banyak connect serentak di libIEC61850 v1.6.2.1 bisa menghasilkan koneksi
  mati yang `IedConnection_getState()`-nya tetap `CONNECTED`. Setiap request di koneksi itu gagal
  (`CONNECTION_LOST` / `NOT_CONNECTED` / timeout), dan FEP sebelumnya terus mencoba ulang RCB di
  koneksi yang sama.
- **Mitigasi**:
  1. `MAX_CONCURRENT_CONNECTS` default 4 (sebelumnya 200).
  2. Error koneksi di request mana pun langsung mengakhiri sesi lalu reconnect.
  3. Liveness `LIVENESS_SECS` (30): tanpa tanda hidup -> baca `LLN0.Mod.stVal`; gagal -> reconnect.
  4. `BRCB_RESV_TMS_SECS` default 20 (< `RCB_RETRY_SECS` 30), supaya reservasi BRCB dari sesi mati
     sudah lepas saat RCB dicoba ulang.
  5. Polling: read yang TIMEOUT menghentikan sisa grup di siklus itu lalu langsung cek liveness.
     IED hang terdeteksi dalam 2 x `request_timeout_ms` (bukan jumlah grup x). Uji: `kill -STOP`
     simulator mode POLLING, ukur waktu sampai log "koneksi ke IED terputus". **Belum dijalankan.**
- **Uji penerimaan**: 20 simulator di port berbeda, FEP di-start 10 kali; dalam 60 detik tidak boleh
  ada IED yang `RTU_STATUS=1` tanpa RCB aktif. **Belum dijalankan.** Perangkat:
  `docker-compose.sim20.yml` (20 simulator, port host 10201..10220), `migrations/seed_sim20.sql`
  (RTU 9101..9120), lalu di container dev `scripts/uji_konek_serentak.sh 10 90`.
- **Perlu dikonfirmasi**: apakah akar masalahnya di libIEC61850 (laporkan ke MZ Automation) atau di
  cara FEP memakai `IedConnection`. Dengan mitigasi di atas gejalanya tertangani, tapi penyebab
  pastinya belum diketahui.

## Keputusan yang sudah dikonfirmasi

- Tabel baru `iec61850s` (belum ada tabel backend untuk form "Add Client").
- `reconnect_retry_count = 0` → reconnect tanpa batas.
- Kolom non-form (`acquisition_mode`, `poll_interval_ms`, `general_interrogation_interval`, `request_timeout_ms`) tetap kolom DB + default.
- Nilai DPS di Kafka: 0..3 apa adanya.
- `group` = `master_data_objects.id_group` (UUID) dari baris `initial` = CDC (sama dengan IEC 104).
- `general_action` = `master_general_actions.code` apa adanya (`protocol='IEC61850'`, `name_action` = action); kode GRITA, bukan COT.
- Grup quality mengikuti urutan IEC 104 (INVALID > SUBSTITUTED > QUESTIONABLE > GOOD).
- `t` IEC 61850 sudah UTC, tidak dikoreksi `TIMEZONE`.
- Cancel: action `CANCEL_RTU`. `orIdent` dari field `user`.
- `general_interrogation_interval >= 0`; 0 = GI hanya saat konek (CHECK langsung di migrasi `0001`, belum dipakai).
- Waktu: atribut `t` -> waktu entri report (`time_desc` `RTU`) -> jam server (`SERVER`).
- Shutdown: tunggu thread IED maks `SHUTDOWN_WAIT_SECS` (5), lalu `RTU_STATUS=0` untuk semua IED, baru flush Kafka.
- `MAX_CONCURRENT_CONNECTS` default 4, `BRCB_RESV_TMS_SECS` default 20, `LIVENESS_SECS` default 30.
- COMMAND masuk antrean IED sesuai urutan kedatangan; hanya tunggu hasil + respons yang di-spawn.

## Belum diuji (butuh Postgres + Kafka dev)

Fase 6–9: migrasi + seed, `cargo run` (publish `RTU_DATAS`/`DATAPOINTS`), status online/offline, event Kafka, command.
Langkah uji ada di [README.md](../README.md) per fase.
