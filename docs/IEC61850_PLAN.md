# Rencana FEP IEC 61850 (MMS Client)

Dokumen ini adalah acuan kerja untuk Claude Code. Simpan di `docs/IEC61850_PLAN.md`.

---

## 0. Aturan kerja (WAJIB dibaca dulu)

1. Saya pemula di Rust dan IEC 61850. Jelaskan setiap langkah dalam **bahasa Indonesia**, singkat dan jelas: apa yang dibuat, kenapa, dan cara menjalankannya.
2. Kerjakan **per fase** (lihat bagian 6). Setelah satu fase selesai, **berhenti**, tunjukkan cara mengujinya, lalu tunggu konfirmasi saya sebelum lanjut ke fase berikutnya.
3. Kerja di branch `iec61850`. Jangan ubah branch `merauke`. Commit kecil-kecil dengan pesan yang jelas.
4. Lingkungan: **Windows + Docker Desktop**. Jangan build langsung di Windows. Semua perintah build/run/test dijalankan lewat
   `docker compose -f docker-compose.dev.yml exec dev <perintah>`.
5. Jangan pernah menyimpan kredensial asli di kode atau di repo. Pakai `.env` lokal dengan nilai dummy.
6. **Jangan menyentuh database atau Kafka produksi/kantor.** Semua uji coba memakai Postgres dan Kafka lokal di Docker.
7. Perubahan skema database dibuat sebagai **file migrasi SQL usulan** (`migrations/`), diuji di Postgres lokal, dan diberi catatan bahwa harus di-review tim sebelum dipakai di lingkungan bersama.
8. Kalau ada keputusan desain yang tidak tertulis di dokumen ini, **tanya saya dulu**.

---

## 1. Konteks repo yang ada (FEP IEC 104)

- Rust + **lib60870-C** (MZ Automation) lewat `build.rs` + `wrapper.h` + `bindgen` (`src/ffi.rs`).
- Kafka via `rdkafka`, Postgres via `deadpool-postgres`, runtime `tokio`, deploy Kubernetes.
- Konfigurasi RTU dari tabel `iec104s`. Tag, formula, group, dan general action dari `master_datapoints`, `formulas`, `master_data_objects`, `master_general_actions`.
- Data dari RTU dipublish ke:
  - `RTU_DATAS`: data mentah, semua titik
  - `DATAPOINTS`: titik yang punya tag, sudah diskalakan formula
  - Format: struct `DataMessage` di `src/kafka/models.rs`
- Status/log: `RTU_STATUS`, `RTU_LOGGING`.
- Perintah masuk lewat `RTU_EVENTS` (event `COMMAND`, `GI`, `UPDATE`, `CREATE`, `DELETE`, `STATE`, `RESET`, `CHANNEL`), plus `MASTER_DATAPOINT_EVENT`, `CAPTURE_DATA_CHANGE`, `MANUAL_SET_EVENT`, dll.

### Topik Kafka (FEP IEC 61850 wajib sama dengan FEP IEC 104)

FEP adalah jembatan antara IED dan service backend lain. Backend tidak berbicara ke IED; semuanya lewat Kafka.

```
[IED / Simulator] ──MMS──► [FEP IEC 61850] ──produce──► Kafka ──consume──► [Service backend lain]
                  ◄──MMS──                  ◄──consume── Kafka ◄──produce── [Service backend lain]
```

**Di-produce FEP** (dikonsumsi service backend lain):

| Topik | Isi | Kode acuan di repo |
|---|---|---|
| `RTU_DATAS` | Semua data mentah dari IED | `kafka/publisher/message_builder.rs` |
| `DATAPOINTS` | Data yang punya tag, sudah diskalakan formula | `kafka/publisher/message_builder.rs` |
| `RTU_STATUS` | Status koneksi IED + status sistem FEP (start/stop) | `kafka/publisher/rtu_status_sender.rs` |
| `RTU_LOGGING` | Log komunikasi per IED | `kafka/publisher/rtu_logging_sender.rs` |
| Topik respons command | Hasil SELECT/OPERATE/CANCEL | `kafka/handlers/rtu_event/command/response.rs` |
| `MANUAL_SET_EVENT_RESPONSE` | Hasil manual set | `kafka/handlers/manual_set.rs` |

**Di-consume FEP** (dikirim service backend lain): `RTU_EVENTS` (COMMAND, GI, UPDATE, CREATE, DELETE, STATE, RESET, CHANNEL), `RTU_COMMAND_CONFIG_REQUEST`, `MASTER_DATAPOINT_EVENT`, `MASTER_DATA_OBJECT_EVENT`, `MASTER_RTU_EVENTS`, `CAPTURE_DATA_CHANGE`, `MANUAL_SET_EVENT`. Daftar diambil dari env `SUBSCRIBE_TOPICS`, sama seperti `src/main.rs`.

Aturan:
- Nama topik, key, dan format JSON **harus identik** dengan versi IEC 104. Bedanya hanya `protocol: "IEC61850"`.
- Handler yang menerima pesan dengan `protocol` selain `IEC61850` harus **mengabaikannya**, karena FEP IEC 104 dan FEP IEC 61850 membaca topik yang sama. Polanya sama seperti `RtuEventHandler::handle` yang mengecek `msg.protocol != "IEC104"`.
- Gunakan `GROUP_ID` Kafka yang **berbeda** dari FEP IEC 104 (contoh `IEC61850_GROUP`), supaya kedua FEP sama-sama menerima semua pesan.
- Sebelum menulis kode Kafka, baca dulu file-file acuan di atas dan ringkas format setiap topik ke saya.

### Yang dipakai ulang
`config/`, `logger.rs`, `repository/pool.rs`, `kafka/` (producer, consumer, senders), `resolver/` (tag, formula, group, general action), `connection/` (manager, reconnection, network ping, health check), `utils/`, Dockerfile, Kubernetes manifest.

### Yang diganti
- `native/lib60870-C`, `build.rs`, `wrapper.h`, `ffi.rs` → **libIEC61850** (MZ Automation, pola FFI sama). Pin ke satu tag rilis stabil terbaru (cek di GitHub mz-automation/libiec61850), sama seperti Dockerfile mem-pin commit lib60870.
- `iec104/` → `iec61850/` (MMS client, report handler, poller, mapper CDC/quality/reason).
- `kafka/handlers/rtu_event/command/commands/*` → kontrol IEC 61850 via `ControlObjectClient`.

---

## 2. Prinsip desain utama

### 2.1 Alamat titik tetap memakai IOA angka
- Setiap titik IEC 61850 diberi **IOA virtual (integer, unik per `id_rtu`)**.
- FEP memuat mapping `object_reference ↔ (id_rtu, ioa)` saat start ke `HashMap`/`DashMap`, lalu menerjemahkannya setiap ada data masuk.
- Dengan begitu `TagResolver`, `FormulaResolver`, `DataMessage.address`, dan event `COMMAND` (field `ioa`) **tetap sama**. Consumer Kafka tidak perlu diubah.
- Mapping dilakukan di level **Data Object** (contoh `BAY1LD0/XCBR1.Pos`), bukan per atribut. Nilai diambil dari `stVal` (status) atau `mag.f` / `mag.i` (measurement). `q` dan `t` ikut sebagai quality dan time.
- Status dan kontrol memakai IOA terpisah (status `XCBR1.Pos`, kontrol `CSWI1.Pos`), sama seperti pola IEC 104 sekarang.

### 2.2 Mode akuisisi per IED
| Mode | Keterangan |
|---|---|
| `REPORT` (default) | Enable RCB (buffered/unbuffered), TrgOps = data-change + quality-change + integrity + GI. Integrity period dari konfigurasi. Kirim GI setelah konek dan setiap `gi_interval_secs`. |
| `POLLING` | Cadangan untuk IED yang RCB-nya penuh/tidak tersedia. Baca **per dataset atau per Logical Node + FC**, jangan per atribut. Interval dari `poll_interval_ms`. |
| `HYBRID` | Report untuk status (ST), polling untuk measurement (MX). Opsional, dikerjakan terakhir. |

Aturan polling:
- Satu siklus polling per IED dijalankan berurutan (satu request MMS selesai sebelum request berikutnya), sedangkan antar-IED berjalan paralel.
- **Kirim ke Kafka hanya jika nilai berubah**: status berubah, quality berubah, atau measurement melewati deadband. Selain itu, kirim ulang semua nilai setiap `integrity_period_ms` (meniru integrity report).
- Kalau satu siklus belum selesai sebelum siklus berikutnya, **lewati** siklus itu (jangan menumpuk) dan catat warning.
- Field `action` di Kafka untuk data hasil polling: `POLLING` (dan `INTEGRITY` untuk kirim ulang berkala).

### 2.3 Concurrency
- Target: **minimal 100 IED** pada satu instance FEP.
- Fungsi libIEC61850 yang blocking **tidak boleh** dipanggil langsung di task `async`. Pakai `tokio::task::spawn_blocking` atau thread khusus per IED.
- Callback dari C (report handler, connection state handler) dibungkus `catch_unwind`, sama seperti `src/iec104/client.rs`.
- Batasi koneksi serentak saat start (pakai `MAX_CONCURRENT_CONNECTS` yang sudah ada).

---

## 3. Skema database (USULAN, harus di-review tim)

Prinsip: **hanya pengaturan akuisisi/protokol IEC 61850.** Hal berikut sudah diurus backend, jadi **jangan diduplikasi**:
- hierarki peralatan: `id_station`, `id_level_voltage`, `id_group_bay`, `id_component` di `master_datapoints`
- alarm (class, priority, limit, warna, RTN, ack): tabel `alarms` + `alarm_rules`
- nilai terakhir: `tabulars`
- scaling: `formulas`; manual set: `manual_sets`
- tag, deskripsi, `show_alarm` / `show_soe` / `show_logsheet`: `master_datapoints`

`master_datapoints` dipakai bersama oleh semua protokol, jadi kolom khusus IEC 61850 **tidak** ditambahkan ke sana. Kolom itu ditaruh di tabel ekstensi 1:1 (bagian 3.3). Titik IEC 61850 tetap punya baris di `master_datapoints` dengan `protocol = 'IEC61850'`, `id_rtu`, `ioa` (IOA virtual), dan `tag`, sehingga tag resolver, formula, alarm, dan tabular tetap jalan tanpa perubahan.

### 3.1 Tabel `iec61850s` (koneksi IED, padanan `iec104s`)

Kolom mengikuti form **"Add Client" IEC 61850 di UI GRITA** (Configure → Front End Processor → IEC61850). Nama label UI ditulis di kolom kedua.

| Kolom | Label di UI GRITA | Default | Keterangan |
|---|---|---|---|
| `id_rtu` | RTU ID | | Sama konsepnya dengan `iec104s.id_rtu` |
| `id_station` | Station Name | | FK ke `master_stations` |
| `state_rtu` | RTU State (Enable/Disable) | true | Sama seperti `iec104s` |
| `ip_address` | Server IP Address | | IP IED |
| `port` | Server Port Number | 102 | Port MMS |
| `local_ip_address` | Client IP Address | `0.0.0.0` | IP/interface lokal FEP untuk konek. `0.0.0.0` = otomatis |
| `latitude`, `longitude` | RTU Location | null | Cek dulu apakah sudah ada di tabel RTU lain; jangan diduplikasi |
| `ae_invoke_id` | AE Invoke ID | 10 | Parameter ACSE/OSI. Cek apakah libIEC61850 menyediakan setter-nya; kalau tidak, simpan saja dan catat ke saya |
| `ae_qualifier` | AE Qualifier | 12 | `IsoConnectionParameters` (AP-title + AE-qualifier) |
| `ap_invoke_id` | AP Invoke ID | 10 | Sama seperti `ae_invoke_id` |
| `ap_title` | Application ID | `1.1.999.1` | AP-title (format OID) |
| `p_selector` | Presentation Selector | `00 00 00 01` | Hex, disimpan sebagai string |
| `s_selector` | Session Selector | `00 01` | Hex |
| `t_selector` | Transport Selector | `00 01` | Hex |
| `goose_interface` | GOOSE Adapter | null | Nama network interface (misal `eth1`). Untuk subscribe GOOSE, **fitur lanjutan**, bukan fase awal |
| `scl_file_name` | SCL File | null | File ICD/CID/SCD IED. Lokasi penyimpanan file (DB/object storage/volume) ditanyakan ke saya |
| `use_scl_file` | Use SCL File | false | true = model dan mapping diambil dari SCL; false = FEP browse model langsung ke IED |
| `ied_name` | IED Name | null | Nama IED di dalam file SCL (wajib jika `use_scl_file` = true dan file berisi banyak IED, mis. SCD) |
| `reconnect_time_ms` | Reconnect Time (ms) | 5000 | Jeda antar percobaan reconnect. Override nilai env global |
| `reconnect_retry_count` | Reconnect Re-try Count | 0 | **Tanyakan arti 0 ke saya** (kemungkinan 0 = tanpa batas) |
| `connect_timeout_ms` | Connect Timeout (ms) | 10000 | |
| `use_password` | Security → Password | false | Autentikasi ACSE password |
| `password_ref` | (isi password) | null | **Jangan simpan password asli dalam teks biasa.** Tanyakan ke saya cara tim menyimpan secret (enkripsi kolom / Kubernetes secret) |
| `use_certificate` | Security → Certificate | false | TLS (IEC 62351-4). Detail dari "Security Setup": **fitur lanjutan** |
| `status_rtu`, `channel_number`, `channel_state` | (tidak di form) | | Sama seperti `iec104s`, supaya handler `RTU_EVENTS` bisa dipakai ulang |
| `is_deleted`, `created_at`, `updated_at` | | | Audit |

Kolom tambahan dari rencana ini yang **tidak ada di form** GRITA. Tanyakan ke saya apakah perlu ditambahkan ke UI, atau cukup default/env:

| Kolom | Default | Keterangan |
|---|---|---|
| `acquisition_mode` | `REPORT` | `REPORT` / `POLLING` / `HYBRID` |
| `poll_interval_ms` | 2000 | Untuk mode polling |
| `general_interrogation_interval` | 300 | Detik |
| `request_timeout_ms` | 5000 | Timeout tiap request MMS |

Catatan:
- `iec104s` punya kolom dengan ejaan `general_interragation_interval` dan `orginator_address`. Untuk tabel baru pakai ejaan yang benar, kecuali tim minta disamakan.
- UI GRITA juga punya menu **"Add Server" (Server/Slave)**, yaitu FEP bertindak sebagai IED/server IEC 61850 ke sistem lain. Itu **di luar lingkup** rencana ini dan dicatat sebagai pengembangan berikutnya.
- Sebelum membuat migrasi, tanyakan ke saya apakah tim backend sudah punya tabel untuk form ini (mungkin UI-nya sudah tersambung ke tabel tertentu). Kalau sudah ada, **ikuti tabel itu**, jangan buat baru.

### 3.2 Tabel `iec61850_report_controls` (hanya untuk mode `REPORT`/`HYBRID`)
| Kolom | Wajib | Keterangan |
|---|---|---|
| `id` | ya | PK |
| `id_rtu` | ya | FK ke `iec61850s` |
| `rcb_reference` | ya | contoh `BAY1LD0/LLN0.BR.brcbST01` (BRCB atau URCB, terbaca dari `.BR.` / `.RP.`) |
| `dataset_reference` | | Null = pakai dataset yang sudah dikonfigurasi di IED |
| `trg_ops` | | Null = pakai setting IED. Contoh `dchg,qchg,integrity,gi` |
| `intg_pd_ms` | | Integrity period. Null = pakai setting IED |
| `buf_tm_ms` | | Buffer time. Null = pakai setting IED |
| `enabled` | ya | Default true |

Kolom yang null **tidak ditulis** ke IED, supaya FEP tidak merusak konfigurasi RCB dari engineering tool vendor.

### 3.3 Tabel ekstensi `master_datapoint_iec61850s` (1:1 dengan `master_datapoints`)
| Kolom | Wajib | Keterangan |
|---|---|---|
| `id_master_datapoint` | ya | PK + FK ke `master_datapoints.id` (uuid) |
| `object_reference` | ya | Data Object, contoh `BAY1LD0/XCBR1.Pos` |
| `functional_constraint` | ya | `ST` (status), `MX` (measurement), `CO` (kontrol), `SP` (setpoint). Jenis titik diturunkan dari sini |
| `cdc` | ya | `SPS`, `DPS`, `INS`, `ENS`, `MV`, `CMV`, `BCR`, `SPC`, `DPC`, `INC`, `APC`, `BSC` |
| `value_attribute` | | Null = default dari CDC (`stVal` untuk SPS/DPS/INS/ENS, `mag.f` untuk MV, `cVal.mag.f` untuk CMV, `actVal` untuk BCR) |
| `ctl_model` | | Hanya titik kontrol. Null = **dibaca otomatis dari IED** (`ctlModel` di FC CF) saat konek |
| `sbo_timeout_ms` | | Hanya titik kontrol. Null = default IED |
| `interlock_check` | | Default false. Bit `Check.interlock-check` saat operate |
| `synchro_check` | | Default false. Bit `Check.synchrocheck` saat operate |
| `deadband` | | Hanya measurement di mode polling: perubahan lebih kecil dari ini tidak dikirim |

Constraint:
- `UNIQUE (object_reference, functional_constraint)` per `id_rtu` (cek lewat join ke `master_datapoints`)
- `master_datapoints`: `UNIQUE (id_rtu, ioa)` untuk `protocol = 'IEC61850'`

Supaya FEP tetap sinkron saat konfigurasi berubah (lewat `CAPTURE_DATA_CHANGE` / `MASTER_DATAPOINT_EVENT`), tabel `iec61850s`, `iec61850_report_controls`, dan `master_datapoint_iec61850s` harus ikut dipantau CDC. Konfirmasi ke saya cara CDC di tim (Debezium atau lainnya) sebelum mengimplementasikan.

### 3.4 Quality (IEC 61850-7-3) → field Kafka
- `quality` (grup, sama seperti sekarang): `GOOD` / `INVALID` / `QUESTIONABLE` dari `q.validity`.
- `flag` (detail, format sama seperti sekarang, dipisah `|`): dari `detailQual` (`OVERFLOW`, `OUT_OF_RANGE`, `BAD_REFERENCE`, `OSCILLATORY`, `FAILURE`, `OLD_DATA`, `INCONSISTENT`, `INACCURATE`), ditambah `SUBSTITUTED` (source), `TEST`, `OPERATOR_BLOCKED`.
- Time quality (`LeapSecondsKnown`, `ClockFailure`, `ClockNotSynchronized`): kalau clock failure atau tidak sinkron, tambahkan flag `CLOCK_NOT_SYNC`.

### 3.5 `DataMessage` di Kafka
Field **tidak berubah** (agar consumer lama tetap jalan):
- `protocol`: `"IEC61850"`
- `address`: IOA virtual
- `ca`: `0`
- `asdu`: `0`
- `type`: CDC (`DPS`, `MV`, ...)
- `action`: reason code (`DATA_CHANGE`, `QUALITY_CHANGE`, `DATA_UPDATE`, `INTEGRITY`, `GENERAL_INTERROGATION`, `POLLING`)
- `time`: dari atribut `t`; `time_desc`: `"RTU"`, atau `"SERVER"` kalau `t` tidak ada

Field tambahan opsional (`object_reference`) **hanya** ditambahkan setelah saya konfirmasi bahwa consumer tidak keberatan.

Nilai DPS (`Pos.stVal`): `intermediate-state`=0, `off`=1, `on`=2, `bad-state`=3. Konversi ke nilai yang dipakai sistem sekarang **ditanyakan ke saya dulu** (IEC 104 DPI: 1=OFF/OPEN, 2=ON/CLOSE, kebetulan sama).

### 3.6 Command (event `COMMAND` di `RTU_EVENTS`)
- Lookup `(id_rtu, ioa)` → `object_reference` kontrol + `ctl_model`.
- `SELECT_RTU` → `select` / `selectWithValue` (SBO). Operate → `operate`. Cancel → `cancel`.
- Pakai `orCat = remote-control` dan `orIdent` = user dari payload Kafka.
- Respons sukses/gagal dikirim dengan format respons yang sama seperti versi IEC 104 (`command/response.rs`). Untuk enhanced security, tunggu `CommandTermination`.

---

## 4. Lingkungan development (Docker)

`docker-compose.dev.yml` berisi:
- `dev`: image `rust:slim-bookworm` + build-essential, cmake, clang, libclang-dev, pkg-config, libssl-dev, libsasl2-dev. Repo di-mount ke `/work`, target dan cargo registry pakai named volume.
- `kafka`: Kafka mode KRaft, satu node, listener bisa diakses dari container `dev`.
- `postgres`: Postgres 16, DB `ICONICS_DEV`, berisi skema minimal hasil migrasi dan data contoh.
- `ied-sim-1`, `ied-sim-2`, ...: contoh server dari libIEC61850 sebagai IED simulasi. Untuk uji 100 IED, buat skrip yang menjalankan banyak instance di port berbeda.

Jaringan memakai bridge biasa, **bukan** `network_mode: host`.

---

## 5. Kualitas kode
- Ikuti gaya repo yang ada (struktur modul, `tracing`, error type).
- Unit test untuk mapper (quality, CDC, reason code, deadband, IOA mapping).
- `cargo clippy` dan `cargo test` harus lolos di setiap fase.
- Tidak ada `unwrap()` di jalur runtime. Semua `unsafe` FFI diberi komentar singkat.

---

## 6. Fase pengerjaan

| Fase | Isi | Kriteria selesai |
|---|---|---|
| **1** | `docker-compose.dev.yml` + simulator IED (contoh server libIEC61850) | Simulator jalan, bisa di-browse pakai IED Explorer dari Windows |
| **2** | Proyek kecil `iec61850-hello/`: `build.rs` + bindgen libIEC61850, konek ke simulator, baca **satu** nilai | `cargo run` menampilkan nilai dari simulator |
| **3** | Browse model: daftar LD, LN, DO, dan dataset | Model simulator tampil rapi di terminal |
| **4** | **Polling**: baca dataset/LN berkala, deteksi perubahan dan deadband | Nilai berubah di simulator → terdeteksi dan dicetak |
| **5** | **Report**: enable RCB, terima report, GI, integrity | Perubahan di simulator langsung diterima tanpa polling |
| **6** | Migrasi SQL usulan (bagian 3) + data contoh + loader mapping IOA | Mapping termuat dari Postgres lokal |
| **7** | Publish ke Kafka `RTU_DATAS` & `DATAPOINTS` dengan `DataMessage` yang sama, memakai ulang `kafka/` dan `resolver/` | Pesan terlihat di Kafka lokal dengan format sama seperti IEC 104 |
| **8** | Integrasi ke struktur repo: modul `iec61850/`, connection manager, reconnect, ping, `RTU_STATUS` & `RTU_LOGGING`, consumer semua topik di bagian 1 (abaikan protocol lain), `acquisition_mode` per IED | Banyak IED jalan bersamaan; status online/offline terkirim ke `RTU_STATUS` saat simulator dimatikan/dinyalakan |
| **9** | Command: select/operate/cancel dari event `COMMAND` + kirim respons command, manual set + `MANUAL_SET_EVENT_RESPONSE` | Breaker di simulator bisa open/close dari payload Kafka, dan respons sukses/gagal muncul di Kafka |
| **10** | Tool import SCL (ICD/CID/SCD) → generate mapping + IOA otomatis (rentang per jenis titik) → file SQL/CSV untuk di-review | Mapping simulator ter-generate otomatis |
| **11** | Uji beban **100 IED simulasi**, ukur CPU/memori/latensi; Dockerfile produksi + manifest Kubernetes | Laporan hasil uji beban |

Mulai dari **Fase 1**.