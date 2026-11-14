# FEP IEC 61850 (MMS Client)

Rencana kerja lengkap: [docs/IEC61850_PLAN.md](docs/IEC61850_PLAN.md).
Acuan arsitektur: repo `iconics-fep-iec104`.

## Fase 1 — Lingkungan dev + simulator IED

Isi:
- `docker/Dockerfile.dev` — build **libIEC61850 v1.6.2.1** sekali dari source di [native/libiec61850/](native/libiec61850/)
  (tanpa unduh dari internet; versi, sumber & perubahan di [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)), lalu dipakai oleh:
  - stage `dev`: Rust + clang/bindgen + libIEC61850 (untuk fase 2 dst.)
  - stage `ied-sim`: contoh server `server_example_basic_io` sebagai IED simulasi
- `docker-compose.yml` — jalankan FEP dari image produksi (pola sama dengan FEP IEC 104): `.env`,
  `network_mode: host`, `docker compose up -d --build`.
- `docker-compose.dev.yml` — hanya service `dev` (toolchain). Kafka, Postgres, dan simulator tidak disertakan.
- `docker-compose.sim.yml` — **opsional**: simulator `ied-sim-1`, `ied-sim-2`. Tidak perlu kalau sudah memakai
  service simulator sendiri (sesuaikan `ip_address`/`port` di seed). `docker-compose.sim20.yml` = 20 simulator.

Model simulator (`server_example_basic_io`):
- LD `simpleIOGenericIO`, LN `GGIO1`
- `AnIn1..4` (MV, berubah tiap 100 ms, gelombang sinus)
- `SPCSO1..4` (SPC, bisa dikontrol), `Ind1..4` (SPS)
- Maksimal 2 koneksi MMS per simulator.

### Cara uji

```powershell
docker compose -f docker-compose.dev.yml up -d --build      # toolchain saja
docker compose -f docker-compose.sim.yml up -d --build      # opsional: 2 simulator
docker compose -f docker-compose.sim.yml ps
docker compose -f docker-compose.sim.yml logs ied-sim-1
```

Cek dari dalam container `dev` (pakai `mms_utility` bawaan libIEC61850):

```powershell
docker compose -f docker-compose.dev.yml exec dev mms_utility -h ied-sim-1 -i   # identitas server
docker compose -f docker-compose.dev.yml exec dev mms_utility -h ied-sim-1 -d   # daftar domain (LD)
```

Dari Windows pakai **IED Explorer**: host `127.0.0.1`, port `10102` (sim-1) atau `10103` (sim-2).

Matikan:

```powershell
docker compose -f docker-compose.dev.yml down
docker compose -f docker-compose.sim.yml down
```

## Alat bantu — `tools/iec61850-cli` (commissioning IED)

Crate terpisah di [tools/iec61850-cli/](tools/iec61850-cli/) (bukan bagian dari image FEP, tidak ikut
`docker build`). Isinya dua binary yang **hanya membaca** dari IED (tidak menulis RCB, tidak mengirim
command). Keduanya berasal dari fase 2–5 (konek, browse, polling); binary latihan (baca satu nilai,
report, loader mapping) sudah dihapus karena fungsinya sudah ada di FEP.

| Binary | Untuk apa |
|---|---|
| `browse` | Tampilkan model IED: LD → LN → DO (+ atribut & FC), dataset + anggotanya, RCB (`.BR.` buffered, `.RP.` unbuffered). Bahan untuk mengisi `object_reference`, `functional_constraint`, `dataset_reference`, `rcb_reference`. |
| `poll` | Baca dataset berkala (satu request MMS per dataset), tampilkan nilai yang berubah (deadband) + integrity. Untuk memastikan dataset terbaca dan nilainya masuk akal sebelum dimapping. |

Dijalankan dari container `dev` (butuh libIEC61850 di image itu):

```powershell
# simulator lokal (default host ied-sim-1 port 102)
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev cargo run --bin browse
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev cargo run --bin poll

# test & clippy
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev cargo test
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev cargo clippy --all-targets
```

### Ke IED sungguhan

Container `dev` harus bisa menjangkau IP IED (port 102). Contoh IED `192.168.10.21`, LD `BAY1LD0`:

```powershell
# 1. Model IED -> simpan ke file untuk bahan mapping
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev `
  cargo run --release --bin browse -- 192.168.10.21 102 > browse-bay1.txt

# 2. Pastikan dataset terbaca (nama dataset dari hasil browse, format LD/LN.Dataset)
docker compose -f docker-compose.dev.yml exec -w /work/tools/iec61850-cli dev `
  cargo run --release --bin poll -- 192.168.10.21 102 BAY1LD0/LLN0.dsStatus BAY1LD0/LLN0.dsMeas

# deadband 0.5 untuk measurement, cetak ulang semua nilai tiap 10 detik
docker compose -f docker-compose.dev.yml exec -e DEADBAND=0.5 -e INTEGRITY_PERIOD_MS=10000 `
  -w /work/tools/iec61850-cli dev cargo run --release --bin poll -- 192.168.10.21 102 BAY1LD0/LLN0.dsMeas
```

Contoh potongan hasil `browse`:

```
LD  BAY1LD0
  LN  XCBR1
      DO  Pos          stVal[ST] q[ST] t[ST] Oper[CO] ctlModel[CF]
  LN  LLN0
      DS  dsStatus (tetap, 12 anggota)
            BAY1LD0/XCBR1.Pos.stVal[ST]
      RCB BAY1LD0/LLN0.BR.brcbST01
```

Perhatian saat ke IED sungguhan:
- Setiap alat memakai **satu asosiasi MMS**. Banyak IED membatasi jumlah client (sering 2–6);
  kalau FEP + SCADA lain sudah konek, alat bisa ditolak (`connection rejected`) atau malah merebut slot.
- `browse` mengirim banyak request (satu per LN/DO). Jalankan sekali dan simpan hasilnya, jangan berulang
  ke IED yang sedang beroperasi.
- `poll` membaca dataset dengan `readDataSetValues`; dataset harus sudah ada di IED (dari SCL/engineering tool).
- Untuk usulan mapping otomatis dari file SCL (ICD/CID/SCD) pakai `scl-import` (fase 10), bukan alat ini.

## Fase 6 — Migrasi SQL usulan + loader mapping IOA

> **USULAN, belum diuji ke Postgres mana pun. Harus di-review tim sebelum dipakai di lingkungan bersama.**
> Jangan jalankan ke DB produksi/kantor.

- [migrations/0001_iec61850.up.sql](migrations/0001_iec61850.up.sql) / [down](migrations/0001_iec61850.down.sql):
  - `iec61850s`: kolom form **"Add Client" IEC 61850 di GRITA** + konvensi `iec104s` (PK `id` uuid,
    `status_rtu` 'green'/'red', `longitude_x`/`latitude_y`, FK `master_stations`/`data_gi`).
  - `iec61850_report_controls` (RCB), `master_datapoint_iec61850s` (ekstensi 1:1).
  - `iec61850_command_formulas`: setting kontrol per titik (`ctl_model`, `interlock_check`, `synchro_check`),
    pola `iec104_command_formulas` remote control GRITA. Maks satu baris aktif per titik.
  - Trigger CDC `notify_table_change()` (milik backend / cdc_listener) di keempat tabel. Test env:
    jalankan dulu [test-env/sql/00_stub_backend.sql](test-env/sql/00_stub_backend.sql) (tabel tiruan backend +
    fungsi trigger kosong, HANYA DB uji kosong). Uji urutan stub → 0001 → 0002 → seed → down → up:
    `scripts/uji_migrasi.sh` (container `postgres:16` sementara).
  - Unik `(id_rtu, object_reference, FC)` lewat trigger (id_rtu ada di `master_datapoints`).
  - Index unik parsial `master_datapoints (id_rtu, ioa) WHERE protocol='IEC61850' AND is_deleted=false`.
  - Daftar hal yang perlu dikonfirmasi ada di header file.
- [migrations/0002_iec61850_telco_status.sql](migrations/0002_iec61850_telco_status.sql): trigger ke `telco_statuses`
  (perlu `'iec61850s'` ditambahkan ke `sync_telco_status()` di repo backend).
- [migrations/seed_simulator.sql](migrations/seed_simulator.sql): data contoh untuk `ied-sim-1` (RTU 9001, REPORT)
  dan `ied-sim-2` (RTU 9002, POLLING). Rentang IOA: 1001.. status, 2001.. measurement, 3001.. kontrol.
- [src/iec61850/mapping.rs](src/iec61850/mapping.rs): `PointMap`
  - `by_ioa(id_rtu, ioa)` → untuk command
  - `by_reference(id_rtu, ref, fc)` → untuk data masuk; menerima DO/SDO/DA, prefix terpanjang menang
    (`LD/GGIO1.AnIn1.mag.f` → titik `AnIn1`, sisa `mag.f`)
  - menolak IOA ganda dan `(reference, FC)` ganda per RTU
- Loader dari Postgres: `load_points` di [src/repository.rs](src/repository.rs) (dipakai FEP saat start/reload).

```powershell
# unit test mapping (tanpa DB)
docker compose -f docker-compose.dev.yml exec -w /work dev cargo test mapping

# urutan di DB lokal/dev (yang sudah punya tabel master_datapoints):
#   psql -f migrations/0001_iec61850.up.sql
#   psql -f migrations/seed_simulator.sql
```

## Fase 7 — Crate FEP + publish ke Kafka (`RTU_DATAS`, `DATAPOINTS`)

> Ditulis **tanpa uji ke Kafka/Postgres** (keduanya tidak ada di compose). Yang bisa diuji sekarang: `cargo test`, `cargo clippy`.

Crate utama di root repo (`fep-iec61850`). Alat bantu commissioning ada di `tools/iec61850-cli/`.

Log (`src/logger.rs`, format sama dengan FEP IEC 104):
- Console selalu; warna hanya kalau stdout terminal (tidak ada kode warna di `docker logs` / `kubectl logs`).
- File harian `logs/iec61850.YYYY-MM-DD` kalau `LOG_TO_FILE=true` (default). `LOG_MAX_FILES` (default 14)
  = jumlah file yang disimpan, lebih lama dihapus, 0 = tanpa batas. Di Kubernetes `LOG_TO_FILE=false`.
  Folder `logs/` tidak bisa ditulis -> hanya console + satu WARN, FEP tetap jalan.
- Log & error librdkafka (broker tidak terjangkau, SASL gagal, ...) ikut masuk lewat
  `src/kafka/context.rs` dengan target `librdkafka`.
- `docker-compose.yml` me-mount `./logs:/app/logs`.

Struktur folder mengikuti `iconics-fep-iec104`:

```
src/
  main.rs                muat konfigurasi DB -> 1 thread per IED -> publish; shutdown
  logger.rs              log console + file harian logs/iec61850.YYYY-MM-DD (format sama dengan IEC 104)
  error.rs               Iec61850Error: Mms { code, msg } (kode IedClientError), InvalidConfiguration, Database, Other
  config/                env Kafka, DB, runtime (nama env sama dengan FEP IEC 104)
  domain/                RtuConfig, Point, PointState, command (CommandAction, CommandReq, ctl_value)
  connection/
    manager.rs           start/stop/reload IED, debounce reload, shutdown
    health_check.rs      heartbeat RTU_STATUS
    network_ping.rs      ping ICMP per IED
    reconnection.rs      loop reconnect per IED (run_ied)
    gate.rs              batas connect serentak (MAX_CONCURRENT_CONNECTS)
  iec61850/
    client.rs            koneksi (timeout, IP lokal, OSI), RCB, dataset, variable spec, readObject, control
    acquisition.rs       sesi: report + polling, liveness, command; gabung value/q/t per titik
    callbacks/report.rs  callback report libIEC61850 (EntryID, waktu entri)
    mapping.rs           PointMap
    mapper.rs            quality/flag/reason/nilai -> field Kafka
  kafka/
    producer.rs          producer
    context.rs           log & error librdkafka -> tracing
    consumer.rs          consumer
    models.rs            DataMessage + pesan status/logging/event (salinan bentuk IEC 104)
    event_handler.rs     pembagi event ke handler (abaikan protocol selain IEC61850)
    handlers/            rtu_event/ (+ command.rs), master_datapoint, master_rtu_event, manual_set, capture_data_change
    publisher/           message_builder.rs: RTU_DATAS selalu, DATAPOINTS jika ada tag & bukan manual set
  repository/            pool.rs, rtu_repository.rs (IED, RCB, mapping), resolver_repository.rs (tag, formula, ...)
  resolver/              SALINAN dari FEP IEC 104 (tag, formula, group, general_action)
  utils/timestamp.rs     waktu pesan (atribut t -> waktu entri report -> jam server)
  scl.rs + bin/          scl-import (fase 10)
```

Aturan yang dipakai (hasil konfirmasi):

| Field | Isi |
|---|---|
| `protocol` / `ca` / `asdu` | `"IEC61850"` / `0` / `0` |
| `address` | IOA virtual |
| `type` | CDC (`DPS`, `MV`, ...) |
| `value` | angka; boolean → 0/1; DPS → 0..3 apa adanya (1=OFF, 2=ON) |
| `action` | `GENERAL_INTERROGATION` > `INTEGRITY` > `DATA_CHANGE` > `QUALITY_CHANGE` > `DATA_UPDATE`; polling: `POLLING` / `INTEGRITY` |
| `quality` | urutan IEC 104: `INVALID` > `SUBSTITUTED` > `QUESTIONABLE` > `GOOD` |
| `flag` | detailQual + `SUBSTITUTED`/`TEST`/`OPERATOR_BLOCKED` + `CLOCK_NOT_SYNC`, dipisah `\|`, kosong = `GOOD` |
| `group` | `master_data_objects.initial` = CDC, `protocol = 'IEC61850'` |
| `general_action` | `master_general_actions` dengan `protocol='IEC61850'`, kunci = action |
| `time` / `time_desc` | atribut `t` (sudah UTC, **tidak** dikoreksi `TIMEZONE`) / `RTU`; tanpa `t` → waktu server / `SERVER` |

Akuisisi per mode (`iec61850s.acquisition_mode`):
- `REPORT`: RCB dari `iec61850_report_controls`; GI setelah konek + tiap `general_interrogation_interval` (0 = hanya GI awal).
  RCB yang gagal di-setup dicoba ulang tiap `RCB_RETRY_SECS` (env, default 30).
- `POLLING`: satu `readObject` per **(LN, FC)** yang punya titik termapping, tiap `poll_interval_ms`;
  kirim jika berubah (deadband per titik), kirim ulang semua tiap `INTEGRITY_PERIOD_MS` (env).
- `HYBRID`: report untuk RCB + polling hanya titik `MX`.

Reload karena `MASTER_DATAPOINT_EVENT` / CDC digabung per `id_rtu` selama `RELOAD_DEBOUNCE_MS` (env, default 2000).

BRCB (`.BR.`):
- `ResvTms` ditulis `BRCB_RESV_TMS_SECS` (default 20, harus < `RCB_RETRY_SECS`) kalau IED punya elemen itu: RCB tetap dipesan untuk FEP selama putus.
- EntryID report terakhir disimpan di memori per RCB. Saat reconnect EntryID ditulis ke IED, jadi event selama putus
  dikirim ulang tanpa dobel. Ditolak IED -> enable tanpa EntryID + WARN di `RTU_LOGGING`.
  EntryID hilang saat reload RTU / restart FEP.
- Saat enable hanya elemen yang BERBEDA dari IED yang ditulis (menulis ulang DatSet bisa mengosongkan buffer).
  OptFlds selalu memuat reason, time-stamp, dan entryID (BRCB).

Waktu (`time` / `time_desc`): atribut `t` -> waktu entri report (`RTU`) -> jam server (`SERVER`).

Shutdown: semua thread IED ditunggu maks `SHUTDOWN_WAIT_SECS` (default 5), lalu `RTU_STATUS=0` dikirim untuk semua IED, baru flush Kafka.

Parameter OSI (`OSI_PARAMS_SIDE` = `none` | `local` | `remote`, default `none`): selain `none`, `ap_title`, `ae_qualifier`,
`p/s/t_selector` dari `iec61850s` dipasang ke sisi itu. Nilai tidak valid -> default libIEC61850 + ERROR di `RTU_LOGGING`.
Saat reload/stop/shutdown, thread IED lama ditunggu berhenti (maks 15 detik) sebelum konek ulang.

```powershell
docker compose -f docker-compose.dev.yml exec dev cargo test
docker compose -f docker-compose.dev.yml exec dev cargo clippy --all-targets

# butuh Postgres (migrasi + seed) & Kafka lokal; salin .env.example -> .env lalu sesuaikan
docker compose -f docker-compose.dev.yml exec dev cargo run
```

## Fase 8 — Connection manager, status, consumer event

> Ditulis **tanpa uji ke Kafka/Postgres**. Yang bisa diuji sekarang: `cargo test`, `cargo clippy`.

File baru: [src/manager.rs](src/manager.rs), [src/handlers.rs](src/handlers.rs), [src/kafka/consumer.rs](src/kafka/consumer.rs), [src/kafka/models.rs](src/kafka/models.rs).

- **Satu thread per IED** dengan saluran kontrol `Stop` / `Gi` / `Wake` (`acquisition.rs`).
  Jeda reconnect = `reconnect_time_ms`, `reconnect_retry_count` 0 = tanpa batas.
- **`MAX_CONCURRENT_CONNECTS`** (default 4): gate yang membatasi percobaan konek serentak.
- **Koneksi mati**: request MMS yang gagal dengan `CONNECTION_LOST` / `NOT_CONNECTED` (RCB, polling, GI,
  command) langsung mengakhiri sesi lalu reconnect; RCB tidak dicoba terus di koneksi yang sama.
- **Liveness**: tanpa request sukses / report masuk selama `LIVENESS_SECS` (default 30), FEP membaca
  `<LD pertama>/LLN0.Mod.stVal` [ST]. Gagal (timeout / koneksi mati) -> sesi diakhiri, reconnect.
  IED yang menjawab dengan error (mis. objek tidak ada) tetap dianggap hidup.
- **Polling timeout**: satu `readObject` TIMEOUT -> sisa grup di siklus itu dilewati (WARN sekali) dan
  liveness dicek saat itu juga; gagal -> reconnect. IED hang terdeteksi dalam 2 x `request_timeout_ms`.
- **COMMAND**: dimasukkan ke antrean IED langsung saat event diterima (urutan kedatangan);
  hanya menunggu hasil + respons `RTU_COMMAND` yang jalan di task terpisah.

Uji koneksi serentak (20 simulator, FEP start 10 kali, RCB harus aktif dalam 60 detik):
```bash
docker compose -f docker-compose.sim20.yml up -d --build
psql ... -f migrations/seed_sim20.sql      # DB dev, RTU 9101..9120
docker compose -f docker-compose.dev.yml exec -w /work dev scripts/uji_konek_serentak.sh 10 90
```
- **`RTU_STATUS`**: 1 saat sesi MMS aktif, 0 saat putus/gagal (sekali per rangkaian gagal);
  heartbeat tiap `STATUS_HEARTBEAT_INTERVAL_SECS`; status sistem 1 saat start, 0 saat stop;
  status per channel dari ping ICMP (hanya IP literal, butuh root/CAP_NET_RAW).
- **`RTU_LOGGING`**: konek, putus, gagal konek, RCB gagal (`[LEVEL] pesan`).
- Peringatan konfigurasi (WARN, log + `RTU_LOGGING`, sekali per sesi setelah konek): mode REPORT/HYBRID tanpa RCB
  `enabled`, dan datapoint `protocol = 'IEC61850'` tanpa baris `master_datapoint_iec61850s` (IOA disebut, maks 20).
- **Consumer** (`GROUP_ID` default `IEC61850_GROUP`), pesan dengan `protocol` ≠ `IEC61850` diabaikan:

| Topik / event | Aksi |
|---|---|
| `RTU_EVENTS` `UPDATE`/`CREATE`/`ADD` | baca ulang RTU dari DB, restart thread |
| `RTU_EVENTS` `DELETE`/`REMOVE` | hentikan thread |
| `RTU_EVENTS` `STATE` | `false`: hentikan + status 0; `true`: baca ulang + konek |
| `RTU_EVENTS` `RESET` | status 0, restart |
| `RTU_EVENTS` `GI` | GI ke semua RCB; mode polling: siklus integrity sekarang |
| `RTU_EVENTS` `CHANNEL` | dicatat (sama dengan IEC 104) |
| `RTU_EVENTS` `COMMAND` | select / operate / cancel (fase 9) |
| `RTU_EVENTS` `BROWSE` | browse model IED di thread IED → `IEC61850_BROWSE_RESPONSE` (lihat di bawah) |
| `MASTER_DATAPOINT_EVENT` | update TagResolver + restart IED (mapping bisa berubah) |
| `MASTER_DATA_OBJECT_EVENT` | muat ulang group |
| `MASTER_RTU_EVENTS` `UPDATE_ID_RTU` | hentikan id lama, jalankan id baru |
| `RTU_COMMAND_CONFIG_REQUEST` | dicatat (sama dengan IEC 104) |
| `CAPTURE_DATA_CHANGE` | `formulas`, `master_data_objects`, `master_general_actions` → muat ulang resolver; `iec61850s`, `iec61850_report_controls`, `master_datapoint_iec61850s`, `iec61850_command_formulas` → restart IED terkait |
| `MANUAL_SET_EVENT` | toggle manual set + `MANUAL_SET_EVENT_RESPONSE` (key `IEC61850`) |

Uji (di kantor, Postgres + Kafka dev):
1. `cargo run` → `RTU_STATUS` berisi `{"ip_address":"system","id_rtu":0,...,"status":1}` lalu status 1 untuk RTU 9001 & 9002.
2. `docker compose -f docker-compose.sim.yml stop ied-sim-1` → status 0 RTU 9001 + log `koneksi MMS terputus` di `RTU_LOGGING`.
3. `docker compose -f docker-compose.sim.yml start ied-sim-1` → status 1 lagi dalam ±`reconnect_time_ms`.
4. Kirim ke `RTU_EVENTS`: `{"event":"GI","protocol":"IEC61850","ip_address":"ied-sim-1","id_rtu":9001}` → data `GENERAL_INTERROGATION`.
5. Kirim `{"event":"STATE","protocol":"IEC61850","ip_address":"ied-sim-1","id_rtu":9001,"state_rtu":false}` → status 0, tidak reconnect.
6. Kirim event yang sama dengan `"protocol":"IEC104"` → harus diabaikan.

### Event `BROWSE`

Payload: `{"event":"BROWSE","protocol":"IEC61850","id_rtu":9001,"key":"k1"}`. Dijalankan di thread IED
(antrean `Ctl` seperti COMMAND): LD → LN → DO → FC (`ST`/`MX`/`CO`/`SP`), satu `getVariableSpecification`
per LN + FC. Kode: [src/iec61850/browse.rs](src/iec61850/browse.rs).

Respons ke `IEC61850_BROWSE_RESPONSE` (key = `key`):

```json
{"key":"k1","id_rtu":9001,"status":"SUCCESS","message":"152 objek","part":1,"total_parts":1,
 "objects":[{"object_reference":"simpleIOGenericIO/GGIO1.SPCSO1","fc":"ST","cdc":"SPC"}]}
```

- IED tidak terkoneksi / thread tidak jalan → `FAILED`, `"IED tidak terkoneksi"`, `objects: []`.
- Lebih dari ~900 KB → beberapa pesan, `part` 1..`total_parts` (key sama).
- `cdc` ditebak dari struktur DO (7-3): `stVal` boolean/bitstring/integer → SPS/DPS/INS (+`Oper` → SPC/DPC/INC),
  `general` → ACT/ACD, `actVal` → BCR, `valWTr`+`Oper` → BSC, `mxVal`+`Oper` → APC, `mag` → MV, `cVal` → CMV
  (sub-DO seperti `WYE.phsA` dicari sampai 3 level). ENS/ENC tidak bisa dibedakan dari INS/INC lewat MMS.
  Tidak dikenali → `null`.
- Selama browse, polling IED itu tertahan (report tetap diterima).

## Fase 9 — Command (select / operate / cancel)

> Ditulis **tanpa uji ke Kafka/Postgres**. Yang bisa diuji sekarang: `cargo test`, `cargo clippy`.

[src/command.rs](src/command.rs) + `Control` di [src/iec61850/client.rs](src/iec61850/client.rs).

Alur: event `COMMAND` di `RTU_EVENTS` → cari IED (`id_rtu` atau IP) → job dikirim ke thread IED →
`(id_rtu, ioa)` → titik `FC = CO` → `ControlObjectClient` (di-cache per IOA selama sesi) → respons ke `RTU_COMMAND`.

| `action` | IEC 61850 |
|---|---|
| `SELECT_RTU` | SBO normal: `select`; SBO enhanced: `selectWithValue`; direct: tidak ada aksi (sukses) |
| `OPERATE_RTU` | `operate`; enhanced security: tunggu `CommandTermination` (maks env `OPER_TIMEOUT_MS`, default 10 s) |
| `CANCEL_RTU` | `cancel` |

- `ctlModel`: dibaca dari IED, ditimpa `iec61850_command_formulas.ctl_model` kalau diisi.
- `orCat = remote-control`, `orIdent` = field `user` di payload (default `FEP-IEC61850`).
- `interlock_check` / `synchro_check` dari `iec61850_command_formulas` (join `id_master_datapoint`, `is_deleted = false`; tanpa baris = false).
- IOA dari field `ioa`, atau dari akhiran tag `NAMA_123` (sama dengan IEC 104).
- Nilai (`value` boleh integer, float, atau string angka): `SPC` value≠0 → true; `INC`/`ENC` bilangan bulat; **`DPC` 2 → CLOSE (true), 1 → OPEN (false)** (default, dibalik dengan env `DPC_CLOSE_VALUE=1`) — pemetaan standar DCS IEC 104,
  **belum dikonfirmasi tim** (lihat `ctl_value` di `domain/command.rs`). `BSC` Tcmd: 0 stop, 1 lower, 2 higher;
  `APC` float (`ctlVal.f`). `command_value` di respons tetap integer kalau nilainya bulat.
- Respons `RTU_COMMAND` sama dengan IEC 104: `{hostname, port, protocol, tag, command_type, command_value, qualifier,
  event: SELECT|OPERATE|CANCEL, status: active|inactive, msg, key, time}`, key = `key` payload.
- Hasil command juga dicatat ke `RTU_LOGGING`.

Uji (simulator: semua `SPCSO` = direct-with-normal-security; seed: IOA 3001..3004 = `SPCSO1..4` kontrol):

```json
{"event":"COMMAND","protocol":"IEC61850","ip_address":"ied-sim-1",
 "data":{"action":"OPERATE_RTU","ioa":3001,"value":1,"type":"SPC","id_rtu":9001,"key":"uji_on","user":"operator1"}}
```
→ `RTU_COMMAND` status `active`, lalu `RTU_DATAS` IOA 1001 (`SPCSO1` status) `DATA_CHANGE` value 1.
Ulangi dengan `"value":0` → value 0. IOA yang bukan titik kontrol (mis. 1001) → status `inactive`.

## Fase 10 — Import SCL → mapping + IOA otomatis

[src/scl.rs](src/scl.rs) + binary [src/bin/scl_import.rs](src/bin/scl_import.rs). Hasilnya **usulan untuk direview**, tidak langsung dipakai FEP.

- Baca `IED` → `LDevice` → `LN0`/`LN` → `DO` (lewat `DataTypeTemplates`). Nama LD = `ldName` atau `IED + inst`, nama LN = `prefix + lnClass + inst`.
- CDC didukung: status `SPS DPS INS ENS` (+ status dari `SPC DPC INC BSC`) → FC `ST`; `BCR` → `ST`;
  measurement `MV CMV APC` → FC `MX`; CDC komposit (`WYE`, `DEL`, ...) diturunkan ke SDO (`PhV.phsA` = CMV).
- Titik kontrol (FC `CO`) dibuat untuk `SPC DPC INC APC BSC` yang punya `Oper` dan `ctlModel` ≠ `status-only`
  (ctlModel dari `DOI/DAI` menimpa default `DOType`).
- IOA: status **1001..**, measurement **2001..**, kontrol **3001..** (maks 999 per jenis, lebih dari itu error).
- Tag usulan: reference dengan `/` `.` → `_`, kontrol diberi akhiran `_CO` (mis. `BAY1LD0_XCBR1_Pos_CO`).
- RCB dari `ReportControl` (instance pertama, `name01` jika indexed) ditulis sebagai **komentar** di SQL; aktifkan yang dipakai.
- SCD berisi banyak IED → sebutkan nama IED.

```powershell
# image dev perlu di-build ulang sekali (file CID simulator disalin ke image)
docker compose -f docker-compose.dev.yml up -d --build dev
docker compose -f docker-compose.dev.yml exec dev cargo test scl
docker compose -f docker-compose.dev.yml exec dev cargo run --bin scl_import -- /usr/local/share/libiec61850/simpleIO_direct_control.cid 9001
```

Hasil yang diharapkan untuk simulator: **22 titik** (14 status, 4 measurement, 4 kontrol `SPCSO1..4`), 6 RCB,
file `simpleIO_direct_control.9001.sql` dan `.csv` muncul di root repo (keduanya di-ignore git).

## Deploy — Dockerfile produksi + Kubernetes

> Uji beban 100 IED (fase 11) **ditunda** sampai service simulator siap.

- [Dockerfile](Dockerfile): build libIEC61850 v1.6.2.1 (statis, tanpa examples) + `cargo build --release`,
  runtime `debian:bookworm-slim`, user non-root dengan `cap_net_raw` (ping).
- [kubernetes/](kubernetes/) — pola `kubernetes/232` FEP IEC 104:
  - `statefulset.yaml`: **1 replika** (tidak boleh 2 pod konek ke IED yang sama), root + `NET_RAW` seperti IEC 104,
    headless service (FEP tidak punya API port).
  - `configmap.yaml`: isi `<...>` sesuai lingkungan. `GROUP_ID` = `IEC61850_FEP_GROUP` (beda dengan IEC 104).
  - `secret.example.yaml` → salin ke `secret.yaml` (di-ignore git), isi `DB_PASSWORD` & `SASL_PASSWORD`.
  - `install.sh [tag]`: build → push → apply → rollout.

```bash
# di server yang punya akses registry + kubectl
cp kubernetes/secret.example.yaml kubernetes/secret.yaml   # lalu isi
kubernetes/install.sh v0.1.0
kubectl logs -f -l app=fep-iec61850 -n fep-iec61850
```

Cek build image di Windows (tanpa deploy): `docker build -t fep-iec61850:dev .`
