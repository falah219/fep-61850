# FEP IEC 61850 (MMS Client)

Rencana kerja lengkap: [docs/IEC61850_PLAN.md](docs/IEC61850_PLAN.md).
Acuan arsitektur: repo `iconics-fep-iec104`.

## Fase 1 — Lingkungan dev + simulator IED

Isi:
- `docker/Dockerfile.dev` — build **libIEC61850 v1.6.2.1** sekali, lalu dipakai oleh:
  - stage `dev`: Rust + clang/bindgen + libIEC61850 (untuk fase 2 dst.)
  - stage `ied-sim`: contoh server `server_example_basic_io` sebagai IED simulasi
- `docker-compose.dev.yml` — service `dev`, `ied-sim-1`, `ied-sim-2` (jaringan bridge).
  Kafka & Postgres tidak disertakan.

Model simulator (`server_example_basic_io`):
- LD `simpleIOGenericIO`, LN `GGIO1`
- `AnIn1..4` (MV, berubah tiap 100 ms, gelombang sinus)
- `SPCSO1..4` (SPC, bisa dikontrol), `Ind1..4` (SPS)
- Maksimal 2 koneksi MMS per simulator.

### Cara uji

```powershell
docker compose -f docker-compose.dev.yml up -d --build
docker compose -f docker-compose.dev.yml ps
docker compose -f docker-compose.dev.yml logs ied-sim-1
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
```

## Fase 2 — `iec61850-hello`: konek + baca satu nilai

Proyek kecil di [iec61850-hello/](iec61850-hello/):
- `build.rs` — link statis `libiec61850.a` + `libhal.a` dari `/usr/local/lib` (di image `dev`),
  lalu bindgen dari `wrapper.h` → `bindings.rs` (pola sama dengan FEP IEC 104).
- `src/ffi.rs` — include hasil bindgen.
- `src/main.rs` — `IedConnection_connect` → `IedConnection_readFloatValue` (FC `MX`) → close.

Image `dev` harus di-build ulang karena Dockerfile diperbaiki (build semua target libIEC61850):

```powershell
docker compose -f docker-compose.dev.yml up -d --build
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run
# argumen opsional: host port object_reference
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run -- ied-sim-2 102 simpleIOGenericIO/GGIO1.AnIn2.mag.f
```

Hasil yang diharapkan:

```
terkoneksi ke ied-sim-1:102
simpleIOGenericIO/GGIO1.AnIn1.mag.f [MX] = 0.84147096
```

Jalankan beberapa kali: nilainya berubah karena simulator mengisi gelombang sinus.

## Fase 3 — Browse model IED

Binary baru `browse` di [iec61850-hello/src/bin/browse.rs](iec61850-hello/src/bin/browse.rs).
Kode FFI bersama dipindah ke [iec61850-hello/src/lib.rs](iec61850-hello/src/lib.rs):
`Ied` (koneksi, otomatis close/destroy saat keluar scope) + helper `LinkedList` → `Vec<String>`.

Yang ditampilkan per LN: DO beserta atribut & FC-nya (`stVal[ST]`, `ctlModel[CF]`, ...),
dataset beserta anggotanya, dan RCB (`.BR.` = buffered, `.RP.` = unbuffered).

```powershell
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run --bin browse
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run --bin browse -- ied-sim-2 102
```

Contoh potongan hasil:

```
LD  simpleIOGenericIO
  LN  GGIO1
      DO  AnIn1        mag[MX] q[MX] t[MX]
      DO  SPCSO1       stVal[ST] q[ST] Oper[CO] ctlModel[CF] t[ST]
  LN  LLN0
      DS  Events (tetap, 4 anggota)
            simpleIOGenericIO/GGIO1.SPCSO1.stVal[ST]
      RCB simpleIOGenericIO/LLN0.RP.EventsRCB01
```

## Fase 4 — Polling dataset

Binary `poll` di [iec61850-hello/src/bin/poll.rs](iec61850-hello/src/bin/poll.rs),
logika di [iec61850-hello/src/lib.rs](iec61850-hello/src/lib.rs):
- `DataSetReader`: baca **seluruh dataset dalam satu request MMS** (`IedConnection_readDataSetValues`),
  `ClientDataSet` dipakai ulang tiap siklus.
- `changed()`: float → kirim jika selisih dari nilai terakhir yang dikirim > `DEADBAND`;
  status/quality/struktur → kirim jika berubah.
- Setiap `INTEGRITY_PERIOD_MS` semua nilai dicetak ulang dengan action `INTEGRITY`.
- `next_deadline()`: jadwal tetap; kalau siklus terlambat, siklus yang terlewat **dilewati** + warning.

Default dataset: `simpleIOGenericIO/LLN0.Measurements` (AnIn1..4) dan `simpleIOGenericIO/LLN0.Events2` (SPCSO1..4).

```powershell
# unit test (deadband, perubahan status/quality, jadwal)
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo test
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo clippy --all-targets

# polling (Ctrl+C untuk berhenti)
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run --bin poll

# uji deadband: nilai sinus hanya -1..1, jadi DEADBAND=5 → hanya muncul saat INTEGRITY
docker compose -f docker-compose.dev.yml exec -e DEADBAND=5 -e INTEGRITY_PERIOD_MS=10000 -w /work/iec61850-hello dev cargo run --bin poll
```

## Fase 5 — Report (RCB)

Binary `report` di [iec61850-hello/src/bin/report.rs](iec61850-hello/src/bin/report.rs),
pembungkus RCB (`Rcb`) + `parse_trg_ops()` + `reason_action()` di [iec61850-hello/src/lib.rs](iec61850-hello/src/lib.rs).

Alur per RCB:
1. `getRCBValues` → ambil dataset yang terpasang di RCB (tidak diubah) + daftar anggotanya.
2. `installReportHandler` → callback `on_report` (dibungkus `catch_unwind`, jalan di thread libIEC61850).
3. `setRCBValues`: `RptEna=true` + `TrgOps` + `IntgPd` (+ `Resv` untuk URCB). Parameter kosong **tidak ditulis**.
4. GI langsung setelah enable, lalu tiap `GI_INTERVAL_SECS`.

Reason code → `action`: `GENERAL_INTERROGATION` > `INTEGRITY` > `DATA_CHANGE` > `QUALITY_CHANGE` > `DATA_UPDATE`.

Default RCB: `LLN0.BR.EventsBRCB01` (dataset `Events`, SPCSO1..4.stVal) dan `LLN0.BR.Measurements01` (dataset `Measurements`).

```powershell
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo test
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo run --bin report

# lebih tenang: tanpa data-change, hanya integrity tiap 5 detik + GI
docker compose -f docker-compose.dev.yml exec -e TRG_OPS=integrity,gi -e INTG_PD_MS=5000 -w /work/iec61850-hello dev cargo run --bin report

# pakai setting TrgOps/IntgPd bawaan IED (tidak menulis apa pun selain RptEna)
docker compose -f docker-compose.dev.yml exec -e TRG_OPS= -e INTG_PD_MS= -w /work/iec61850-hello dev cargo run --bin report
```

Catatan: AnIn1..4 berubah tiap 100 ms, jadi dengan `dchg` aktif report measurement akan sangat ramai.

## Fase 6 — Migrasi SQL usulan + loader mapping IOA

> **USULAN, belum diuji ke Postgres mana pun. Harus di-review tim sebelum dipakai di lingkungan bersama.**
> Jangan jalankan ke DB produksi/kantor.

- [migrations/0001_iec61850.up.sql](migrations/0001_iec61850.up.sql) / [down](migrations/0001_iec61850.down.sql):
  - `iec61850s`: kolom form **"Add Client" IEC 61850 di GRITA** + konvensi `iec104s` (PK `id` uuid,
    `status_rtu` 'green'/'red', `longitude_x`/`latitude_y`, FK `master_stations`/`data_gi`).
  - `iec61850_report_controls` (RCB), `master_datapoint_iec61850s` (ekstensi 1:1).
  - Unik `(id_rtu, object_reference, FC)` lewat trigger (id_rtu ada di `master_datapoints`).
  - Index unik parsial `master_datapoints (id_rtu, ioa) WHERE protocol='IEC61850' AND is_deleted=false`.
  - Daftar hal yang perlu dikonfirmasi ada di header file.
- [migrations/0002_iec61850_telco_status.sql](migrations/0002_iec61850_telco_status.sql): trigger ke `telco_statuses`
  (perlu `'iec61850s'` ditambahkan ke `sync_telco_status()` di repo backend).
- [migrations/seed_simulator.sql](migrations/seed_simulator.sql): data contoh untuk `ied-sim-1` (RTU 9001, REPORT)
  dan `ied-sim-2` (RTU 9002, POLLING). Rentang IOA: 1001.. status, 2001.. measurement, 3001.. kontrol.
- [iec61850-hello/src/mapping.rs](iec61850-hello/src/mapping.rs): `PointMap`
  - `by_ioa(id_rtu, ioa)` → untuk command
  - `by_reference(id_rtu, ref, fc)` → untuk data masuk; menerima DO/SDO/DA, prefix terpanjang menang
    (`LD/GGIO1.AnIn1.mag.f` → titik `AnIn1`, sisa `mag.f`)
  - menolak IOA ganda dan `(reference, FC)` ganda per RTU
- [iec61850-hello/src/bin/mapping.rs](iec61850-hello/src/bin/mapping.rs): muat dari Postgres lalu tampilkan.

```powershell
# unit test mapping (tanpa DB)
docker compose -f docker-compose.dev.yml exec -w /work/iec61850-hello dev cargo test

# urutan di DB lokal/dev (yang sudah punya tabel master_datapoints):
#   psql -f migrations/0001_iec61850.up.sql
#   psql -f migrations/seed_simulator.sql
docker compose -f docker-compose.dev.yml exec -e POSTGRE_HOST=host.docker.internal -e POSTGRE_PORT=5432 `
  -e DB_USERNAME=postgres -e DB_PASSWORD=... -e DB_DATABASE=ICONICS_DEV `
  -w /work/iec61850-hello dev cargo run --bin mapping
```
