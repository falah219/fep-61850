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
