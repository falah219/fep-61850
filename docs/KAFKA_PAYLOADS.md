# Payload Kafka FEP IEC 61850

Daftar lengkap pesan Kafka yang **dikonsumsi** dan **diproduksi** FEP IEC 61850 (`fep-iec61850`).
Sumber kebenaran: **kode** di branch `iec61850`, bukan README lama. Kalau kode berubah, dokumen ini ikut diubah.

- Semua contoh JSON ada juga sebagai file di [docs/examples/](examples/) (`consume/` dan `produce/`).
- Test `kafka::payload_examples` (+ test contoh di `rtu_event/command.rs` dan `rtu_event/browse.rs`) memastikan:
  - setiap contoh consume bisa dibaca struct yang dipakai handler;
  - setiap contoh produce sama dengan hasil serialize struct/fungsi pembuat pesan;
  - setiap file contoh dicantumkan apa adanya di dokumen ini.
  Contoh yang basi = `cargo test` gagal.
- Nilai `ip_address`, `id_rtu`, `key`, tag, UUID, dan waktu di contoh adalah **contoh**, bukan data lapangan.

Daftar isi:
1. [Ringkasan](#1-ringkasan)
2. [Topic yang dikonsumsi](#2-topic-yang-dikonsumsi)
3. [Topic yang diproduksi](#3-topic-yang-diproduksi)
4. [Perbandingan dengan FEP IEC 104](#4-perbandingan-dengan-fep-iec-104)
5. [Catatan ketidaksesuaian](#5-catatan-ketidaksesuaian)

---

## 1. Ringkasan

### 1.1 Semua topic

| Topic | Arah | Key pesan | Kapan | Handler / pembuat (path) |
|---|---|---|---|---|
| `RTU_EVENTS` | consume | (tidak dibaca) | Event RTU dari backend/GRITA: CREATE/ADD/UPDATE, DELETE/REMOVE, STATE, RESET, GI, CHANNEL, COMMAND, BROWSE | [event_handler.rs](../src/kafka/event_handler.rs) → [rtu_event/mod.rs](../src/kafka/handlers/rtu_event/mod.rs), [rtu_event/command.rs](../src/kafka/handlers/rtu_event/command.rs), [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs) |
| `MASTER_DATAPOINT_EVENT` | consume | (tidak dibaca) | Datapoint ditambah/diubah/dihapus | [master_datapoint.rs](../src/kafka/handlers/master_datapoint.rs) |
| `MASTER_DATA_OBJECT_EVENT` | consume | (tidak dibaca) | Master data object berubah | [capture_data_change.rs](../src/kafka/handlers/capture_data_change.rs) (`reload_groups`) |
| `CAPTURE_DATA_CHANGE` | consume | (tidak dibaca) | CDC tabel konfigurasi (`iconics_configuration_cdc_listener`) | [capture_data_change.rs](../src/kafka/handlers/capture_data_change.rs) |
| `MASTER_RTU_EVENTS` | consume | (tidak dibaca) | `id_rtu` sebuah RTU diganti | [master_rtu_event.rs](../src/kafka/handlers/master_rtu_event.rs) |
| `MANUAL_SET_EVENT` | consume | (tidak dibaca) | Manual set tag dinyalakan/dimatikan | [manual_set.rs](../src/kafka/handlers/manual_set.rs) |
| `RTU_COMMAND_CONFIG_REQUEST` | consume | (tidak dibaca) | Permintaan konfigurasi command | [event_handler.rs](../src/kafka/event_handler.rs) (hanya dicatat di log) |
| `RTU_DATAS` | produce | `id_rtu` | Setiap nilai titik dikirim (report, GI, integrity, polling) | [publisher/message_builder.rs](../src/kafka/publisher/message_builder.rs) |
| `DATAPOINTS` | produce | `id_rtu` | Sama dengan `RTU_DATAS`, hanya titik yang punya tag dan tidak manual set | [publisher/message_builder.rs](../src/kafka/publisher/message_builder.rs) |
| `RTU_STATUS` | produce | `id_rtu` (`"0"` untuk sistem) | Konek/putus MMS, heartbeat, ping channel, start/stop FEP | [producer.rs](../src/kafka/producer.rs), [health_check.rs](../src/connection/health_check.rs), [network_ping.rs](../src/connection/network_ping.rs), [reconnection.rs](../src/connection/reconnection.rs) |
| `RTU_LOGGING` | produce | `id_rtu` | Kejadian koneksi/RCB/command, lalu lintas data (`LOG_DATA_TRAFFIC`), CONTROL TX/RX | [producer.rs](../src/kafka/producer.rs), [traffic.rs](../src/iec61850/traffic.rs), [acquisition.rs](../src/iec61850/acquisition.rs) |
| `RTU_COMMAND` | produce | `key` payload (`"unknown"` kalau kosong) | Hasil SELECT/OPERATE/CANCEL | [rtu_event/command.rs](../src/kafka/handlers/rtu_event/command.rs) |
| `MANUAL_SET_EVENT_RESPONSE` | produce | `"IEC61850"` | Jawaban `MANUAL_SET_EVENT` | [manual_set.rs](../src/kafka/handlers/manual_set.rs) |
| `IEC61850_BROWSE_RESPONSE` | produce | `key` payload (`"unknown"` kalau kosong) | Hasil event `BROWSE` (bisa beberapa bagian) | [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs) |

### 1.2 Aturan umum

- **Protocol.** FEP IEC 104 dan FEP IEC 61850 membaca topic yang sama. Pesan dengan `protocol` selain `IEC61850`
  **diabaikan tanpa log** (`is_ours`, perbandingan tidak peka huruf besar/kecil: `iec61850` juga diterima).
  Pengecualian: `CAPTURE_DATA_CHANGE` (tidak punya `protocol`; disaring per tabel, `master_data_objects` /
  `master_general_actions` per `record.protocol`) dan `MASTER_DATA_OBJECT_EVENT` (isi pesan tidak dibaca).
- **Consumer group.** Env `GROUP_ID`, default `IEC61850_FEP_GROUP` ([config/kafka.rs](../src/config/kafka.rs)), sama dengan
  `.env.example` dan `kubernetes/configmap.yaml` (dicek test `group_id_seragam`). Harus berbeda dengan group FEP IEC 104.
  **Hanya satu instance FEP IEC 61850 aktif per lingkungan.**
- **Topic yang di-subscribe.** Env `SUBSCRIBE_TOPICS` (dipisah koma), default
  `RTU_EVENTS,RTU_COMMAND_CONFIG_REQUEST,MASTER_DATAPOINT_EVENT,MASTER_DATA_OBJECT_EVENT,CAPTURE_DATA_CHANGE,MASTER_RTU_EVENTS,MANUAL_SET_EVENT`.
  Nama topic dibaca dari bagian setelah titik terakhir, jadi topic berawalan (`dev.RTU_EVENTS`) tetap dikenali.
  Topic produce tidak bisa diatur (konstanta di [kafka/mod.rs](../src/kafka/mod.rs)).
- **Offset.** Commit manual (async) paling cepat tiap 2 detik, pola sama dengan FEP IEC 104.
- **Waktu.** Field `time` / `timestamp` = **milidetik epoch UTC** (angka). Waktu di teks `logging` = ISO-8601 UTC
  mikrodetik (`2026-11-15T03:04:05.123456Z`).
- **JSON tidak valid / field wajib tidak ada / tipe salah:** hanya log `ERROR <TOPIC> parse error: ...` di FEP.
  Pesan dibuang, **tidak ada balasan** ke Kafka. Pengecualian: `RTU_EVENTS` COMMAND dengan `protocol` IEC61850 dan
  `key` yang masih terbaca → `RTU_COMMAND` `inactive` (`payload tidak valid: ...`). Field yang tidak dikenal diabaikan.
- **Payload bukan UTF-8:** dibuang tanpa log.
- **Topic tidak dikenal** (di-subscribe lewat `SUBSCRIBE_TOPICS` tapi tidak ditangani): log `WARN topik tidak dikenal`.

### 1.3 Pencarian `id_rtu` (RTU_EVENTS)

| Event | Urutan |
|---|---|
| CREATE/ADD/UPDATE, DELETE/REMOVE, STATE, RESET, GI, CHANNEL | `id_rtu` tingkat atas → `data.id_rtu` → dicari dari `ip_address` di antara IED yang **sedang berjalan** |
| COMMAND | `data.id_rtu` → `id_rtu` tingkat atas → dicari dari `data.ip_address` / `ip_address` di antara IED yang berjalan |
| BROWSE | `id_rtu` tingkat atas → `data.id_rtu` → dicari dari `data.ip_address` / `ip_address` di antara IED yang berjalan |

Tidak ketemu → event tidak dijalankan (log `WARN`), kecuali COMMAND/BROWSE yang tetap dibalas `FAILED`/`inactive`.

---

## 2. Topic yang dikonsumsi

### 2.1 `RTU_EVENTS`

Struct: `RtuEventMessage` ([models.rs](../src/kafka/models.rs)). Field yang dibaca semua event:

| Field | Tipe JSON | Wajib | Default | Posisi | Arti |
|---|---|---|---|---|---|
| `event` | string | ya | - | atas | Nama event (huruf besar, persis) |
| `protocol` | string | ya | - | atas | Harus `IEC61850` |
| `ip_address` | string | tidak | `""` | atas | IP IED, dipakai mencari `id_rtu` kalau tidak diisi |
| `id_rtu` | integer | tidak | - | atas | `iec61850s.id_rtu` |
| `data` | object | tidak | - | atas | Isi tambahan; `data.id_rtu` dipakai kalau `id_rtu` atas kosong |
| `port` | integer | tidak | - | atas | Hanya dipakai COMMAND format datar |
| `state_rtu` | boolean | STATE: ya | - | atas | Hanya event STATE |

Event yang tidak dikenal: log `WARN [RTU_EVENTS] <event> RTU <id>: event tidak dikenal: <event>`.

#### CREATE / ADD / UPDATE

Efek: baca ulang RTU dari DB (`iec61850s`, RCB, mapping), hentikan thread lama (tunggu maks 15 detik), jalankan thread baru.
`state_rtu = false` di DB → tidak dikoneksikan (+ `RTU_STATUS` 0 kalau sebelumnya berjalan). Tidak ada balasan khusus;
hasilnya terlihat di `RTU_STATUS` / `RTU_LOGGING` saat konek.

Gagal: `id_rtu` tidak ada di `iec61850s` → thread dihentikan, log `WARN [RTU_EVENTS] UPDATE RTU 9001: RTU 9001 tidak ada di iec61850s`.
DB tidak tersedia → log `WARN`. RTU **baru** harus dikirim dengan `id_rtu` (pencarian dari IP hanya untuk IED yang sudah berjalan).

Minimal:

`docs/examples/consume/RTU_EVENTS__CREATE.json`

```json
{
  "event": "CREATE",
  "protocol": "IEC61850",
  "id_rtu": 9001
}
```

Lengkap:

`docs/examples/consume/RTU_EVENTS__UPDATE.json`

```json
{
  "event": "UPDATE",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "port": 102,
  "id_rtu": 9001,
  "data": {
    "id_rtu": 9001,
    "name": "GI BAY 1"
  }
}
```

Hanya IP (RTU yang sudah berjalan):

`docs/examples/consume/RTU_EVENTS__ADD.by_ip.json`

```json
{
  "event": "ADD",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21"
}
```

Log FEP: `INFO [RTU_EVENTS] event=UPDATE ip=10.10.1.21 id_rtu=Some(9001)`.

#### DELETE / REMOVE

Efek: hentikan thread IED (koneksi MMS ditutup). Tidak ada balasan; `RTU_STATUS` 0 dikirim thread saat sesi berakhir
(kalau sedang terkoneksi).

`docs/examples/consume/RTU_EVENTS__DELETE.json`

```json
{
  "event": "DELETE",
  "protocol": "IEC61850",
  "id_rtu": 9001
}
```

`docs/examples/consume/RTU_EVENTS__REMOVE.data.json`

```json
{
  "event": "REMOVE",
  "protocol": "IEC61850",
  "data": {
    "id_rtu": 9001
  }
}
```

#### STATE

`state_rtu` wajib. `false`: hentikan thread + `RTU_STATUS` 0 (kalau sedang berjalan), tidak reconnect.
`true`: baca ulang dari DB lalu konek. Nilai event **menimpa** `state_rtu` di DB (berjaga-jaga DB belum ter-update).
Tanpa `state_rtu`: log `WARN [RTU_EVENTS] STATE RTU 9001: STATE tanpa state_rtu`.

`docs/examples/consume/RTU_EVENTS__STATE.off.json`

```json
{
  "event": "STATE",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "id_rtu": 9001,
  "state_rtu": false
}
```

`docs/examples/consume/RTU_EVENTS__STATE.on.json`

```json
{
  "event": "STATE",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "state_rtu": true
}
```

#### RESET

`RTU_STATUS` 0 (kalau IED berjalan), lalu sama dengan UPDATE (reload + konek ulang).

`docs/examples/consume/RTU_EVENTS__RESET.json`

```json
{
  "event": "RESET",
  "protocol": "IEC61850",
  "id_rtu": 9001
}
```

#### GI

Mode `REPORT`/`HYBRID`: GI ke semua RCB yang aktif → data `GENERAL_INTERROGATION` di `RTU_DATAS`.
Mode `POLLING`: siklus polling penuh sekarang juga → semua titik dengan action `GENERAL_INTERROGATION` (sama dengan mode REPORT).
Mode `HYBRID`: GI ke RCB + polling penuh titik `MX`, keduanya `GENERAL_INTERROGATION`.
IED tidak berjalan: log `WARN [RTU_EVENTS] GI RTU 9001: IED tidak berjalan`. Kalau `LOG_DATA_TRAFFIC=true`, baris
`GI TX` / `GI RX` muncul di `RTU_LOGGING`.

`docs/examples/consume/RTU_EVENTS__GI.json`

```json
{
  "event": "GI",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "id_rtu": 9001
}
```

Diabaikan (protocol lain):

`docs/examples/consume/RTU_EVENTS__GI.ignored_iec104.json`

```json
{
  "event": "GI",
  "protocol": "IEC104",
  "ip_address": "10.10.2.5",
  "port": 2404
}
```

#### CHANNEL

Hanya dicatat di log (sama dengan FEP IEC 104). Isi `data` tidak dibaca.

`docs/examples/consume/RTU_EVENTS__CHANNEL.json`

```json
{
  "event": "CHANNEL",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "id_rtu": 9001,
  "data": {
    "channel": 1
  }
}
```

#### COMMAND

Handler: [rtu_event/command.rs](../src/kafka/handlers/rtu_event/command.rs). Command langsung dimasukkan ke antrean thread IED
(urutan kedatangan terjaga: SELECT sebelum OPERATE), hasil ditunggu maks **30 detik** di task terpisah, lalu balasan ke
`RTU_COMMAND` (bagian [3.5](#35-rtu_command)).

Bentuk: objek `data` (diutamakan) **atau** field datar di tingkat atas. Kalau `data` ada tapi salah tipe (mis. `port`
berupa string), command **ditolak** dengan `inactive` `field data tidak valid: ...` (tidak memakai field datar).

| Field | Tipe JSON | Wajib | Default | Posisi | Arti |
|---|---|---|---|---|---|
| `action` | string | ya | - | `data` / atas | `SELECT_RTU`, `OPERATE_RTU`, `CANCEL_RTU` (tidak peka huruf besar/kecil) |
| `ioa` | integer / string angka | ya* | - | `data` / atas | IOA virtual titik kontrol (FC `CO`) |
| `tag` | string | ya* | - | `data` / atas | *Dipakai kalau `ioa` kosong: IOA dari akhiran `_<angka>` (`BAY1_CB_3005` → 3005). Juga disalin ke respons |
| `value` | integer / float / string angka / boolean | SELECT, OPERATE: ya | - | `data` / atas | Nilai perintah, diartikan per CDC (tabel di bawah) |
| `type` | string | tidak | `""` | `data` / atas | Hanya disalin ke `command_type` respons. CDC diambil dari mapping DB, bukan dari sini |
| `id_rtu` | integer | tidak** | - | `data` → atas | **Kalau kosong dicari dari `ip_address` |
| `ip_address` | string | tidak** | `ip_address` atas | `data` / atas | Dipakai mencari `id_rtu` dan sebagai `hostname` respons |
| `port` | integer 0..65535 | tidak | port konfigurasi IED | `data` → atas | Hanya untuk respons kalau IED tidak dikenal (tanpa `port`: 102). IED dikenal → selalu port konfigurasi |
| `key` | string | tidak | `""` | `data` / atas | Disalin ke respons, juga jadi key Kafka respons |
| `user` | string | tidak | `FEP-IEC61850` | `data` → atas | `orIdent` (pemberi perintah) di IEC 61850 |
| `qualifier` | integer 0..255 | tidak | `0` | `data` → atas | Dikembalikan apa adanya di respons. Tidak dikirim ke IED (IEC 61850 tidak punya qualifier) |

Arti `value` per CDC titik kontrol (`ctl_value`, [domain/command.rs](../src/domain/command.rs)):

| CDC | `value` | ctlVal ke IED |
|---|---|---|
| `SPC` | `0` → false, selain 0 → true | BOOLEAN |
| `DPC` | `DPC_CLOSE_VALUE=2` (default): `2` = CLOSE (true), `1` = OPEN (false). `DPC_CLOSE_VALUE=1`: dibalik. Lainnya ditolak | BOOLEAN |
| `INC`, `ENC` | bilangan bulat (int32) | INT32 |
| `BSC` | `0` stop, `1` lower, `2` higher | Tcmd (BIT STRING 2 bit) |
| `APC` | angka (float) | `ctlVal.f` FLOAT32 |
| lainnya | - | ditolak: `CDC <x> belum didukung untuk command` |

Aksi IEC 61850 per `ctlModel` (dibaca dari IED, ditimpa `iec61850_command_formulas.ctl_model` kalau diisi):

| `action` | direct-normal / direct-enhanced | sbo-normal | sbo-enhanced |
|---|---|---|---|
| `SELECT_RTU` | tidak ada request MMS, langsung sukses (`CONTROL SKIP` di RTU_LOGGING) | `select` | `selectWithValue` |
| `OPERATE_RTU` | `operate` (enhanced: tunggu CommandTermination maks `OPER_TIMEOUT_MS`) | `operate` | `operate` + tunggu CommandTermination |
| `CANCEL_RTU` | `cancel` | `cancel` | `cancel` |

`interlock_check` / `synchro_check` dari `iec61850_command_formulas` (tanpa baris = false). `status-only` → ditolak.

Minimal:

`docs/examples/consume/RTU_EVENTS__COMMAND.min.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "data": {
    "action": "OPERATE_RTU",
    "id_rtu": 9001,
    "ioa": 3001,
    "value": 1
  }
}
```

Lengkap (DPC, SBO: kirim SELECT lalu OPERATE dengan `key` sendiri-sendiri):

`docs/examples/consume/RTU_EVENTS__COMMAND.full.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "data": {
    "action": "SELECT_RTU",
    "tag": "BAY1_CB_3005",
    "type": "DPC",
    "value": 2,
    "ioa": 3005,
    "id_rtu": 9001,
    "key": "cmd-20261115-0001",
    "user": "operator1",
    "ip_address": "10.10.1.21",
    "port": 102
  }
}
```

Format datar (tanpa `data`, IOA dari tag, value string):

`docs/examples/consume/RTU_EVENTS__COMMAND.flat.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "port": 102,
  "id_rtu": 9001,
  "action": "OPERATE_RTU",
  "tag": "BAY1_CB_3005",
  "type": "DPC",
  "value": "2",
  "key": "cmd-20261115-0002",
  "user": "operator1"
}
```

CANCEL (tanpa `value`):

`docs/examples/consume/RTU_EVENTS__COMMAND.cancel.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "data": {
    "action": "CANCEL_RTU",
    "ioa": 3005,
    "id_rtu": 9001,
    "key": "cmd-20261115-0003",
    "user": "operator1"
  }
}
```

APC (float) dan BSC (Tcmd higher):

`docs/examples/consume/RTU_EVENTS__COMMAND.apc.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "data": {
    "action": "OPERATE_RTU",
    "ioa": 3010,
    "type": "APC",
    "value": 12.5,
    "id_rtu": 9001,
    "key": "cmd-20261115-0004"
  }
}
```

`docs/examples/consume/RTU_EVENTS__COMMAND.bsc.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "data": {
    "action": "OPERATE_RTU",
    "ioa": 3011,
    "type": "BSC",
    "value": 2,
    "id_rtu": 9001,
    "key": "cmd-20261115-0006"
  }
}
```

Aman untuk FEP IEC 104 dan 61850 (lihat [bagian 4](#4-perbandingan-dengan-fep-iec-104)):

`docs/examples/consume/RTU_EVENTS__COMMAND.compat104.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "port": 102,
  "data": {
    "action": "OPERATE_RTU",
    "ip_address": "10.10.1.21",
    "port": 102,
    "id_rtu": 9001,
    "ioa": 3005,
    "tag": "BAY1_CB_3005",
    "type": "DPC",
    "value": 2,
    "key": "cmd-20261115-0005",
    "user": "operator1"
  }
}
```

Log FEP: `INFO [COMMAND] OPERATE ip=10.10.1.21 id_rtu=Some(9001) ioa=Some(3005) value=Some(2.0) key=Some("cmd-20261115-0005")`,
lalu `INFO [COMMAND] OPERATE sukses ioa=Some(3005)` atau `ERROR [COMMAND] OPERATE gagal ioa=Some(3005): <alasan>`.
Di `RTU_LOGGING`: baris `CONTROL TX/RX` + `[INFO] Operate ioa 3005 sukses` / `[WARN] Operate ioa 3005 gagal: <alasan>`.

Gagal / ditolak (semua dibalas `RTU_COMMAND` `status: "inactive"`, alasan di `msg`):

| Kondisi | Alasan di `msg` |
|---|---|
| JSON tidak terbaca struct (salah tipe di tingkat atas / JSON rusak) tapi `event` = COMMAND, `protocol` = IEC61850, dan `key` terbaca | `payload tidak valid: <error serde>`. `event` respons dari `action` kalau terbaca, selain itu `UNKNOWN`. Tanpa `key` → tidak ada balasan (log ERROR) |
| `data` ada tapi salah tipe | `field data tidak valid: <error serde>` |
| `action` tidak dikenal / kosong | `action tidak dikenal: "<action>" (SELECT_RTU, OPERATE_RTU, CANCEL_RTU)`; `event` = action tanpa `_RTU` (kosong: `UNKNOWN`) |
| `id_rtu` tidak ada dan IP tidak cocok IED yang berjalan | `RTU tidak ditemukan untuk IP ini` |
| `ioa` kosong dan tag tanpa akhiran angka | `ioa atau tag dengan IOA wajib diisi` |
| `value` kosong (SELECT/OPERATE) | `value wajib diisi` |
| Thread IED tidak ada | `RTU 9001 tidak berjalan` |
| IED sedang tidak terkoneksi (menunggu reconnect) | `IED tidak terkoneksi` |
| IOA bukan titik FC `CO` | `IOA 3001 bukan titik kontrol (FC CO) di RTU ini` |
| `value` tidak cocok CDC | mis. `value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan 0`, `value BSC harus 0 (stop), 1 (lower), atau 2 (higher), bukan 5`, `value INC harus bilangan bulat, bukan 1.5` |
| Objek kontrol tidak ada di IED | `objek kontrol <ref> tidak ditemukan / tidak bisa dibaca` |
| `ctlModel` status-only | `ctlModel status-only, tidak bisa dikontrol` |
| IED menolak (negative response) | `<select\|operate\|cancel> gagal: <teks IedClientError atau "ditolak IED"> (LastApplError error=<0..3>, addCause=<kode> (<nama>))` |
| CommandTermination negatif | `CommandTermination- gagal: ... (LastApplError error=..., addCause=<kode> (<nama>))` |
| CommandTermination tidak datang | `CommandTermination tidak diterima (timeout)` |
| Tidak ada hasil dalam 30 detik | `timeout menunggu hasil command` |
| Thread IED berhenti di tengah | `thread IED berhenti sebelum command selesai` |

#### BROWSE

Handler: [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs). Dijalankan di thread IED (antrean yang sama dengan
COMMAND): LD → LN → DO → FC (`ST`, `MX`, `CO`, `SP`), satu `getVariableSpecification` per LN + FC. Hasil ditunggu maks
**300 detik**. Selama browse, polling IED itu tertahan (report tetap diterima). Balasan ke `IEC61850_BROWSE_RESPONSE`
(bagian [3.7](#37-iec61850_browse_response)).

| Field | Tipe JSON | Wajib | Default | Posisi | Arti |
|---|---|---|---|---|---|
| `id_rtu` | integer | ya* | - | atas → `data` | IED yang di-browse |
| `ip_address` | string | ya* | - | `data` → atas | *Dipakai mencari `id_rtu` kalau `id_rtu` kosong (IED yang sedang berjalan) |
| `key` | string | tidak | `""` | atas → `data` | Disalin ke respons + key Kafka respons |

`docs/examples/consume/RTU_EVENTS__BROWSE.json`

```json
{
  "event": "BROWSE",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "key": "browse-9001-01"
}
```

`docs/examples/consume/RTU_EVENTS__BROWSE.data.json`

```json
{
  "event": "BROWSE",
  "protocol": "IEC61850",
  "data": {
    "id_rtu": 9001,
    "key": "browse-9001-02"
  }
}
```

Gagal: tanpa `id_rtu` dan `ip_address` → `FAILED` `id_rtu atau ip_address wajib diisi`; IP tidak cocok IED yang berjalan →
`FAILED` `RTU tidak ditemukan untuk IP <ip>`; IED tidak berjalan / belum terkoneksi → `FAILED` `IED tidak terkoneksi`;
lebih dari 300 detik → `FAILED` `timeout menunggu hasil browse`; error MMS → `FAILED` dengan teks error.

### 2.2 `MASTER_DATAPOINT_EVENT`

Struct: `MasterDatapointEventMessage`. Handler: [master_datapoint.rs](../src/kafka/handlers/master_datapoint.rs).

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `id_rtu` | integer | ya | RTU pemilik datapoint |
| `protocol` | string | ya | `IEC61850` |
| `ioa` | **integer** (string ditolak) | ya | IOA virtual |
| `event` | string | ya | `ADD`, `CREATE`, `UPDATE`, `DELETE` |
| `data.tag` | string | ADD/CREATE/UPDATE: ya | Tag baru |

Efek: ADD/CREATE/UPDATE → tag `(id_rtu, ioa)` diganti di TagResolver; DELETE → tag dihapus. Lalu, kalau IED berjalan,
reload IED digabung selama `RELOAD_DEBOUNCE_MS` (mapping bisa ikut berubah). Tidak ada balasan.
Log: `INFO [MASTER_DATAPOINT] UPDATE id_rtu=9001 ioa=2001`. Event lain / tanpa `data.tag`:
`WARN [MASTER_DATAPOINT] event <x> tanpa tag / tidak dikenal` (tag tidak diubah, tidak reload).

`docs/examples/consume/MASTER_DATAPOINT_EVENT__UPDATE.json`

```json
{
  "id_rtu": 9001,
  "protocol": "IEC61850",
  "ioa": 2001,
  "event": "UPDATE",
  "data": {
    "tag": "BAY1_ANIN1_2001",
    "description": "Arus fasa R"
  }
}
```

`docs/examples/consume/MASTER_DATAPOINT_EVENT__DELETE.json`

```json
{
  "id_rtu": 9001,
  "protocol": "IEC61850",
  "ioa": 2001,
  "event": "DELETE"
}
```

### 2.3 `MASTER_DATA_OBJECT_EVENT`

Isi pesan **tidak dibaca sama sekali** (boleh `{}`; protocol tidak dicek). Efek: muat ulang group
(`master_data_objects` dengan `protocol = 'IEC61850'`). Tidak ada balasan. DB tidak tersedia → `WARN [CDC] DB tidak tersedia`.

`docs/examples/consume/MASTER_DATA_OBJECT_EVENT__UPDATE.json`

```json
{
  "event": "UPDATE",
  "protocol": "IEC61850",
  "data": {
    "initial": "MV",
    "id_group": "a2b4c6d8-1e3f-4a5b-8c7d-9e0f1a2b3c4d"
  }
}
```

### 2.4 `CAPTURE_DATA_CHANGE`

Struct: `CaptureDataChangeMessage`. Handler: [capture_data_change.rs](../src/kafka/handlers/capture_data_change.rs)
(keputusan di fungsi `plan`, diuji tanpa DB). Dikirim `iconics_configuration_cdc_listener` dari trigger `notify_table_change()`.

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `table` | string | ya | Nama tabel |
| `schema` | string | ya | Selain `public` diabaikan |
| `operation` | string | ya | `INSERT`, `UPDATE`, `DELETE` |
| `record` | object | tidak | Baris **lengkap** setelah perubahan (DELETE: baris yang dihapus). **Tidak ada nilai lama** |
| `data` | object | tidak | Bentuk lama, dipakai kalau `record` tidak ada |
| `timestamp`, `source` | string | tidak | Tidak dibaca |

| `table` | Efek |
|---|---|
| `iec61850s` | `record.id_rtu`. `DELETE` atau `is_deleted = true` → hentikan IED. `INSERT` → jalankan IED (reload). `UPDATE` → dibandingkan dengan konfigurasi IED yang **sedang berjalan**; reload IED itu hanya kalau kolom koneksi/akuisisi berubah (tabel di bawah). IED belum berjalan → dijalankan kalau `state_rtu` tidak `false` |
| `iec61850_report_controls` | reload IED `record.id_rtu` |
| `master_datapoint_iec61850s`, `iec61850_command_formulas` | `id_rtu` dicari dari `record.id_master_datapoint` lewat `master_datapoints` → reload IED itu. Tidak ketemu (mis. DELETE dan datapoint sudah hilang) → reload **semua** IED + WARN |
| `master_data_objects` | muat ulang group (tanpa reconnect). `record.protocol` ada dan bukan `IEC61850` → diabaikan |
| `master_general_actions` | muat ulang general action (tanpa reconnect). `record.protocol` ada dan bukan `IEC61850` → diabaikan |
| `formulas` | muat ulang formula (tanpa reconnect) |
| `manual_sets` | muat ulang daftar tag manual set (tanpa reconnect) |
| lainnya (`dnp3s`, `iec104s`, ...) | diabaikan, hanya log DEBUG |

Kolom `iec61850s` yang memicu reload (dipakai `RtuConfig`): `ip_address`, `port`, `local_ip_address`, `acquisition_mode`,
`poll_interval_ms`, `general_interrogation_interval`, `connect_timeout_ms`, `request_timeout_ms`, `reconnect_time_ms`,
`reconnect_retry_count`, `state_rtu`, `ap_title`, `ae_qualifier`, `p_selector`, `s_selector`, `t_selector` (+ `is_deleted`).
Kolom lain **diabaikan**, termasuk kolom status yang sering di-UPDATE backend saat RTU online/offline (`status_rtu`,
`rtu_online_at`, `rtu_offline_at`, `status_channel`, `channel_*`, `created_at`, `updated_at`, `name`, ...). Kolom yang tidak ada
di `record` dianggap tidak berubah. Angka negatif dianggap 0 (sama dengan saat dimuat).

Log: `INFO [CDC] UPDATE iec61850s: reload IED`, `DEBUG [CDC] UPDATE iec61850s diabaikan: hanya kolom status / non-koneksi yang berubah`,
`WARN [CDC] DELETE master_datapoint_iec61850s: id_rtu datapoint <uuid> tidak ditemukan, reload semua IED`. Tidak ada balasan Kafka.

Sampel asli dari cdc_listener (tabel lain, diabaikan FEP; `...` apa adanya dari sampel):

`docs/examples/consume/CAPTURE_DATA_CHANGE__dnp3s.real.json`

```json
{
  "table": "dnp3s",
  "schema": "public",
  "operation": "UPDATE",
  "record": {
    "id": "30a0f2b7-...",
    "id_rtu": 83020012,
    "protocol": "DNP3",
    "tcp_server_ip": "192.168.212.12",
    "tcp_port": 20000,
    "state_rtu": true,
    "rtu_online_at": "1794785701262",
    "rtu_offline_at": "1794791384243",
    "status_rtu": "red",
    "status_channel": false,
    "is_deleted": false,
    "updated_at": "2026-11-16T01:09:44.737+00:00",
    "...": "..."
  },
  "timestamp": "2026-11-16T01:09:44.760Z",
  "source": "cdc_listener"
}
```

`iec61850s`, hanya kolom status berubah → diabaikan (konfigurasi sama dengan yang berjalan):

`docs/examples/consume/CAPTURE_DATA_CHANGE__iec61850s.status_only.json`

```json
{
  "table": "iec61850s",
  "schema": "public",
  "operation": "UPDATE",
  "record": {
    "id": "5d0f3c1a-7b2e-4f60-9a11-2c3d4e5f6a7b",
    "id_rtu": 9001,
    "name": "GI BAY 1",
    "ip_address": "10.10.1.21",
    "port": 102,
    "local_ip_address": "0.0.0.0",
    "acquisition_mode": "REPORT",
    "poll_interval_ms": 1000,
    "general_interrogation_interval": 0,
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 1000,
    "reconnect_time_ms": 1000,
    "reconnect_retry_count": 0,
    "state_rtu": true,
    "channel_number": 1,
    "ap_title": "1.1.999.1",
    "ae_qualifier": 12,
    "p_selector": "00 00 00 01",
    "s_selector": "00 01",
    "t_selector": "00 01",
    "status_rtu": "red",
    "rtu_online_at": "1794785701262",
    "rtu_offline_at": "1794791384243",
    "status_channel": false,
    "is_deleted": false,
    "created_at": "2026-11-01T02:00:00.000+00:00",
    "updated_at": "2026-11-16T01:09:44.737+00:00"
  },
  "timestamp": "2026-11-16T01:09:44.760Z",
  "source": "cdc_listener"
}
```

`iec61850_command_formulas` → reload IED pemilik datapoint:

`docs/examples/consume/CAPTURE_DATA_CHANGE__iec61850_command_formulas.json`

```json
{
  "table": "iec61850_command_formulas",
  "schema": "public",
  "operation": "UPDATE",
  "record": {
    "id": "2b3c4d5e-6f70-4a81-9b2c-3d4e5f6a7b8c",
    "id_master_datapoint": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    "type": "DPC",
    "ctl_model": "sbo-with-enhanced-security",
    "value": null,
    "interlock_check": true,
    "synchro_check": false,
    "is_deleted": false,
    "created_at": "2026-11-01T02:00:00.000+00:00",
    "updated_at": "2026-11-16T01:10:00.000+00:00"
  },
  "timestamp": "2026-11-16T01:10:00.020Z",
  "source": "cdc_listener"
}
```

Bentuk lama (`data`), masih diterima:

`docs/examples/consume/CAPTURE_DATA_CHANGE__master_datapoint_iec61850s.data.json`

```json
{
  "table": "master_datapoint_iec61850s",
  "schema": "public",
  "operation": "UPDATE",
  "data": {
    "id": "9e8d7c6b-5a49-4382-a1b0-c9d8e7f6a5b4",
    "id_master_datapoint": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    "object_reference": "simpleIOGenericIO/GGIO1.AnIn1",
    "functional_constraint": "MX"
  }
}
```

### 2.5 `MASTER_RTU_EVENTS`

Struct: `MasterRtuEventMessage`. Handler: [master_rtu_event.rs](../src/kafka/handlers/master_rtu_event.rs).

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `id_rtu` | integer | ya | `id_rtu` lama |
| `protocol` | string | ya | `IEC61850` |
| `event` | string | ya | Hanya `UPDATE_ID_RTU`; lainnya: `INFO [MASTER_RTU_EVENTS] event tidak dikenal` |
| `new_data.id_rtu` | integer | ya | `id_rtu` baru |
| `id_station` | string | tidak (dibaca FEP IEC 104) | Diabaikan FEP IEC 61850 |

Efek: thread `id_rtu` lama dihentikan, RTU baru dibaca dari DB lalu dikoneksikan. Sama dengan id lama → tidak ada aksi.
Tanpa `new_data.id_rtu` → `WARN [UPDATE_ID_RTU] new_data.id_rtu tidak ada`. Tidak ada balasan.

`docs/examples/consume/MASTER_RTU_EVENTS__UPDATE_ID_RTU.json`

```json
{
  "id_rtu": 9001,
  "id_station": "7c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f",
  "protocol": "IEC61850",
  "event": "UPDATE_ID_RTU",
  "new_data": {
    "id_rtu": 9011
  }
}
```

### 2.6 `MANUAL_SET_EVENT`

Struct: `ManualSetEventMessage`. Handler: [manual_set.rs](../src/kafka/handlers/manual_set.rs).

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `event` | string | ya | `TOGGLE_MANUAL_SET` (tidak peka huruf besar/kecil) |
| `protocol` | string | ya | `IEC61850` |
| `tag` | string | ya | Tag datapoint |
| `manual_set` | boolean | ya | `true` = berhenti kirim ke `DATAPOINTS`, `false` = kirim lagi |
| `id_rtu` | integer | ya | Harus IED yang sedang berjalan di FEP ini |
| `user` | string | tidak | Disalin ke respons |

Efek + balasan `MANUAL_SET_EVENT_RESPONSE` (bagian [3.6](#36-manual_set_event_response)). `id_rtu` bukan IED FEP ini
→ **diam** (FEP lain yang menjawab). Event lain → `WARN [MANUAL_SET] event tidak dikenal`, tanpa balasan.
Log: `INFO [MANUAL_SET] STOPPED tag=BAY1_ANIN1_2001 user=operator1`.

`docs/examples/consume/MANUAL_SET_EVENT__TOGGLE_MANUAL_SET.json`

```json
{
  "event": "TOGGLE_MANUAL_SET",
  "protocol": "IEC61850",
  "tag": "BAY1_ANIN1_2001",
  "manual_set": true,
  "id_rtu": 9001,
  "user": "operator1"
}
```

### 2.7 `RTU_COMMAND_CONFIG_REQUEST`

Struct: `RtuCommandConfigMessage`. Hanya dicatat: `INFO [RTU_COMMAND_CONFIG] action=GET ip=10.10.1.21 ioa=Some(3005) value=Some(1)`.
Tidak ada balasan (sama dengan FEP IEC 104).

| Field | Tipe JSON | Wajib |
|---|---|---|
| `action` | string | ya |
| `protocol` | string | ya |
| `ip_address` | string | ya |
| `ioa` | integer | tidak |
| `value` | integer | tidak |

`docs/examples/consume/RTU_COMMAND_CONFIG_REQUEST__GET.json`

```json
{
  "action": "GET",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "ioa": 3005,
  "value": 1
}
```

---

## 3. Topic yang diproduksi

### 3.1 `RTU_DATAS` dan `DATAPOINTS`

Struct: `DataMessage` ([models.rs](../src/kafka/models.rs)), dibuat di
[message_builder.rs](../src/kafka/publisher/message_builder.rs). Key = `id_rtu`. Urutan field sama dengan FEP IEC 104.

- `RTU_DATAS`: **setiap** nilai titik yang dikirim. `tag` selalu `""`, `value` mentah.
- `DATAPOINTS`: pesan yang sama, hanya kalau titik punya tag (TagResolver), tag tidak manual set, dan tag tidak kosong
  setelah disaring (hanya `A-Z a-z 0-9 _ -`). `tag` terisi, `value` sudah melalui formula (`formulas`).

| Field | Tipe | Arti / nilai |
|---|---|---|
| `ip_address` | string | IP IED (`iec61850s.ip_address`) |
| `protocol` | string | selalu `IEC61850` |
| `id_rtu` | integer | `iec61850s.id_rtu` |
| `action` | string | Penyebab pengiriman (tabel di bawah) |
| `tag` | string | `""` di `RTU_DATAS`; tag di `DATAPOINTS` |
| `value` | number / null | Nilai (tabel per CDC). `null` kalau tipe MMS tidak didukung / float NaN |
| `address` | integer | IOA virtual (`master_datapoints.ioa`) |
| `ca` | integer | selalu `0` |
| `asdu` | integer | selalu `0` |
| `type` | string | CDC titik (`SPS`, `DPS`, `MV`, ...) dari mapping |
| `quality` | string | `GOOD`, `INVALID`, `SUBSTITUTED`, `QUESTIONABLE` |
| `flag` | string | Detail quality dipisah `\|`, `GOOD` kalau kosong |
| `group` | string | `master_data_objects.id_group` (UUID) untuk `initial` = CDC dan `protocol = 'IEC61850'`; `""` kalau tidak ada |
| `general_action` | integer / null | `master_general_actions.code` untuk `name_action` = `action` (`protocol = 'IEC61850'`); `null` kalau tidak ada |
| `time` | integer | ms epoch UTC |
| `time_desc` | string | `RTU` atau `SERVER` |

`action` ([mapper.rs](../src/iec61850/mapper.rs)):

| `action` | Kapan |
|---|---|
| `GENERAL_INTERROGATION` | Report dengan reason `gi` (GI awal, GI berkala `general_interrogation_interval`, event GI, RCB pulih); polling penuh karena event GI |
| `INTEGRITY` | Report reason `integrity`; polling siklus integrity (`INTEGRITY_PERIOD_MS`, siklus pertama setelah konek) |
| `DATA_CHANGE` | Report reason `dchg` |
| `QUALITY_CHANGE` | Report reason `qchg` |
| `DATA_UPDATE` | Report reason `dupd`, atau report tanpa reason |
| `POLLING` | Polling, nilai/quality berubah melewati deadband |

Beberapa reason sekaligus: prioritas `GENERAL_INTERROGATION` > `INTEGRITY` > `DATA_CHANGE` > `QUALITY_CHANGE` > `DATA_UPDATE`.
Beberapa anggota dataset untuk satu titik (mis. `stVal` dan `q` terpisah) digabung, action tertinggi yang dipakai.

`quality` (urutan sama dengan IEC 104):

| `quality` | Syarat (`q` IEC 61850-7-3) |
|---|---|
| `INVALID` | validity = invalid |
| `SUBSTITUTED` | source = substituted |
| `QUESTIONABLE` | validity = questionable/reserved, atau detail `oldData` / `operatorBlocked` / `overflow` |
| `GOOD` | lainnya, atau titik tanpa `q` |

`flag`: gabungan `INVALID`, `QUESTIONABLE`, `OVERFLOW`, `OUT_OF_RANGE`, `BAD_REFERENCE`, `OSCILLATORY`, `FAILURE`, `OLD_DATA`,
`INCONSISTENT`, `INACCURATE`, `SUBSTITUTED`, `TEST`, `OPERATOR_BLOCKED`, `CLOCK_NOT_SYNC` (time quality `t`: clockFailure atau
clockNotSynchronized), urutan tetap, dipisah `|`. Tidak ada satu pun → `GOOD`.

`time_desc`: `RTU` = atribut `t` titik, atau waktu entri report kalau titik tidak punya `t` (tidak dikoreksi `TIMEZONE`);
`SERVER` = jam server FEP (polling tanpa `t`).

`general_action`: kode dari tabel `master_general_actions` (bukan konstanta FEP). Angka di contoh hanya contoh.

`value` per CDC ([mapper.rs](../src/iec61850/mapper.rs) `mms_to_json`):

| Tipe MMS / CDC | `value` |
|---|---|
| BOOLEAN (`SPS`, `SPC`, `ACT.general`, ...) | `0` / `1` |
| Dbpos (`DPS`, `DPC`) | `0` intermediate, `1` OFF/open, `2` ON/closed, `3` bad (sama dengan DPI IEC 104) |
| INTEGER (`INS`, `INC`, `ENS`, `BCR.actVal`, ...) | integer |
| UNSIGNED | integer |
| FLOAT (`MV.mag.f`, `CMV.cVal.mag.f`, `APC.mxVal.f`) | float |
| BIT STRING lain (mis. `BSC.valWTr` posisi/transient) | integer dari bit string |
| lainnya (string, struktur, ...) | `null` |

Nilai command `DPC` memakai pemetaan lain (`DPC_CLOSE_VALUE`, bagian [2.1 COMMAND](#command)).

`DATA_CHANGE` (DPS dari BRCB, waktu dari `t`):

`docs/examples/produce/RTU_DATAS__DATA_CHANGE.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "DATA_CHANGE",
  "tag": "",
  "value": 2,
  "address": 1001,
  "ca": 0,
  "asdu": 0,
  "type": "DPS",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "6f1c2a9e-3b7d-4c11-9a52-0d8e4b7f2a10",
  "general_action": 3,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

`QUALITY_CHANGE` (MV invalid):

`docs/examples/produce/RTU_DATAS__QUALITY_CHANGE.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "QUALITY_CHANGE",
  "tag": "",
  "value": 0.0,
  "address": 2001,
  "ca": 0,
  "asdu": 0,
  "type": "MV",
  "quality": "INVALID",
  "flag": "INVALID|FAILURE",
  "group": "a2b4c6d8-1e3f-4a5b-8c7d-9e0f1a2b3c4d",
  "general_action": 4,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

`INTEGRITY`:

`docs/examples/produce/RTU_DATAS__INTEGRITY.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "INTEGRITY",
  "tag": "",
  "value": 231.4,
  "address": 2001,
  "ca": 0,
  "asdu": 0,
  "type": "MV",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "a2b4c6d8-1e3f-4a5b-8c7d-9e0f1a2b3c4d",
  "general_action": 2,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

`GENERAL_INTERROGATION` (jam IED tidak sinkron):

`docs/examples/produce/RTU_DATAS__GENERAL_INTERROGATION.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "GENERAL_INTERROGATION",
  "tag": "",
  "value": 1,
  "address": 1002,
  "ca": 0,
  "asdu": 0,
  "type": "SPS",
  "quality": "GOOD",
  "flag": "CLOCK_NOT_SYNC",
  "group": "0c9b8a7f-6e5d-4c3b-2a19-08f7e6d5c4b3",
  "general_action": 1,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

`POLLING` (tanpa `t`, group / general action belum dikonfigurasi):

`docs/examples/produce/RTU_DATAS__POLLING.json`

```json
{
  "ip_address": "10.10.1.22",
  "protocol": "IEC61850",
  "id_rtu": 9002,
  "action": "POLLING",
  "tag": "",
  "value": 49.98,
  "address": 2003,
  "ca": 0,
  "asdu": 0,
  "type": "MV",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "",
  "general_action": null,
  "time": 1794711846000,
  "time_desc": "SERVER"
}
```

`DATAPOINTS` (tag terisi; nilai MV setelah formula):

`docs/examples/produce/DATAPOINTS__DATA_CHANGE.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "DATA_CHANGE",
  "tag": "BAY1_CB_1001",
  "value": 2,
  "address": 1001,
  "ca": 0,
  "asdu": 0,
  "type": "DPS",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "6f1c2a9e-3b7d-4c11-9a52-0d8e4b7f2a10",
  "general_action": 3,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

`docs/examples/produce/DATAPOINTS__INTEGRITY.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "action": "INTEGRITY",
  "tag": "BAY1_ANIN1_2001",
  "value": 2.314,
  "address": 2001,
  "ca": 0,
  "asdu": 0,
  "type": "MV",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "a2b4c6d8-1e3f-4a5b-8c7d-9e0f1a2b3c4d",
  "general_action": 2,
  "time": 1794711845123,
  "time_desc": "RTU"
}
```

### 3.2 `RTU_STATUS`

Struct: `RtuStatusMessage`. Key = `id_rtu`.

| Field | Tipe | Arti |
|---|---|---|
| `ip_address` | string | IP IED (disaring: hanya huruf/angka ASCII, `.`, `:`), `system` untuk status FEP |
| `id_rtu` | integer | `0` untuk status FEP |
| `protocol` | string | `IEC61850` |
| `channel` | integer | **Hanya** di status ping: `iec61850s.channel_number` |
| `status` | integer | `1` = terkoneksi / terjangkau / FEP jalan, `0` = putus / tidak terjangkau / FEP berhenti |
| `time` | integer | ms epoch UTC |

Kapan dikirim:

| Variasi | Kapan |
|---|---|
| Koneksi MMS (tanpa `channel`) | `1` saat sesi MMS aktif; `0` saat sesi berakhir, saat gagal konek pertama dari rangkaian gagal, STATE false, RESET, shutdown (semua IED). Heartbeat semua IED tiap `STATUS_HEARTBEAT_INTERVAL_SECS` (default 10) |
| Channel / ping | Hasil ping ICMP tiap `PING_INTERVAL_MS` (default 5000): dikirim saat berubah atau tiap `PING_RESEND_EVERY` (default 6) hasil sama. Hanya IP literal, butuh `CAP_NET_RAW` |
| Sistem (`id_rtu` 0) | `1` saat FEP start, `0` saat FEP berhenti |

`docs/examples/produce/RTU_STATUS__connection.json`

```json
{
  "ip_address": "10.10.1.21",
  "id_rtu": 9001,
  "protocol": "IEC61850",
  "status": 1,
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_STATUS__channel.json`

```json
{
  "ip_address": "10.10.1.21",
  "id_rtu": 9001,
  "protocol": "IEC61850",
  "channel": 1,
  "status": 0,
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_STATUS__system.json`

```json
{
  "ip_address": "system",
  "id_rtu": 0,
  "protocol": "IEC61850",
  "status": 1,
  "time": 1794711845123
}
```

### 3.3 `RTU_LOGGING`

Struct: `RtuLoggingMessage`. Key = `id_rtu`. Karakter non-ASCII di `logging` dibuang.

| Field | Tipe | Arti |
|---|---|---|
| `ip_address` | string | IP IED (disaring) |
| `protocol` | string | `IEC61850` |
| `id_rtu` | integer | RTU |
| `logging` | string | Isi baris log (format di bawah) |
| `time` | integer | ms epoch UTC |

Tiga jenis isi `logging`:

**a. Kejadian** — `[LEVEL] pesan`, level `INFO` / `WARN` / `ERROR`:

| Pesan | Level |
|---|---|
| `terkoneksi MMS <ip>:<port> mode <REPORT\|POLLING\|HYBRID>` | INFO |
| `koneksi MMS terputus` | WARN |
| `gagal konek MMS: <error>` (sekali per rangkaian gagal) | ERROR |
| `berhenti reconnect, batas percobaan tercapai` | ERROR |
| `mode <mode> tapi tidak ada RCB enabled di iec61850_report_controls` (sekali per sesi) | WARN |
| `<n> datapoint IEC61850 tanpa baris master_datapoint_iec61850s (IOA ...)` (sekali per sesi) | WARN |
| `RCB <ref> gagal: <error>; dicoba ulang tiap <n> detik` / `RCB <ref> aktif setelah dicoba ulang` | WARN / INFO |
| `RCB <ref>: EntryID <hex> ditolak IED (...), enable tanpa EntryID; event selama putus bisa hilang` | WARN |
| `<alasan>, <error cek>; reconnect` (liveness / polling timeout) | WARN |
| `parameter OSI: <error>` | ERROR |
| `Select\|Operate\|Cancel ioa <ioa> sukses` / `... gagal: <alasan>` (hasil command) | INFO / WARN |

**b. Lalu lintas data** — `<timestamp> <isi>`, hanya kalau `LOG_DATA_TRAFFIC=true` (default `false`), maks
`LOG_DATA_MAX_PER_SEC` baris per IED per detik (default 50, `0` = tanpa batas). Baris yang lewat batas dilewati, lalu
bersama baris data pertama di detik berikutnya dikirim baris ringkas `[INFO] <n> log data dilewati (batas LOG_DATA_MAX_PER_SEC)`
(tanpa timestamp). Format ([traffic.rs](../src/iec61850/traffic.rs)):

```text
REPORT RX - RCB: <nama RCB>, Dataset: <nama dataset>, Reason: <dchg,qchg,dupd,integrity,gi|->, Entries: <n>, SeqNum: <n|->[, EntryID: <hex>]
GI TX - RCB: <nama RCB>
GI RX - RCB: <nama RCB>, Entries: <n>          (report yang memuat reason gi)
POLL RX - Group: <LD/LN>[<FC>], Elements: <n>
```

`EntryID` hanya untuk BRCB (`.BR.`). Nama RCB/dataset = bagian terakhir reference.

**c. CONTROL** — `<timestamp> <isi>`, **selalu** dikirim (audit), tidak ikut `LOG_DATA_TRAFFIC`:

```text
CONTROL TX - <SELECT|OPERATE|CANCEL> <object_reference> ctlVal=<nilai> orIdent=<user>   (CANCEL: tanpa ctlVal)
CONTROL RX - <SELECT|OPERATE|CANCEL> <object_reference> positive
CONTROL RX - <SELECT|OPERATE|CANCEL> <object_reference> negative AddCause=<nama>
CONTROL RX - CommandTermination <object_reference> <positive|negative>
CONTROL SKIP - SELECT <object_reference> (direct, tidak dikirim)                        (SELECT pada ctlModel direct)
```

Command yang ditolak FEP **sebelum** dikirim (value tidak cocok CDC, `value` kosong, ctlModel status-only) **tidak** ditulis
`CONTROL TX`, melainkan `[ERROR] CONTROL <SELECT|OPERATE|CANCEL> <object_reference> tidak dikirim: <alasan>`.

`ctlVal`: `true`/`false` (SPC/DPC), integer (INC/ENC), float (APC), `stop`/`lower`/`higher` (BSC).
`CONTROL RX` hanya untuk jawaban IED (timeout / koneksi putus tidak ditulis sebagai RX). Nama AddCause: tabel di [3.5](#35-rtu_command).

`docs/examples/produce/RTU_LOGGING__event_connect.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "[INFO] terkoneksi MMS 10.10.1.21:102 mode REPORT",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__event_disconnect.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "[WARN] koneksi MMS terputus",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__traffic_report.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:05.123456Z REPORT RX - RCB: brcbST01, Dataset: dsStatus, Reason: dchg, Entries: 2, SeqNum: 15, EntryID: 0a00000000000000",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__traffic_gi_tx.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:05.123456Z GI TX - RCB: brcbST01",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__traffic_gi_rx.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:05.223456Z GI RX - RCB: brcbST01, Entries: 12",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__traffic_poll.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:06.000000Z POLL RX - Group: simpleIOGenericIO/GGIO1[MX], Elements: 4",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__traffic_dropped.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "[INFO] 12 log data dilewati (batas LOG_DATA_MAX_PER_SEC)",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_tx.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:07.000000Z CONTROL TX - SELECT BAY1LD0/CSWI1.Pos ctlVal=true orIdent=operator1",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_rx_positive.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:07.050000Z CONTROL RX - SELECT BAY1LD0/CSWI1.Pos positive",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_rx_negative.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:08.000000Z CONTROL RX - OPERATE BAY1LD0/CSWI1.Pos negative AddCause=blocked-by-interlocking",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_termination.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:09.000000Z CONTROL RX - CommandTermination BAY1LD0/CSWI1.Pos positive",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_skip.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "2026-11-15T03:04:07.000000Z CONTROL SKIP - SELECT BAY1LD0/CSWI1.Pos (direct, tidak dikirim)",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_LOGGING__control_rejected.json`

```json
{
  "ip_address": "10.10.1.21",
  "protocol": "IEC61850",
  "id_rtu": 9001,
  "logging": "[ERROR] CONTROL OPERATE BAY1LD0/CSWI1.Pos tidak dikirim: value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan 0",
  "time": 1794711845123
}
```

### 3.4 Urutan pesan satu command (SBO normal, sukses)

```text
RTU_EVENTS       COMMAND SELECT_RTU key=k1
RTU_LOGGING      CONTROL TX - SELECT ...           CONTROL RX - SELECT ... positive     [INFO] Select ioa 3005 sukses
RTU_COMMAND      event=SELECT status=active key=k1
RTU_EVENTS       COMMAND OPERATE_RTU key=k2
RTU_LOGGING      CONTROL TX - OPERATE ...          CONTROL RX - OPERATE ... positive    [INFO] Operate ioa 3005 sukses
RTU_COMMAND      event=OPERATE status=active key=k2
RTU_DATAS        DPS ... action=DATA_CHANGE        (report posisi baru dari IED)
```

### 3.5 `RTU_COMMAND`

Dibuat di [rtu_event/command.rs](../src/kafka/handlers/rtu_event/command.rs). Key = `key` payload (`"unknown"` kalau kosong).
Satu balasan per COMMAND, termasuk yang gagal sebelum dieksekusi (kecuali JSON tidak valid tanpa `key` yang terbaca).

| Field | Tipe | Arti |
|---|---|---|
| `hostname` | string | `ip_address` payload; kosong → IP dari konfigurasi IED |
| `port` | integer | Port dari konfigurasi IED; IED tidak dikenal → `port` payload, tanpa `port` → 102 |
| `protocol` | string | `IEC61850` |
| `tag` | string | `tag` payload (`""` kalau tidak ada) |
| `command_type` | string | `type` payload apa adanya (`""` kalau tidak ada) |
| `command_value` | integer / float / null | `value` payload; bilangan bulat tetap integer, `null` kalau tidak ada (CANCEL) |
| `qualifier` | integer | `qualifier` payload, default `0` |
| `event` | string | `SELECT`, `OPERATE`, `CANCEL` |
| `status` | string | `active` (sukses) / `inactive` (gagal) |
| `msg` | string | `<EVENT> success RTU status is Active` / `<EVENT> failed: <alasan> - RTU status is Inactive` |
| `key` | string | `key` payload (`""` kalau tidak ada) |
| `time` | integer | ms epoch UTC |

Alasan gagal: tabel di [2.1 COMMAND](#command).

`docs/examples/produce/RTU_COMMAND__active.json`

```json
{
  "hostname": "10.10.1.21",
  "port": 102,
  "protocol": "IEC61850",
  "tag": "BAY1_CB_3005",
  "command_type": "DPC",
  "command_value": 2,
  "qualifier": 0,
  "event": "OPERATE",
  "status": "active",
  "msg": "OPERATE success RTU status is Active",
  "key": "cmd-20261115-0001",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_COMMAND__active_cancel.json`

```json
{
  "hostname": "10.10.1.21",
  "port": 102,
  "protocol": "IEC61850",
  "tag": "",
  "command_type": "",
  "command_value": null,
  "qualifier": 0,
  "event": "CANCEL",
  "status": "active",
  "msg": "CANCEL success RTU status is Active",
  "key": "cmd-20261115-0003",
  "time": 1794711845123
}
```

Ditolak IED dengan AddCause 10 (`blocked-by-interlocking`):

`docs/examples/produce/RTU_COMMAND__inactive_addcause.json`

```json
{
  "hostname": "10.10.1.21",
  "port": 102,
  "protocol": "IEC61850",
  "tag": "BAY1_CB_3005",
  "command_type": "DPC",
  "command_value": 2,
  "qualifier": 0,
  "event": "OPERATE",
  "status": "inactive",
  "msg": "OPERATE failed: operate gagal: ditolak IED (LastApplError error=1, addCause=10 (blocked-by-interlocking)) - RTU status is Inactive",
  "key": "cmd-20261115-0001",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_COMMAND__inactive_not_control.json`

```json
{
  "hostname": "10.10.1.21",
  "port": 102,
  "protocol": "IEC61850",
  "tag": "",
  "command_type": "",
  "command_value": 1,
  "qualifier": 0,
  "event": "SELECT",
  "status": "inactive",
  "msg": "SELECT failed: IOA 1001 bukan titik kontrol (FC CO) di RTU ini - RTU status is Inactive",
  "key": "cmd-20261115-0007",
  "time": 1794711845123
}
```

`docs/examples/produce/RTU_COMMAND__inactive_not_connected.json`

```json
{
  "hostname": "10.10.1.21",
  "port": 102,
  "protocol": "IEC61850",
  "tag": "BAY1_CB_3005",
  "command_type": "DPC",
  "command_value": 2,
  "qualifier": 0,
  "event": "OPERATE",
  "status": "inactive",
  "msg": "OPERATE failed: IED tidak terkoneksi - RTU status is Inactive",
  "key": "cmd-20261115-0001",
  "time": 1794711845123
}
```

AddCause (IEC 61850-7-2, enum `ControlAddCause` libIEC61850). Di `msg` RTU_COMMAND ditulis kode + nama (`addCause=10 (blocked-by-interlocking)`),
di `RTU_LOGGING` `CONTROL RX` hanya nama:

| Kode | Nama | Kode | Nama |
|---|---|---|---|
| 0 | `unknown` | 14 | `1-of-n-control` |
| 1 | `not-supported` | 15 | `abortion-by-cancel` |
| 2 | `blocked-by-switching-hierarchy` | 16 | `time-limit-over` |
| 3 | `select-failed` | 17 | `abortion-by-trip` |
| 4 | `invalid-position` | 18 | `object-not-selected` |
| 5 | `position-reached` | 19 | `object-already-selected` |
| 6 | `parameter-change-in-execution` | 20 | `no-access-authority` |
| 7 | `step-limit` | 21 | `ended-with-overshoot` |
| 8 | `blocked-by-mode` | 22 | `abortion-due-to-deviation` |
| 9 | `blocked-by-process` | 23 | `abortion-by-communication-loss` |
| 10 | `blocked-by-interlocking` | 24 | `abortion-by-command` |
| 11 | `blocked-by-synchrocheck` | 25 | `none` |
| 12 | `command-already-in-execution` | 26 | `inconsistent-parameters` |
| 13 | `blocked-by-health` | 27 | `locked-by-other-client` |

Kode lain ditulis sebagai angka. `LastApplError error`: `0` no-error, `1` unknown, `2` timeout-test-not-ok, `3` operator-test-not-ok.

### 3.6 `MANUAL_SET_EVENT_RESPONSE`

Struct: `ManualSetEventResponse`. Key = `"IEC61850"`.

| Field | Tipe | Ada kalau | Arti |
|---|---|---|---|
| `status` | string | selalu | `SUCCESS` (tag ditemukan) / `FAILED` |
| `protocol` | string | selalu | `protocol` payload apa adanya |
| `tag` | string | selalu | `tag` payload |
| `manual_set` | boolean | SUCCESS | Nilai baru |
| `user` | string | SUCCESS dan payload punya `user` | `user` payload |
| `error` | string | FAILED | `Datapoint not found` |
| `timestamp` | integer | selalu | ms epoch UTC |

`docs/examples/produce/MANUAL_SET_EVENT_RESPONSE__success.json`

```json
{
  "status": "SUCCESS",
  "protocol": "IEC61850",
  "tag": "BAY1_ANIN1_2001",
  "manual_set": true,
  "user": "operator1",
  "timestamp": 1794711845123
}
```

`docs/examples/produce/MANUAL_SET_EVENT_RESPONSE__failed.json`

```json
{
  "status": "FAILED",
  "protocol": "IEC61850",
  "tag": "TAG_TIDAK_ADA",
  "error": "Datapoint not found",
  "timestamp": 1794711845123
}
```

### 3.7 `IEC61850_BROWSE_RESPONSE`

Dibuat di [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs). Key = `key` payload (`"unknown"` kalau kosong).

| Field | Tipe | Arti |
|---|---|---|
| `key` | string | `key` payload (`""` kalau tidak ada) |
| `id_rtu` | integer / null | `id_rtu` payload (`null` kalau tidak diisi) |
| `status` | string | `SUCCESS` / `FAILED` |
| `message` | string | SUCCESS: `<total> objek` (jumlah semua bagian); FAILED: alasan |
| `objects` | array | Objek di bagian ini (`[]` kalau FAILED) |
| `objects[].object_reference` | string | `LD/LN.DO` atau `LD/LN.DO.SDO` (untuk `master_datapoint_iec61850s.object_reference`) |
| `objects[].fc` | string | `ST`, `MX`, `CO`, `SP` |
| `objects[].cdc` | string / null | CDC tebakan dari struktur DO: `SPS DPS INS SPC DPC INC ACT ACD BCR BSC APC MV CMV`; `null` = tidak dikenali. ENS/ENC terbaca INS/INC |
| `part` | integer | Nomor bagian, mulai 1 |
| `total_parts` | integer | Jumlah bagian |

Satu DO bisa muncul beberapa kali (satu per FC). JSON tiap bagian ≤ ~900 KB (di bawah `message.max.bytes` default 1 MB);
lebih → beberapa pesan dengan `key` dan `message` sama, `part` 1..`total_parts`. Gabungkan `objects` semua bagian.

`docs/examples/produce/IEC61850_BROWSE_RESPONSE__success.json`

```json
{
  "key": "browse-9001-01",
  "id_rtu": 9001,
  "status": "SUCCESS",
  "message": "4 objek",
  "objects": [
    {
      "object_reference": "simpleIOGenericIO/GGIO1.AnIn1",
      "fc": "MX",
      "cdc": "MV"
    },
    {
      "object_reference": "simpleIOGenericIO/GGIO1.Ind1",
      "fc": "ST",
      "cdc": "SPS"
    },
    {
      "object_reference": "simpleIOGenericIO/GGIO1.SPCSO1",
      "fc": "ST",
      "cdc": "SPC"
    },
    {
      "object_reference": "simpleIOGenericIO/GGIO1.SPCSO1",
      "fc": "CO",
      "cdc": "SPC"
    }
  ],
  "part": 1,
  "total_parts": 1
}
```

Multi-part (di sini dibagi dengan batas kecil supaya contoh pendek; FEP memakai ~900 KB):

`docs/examples/produce/IEC61850_BROWSE_RESPONSE__multipart_1.json`

```json
{
  "key": "browse-9001-02",
  "id_rtu": 9001,
  "status": "SUCCESS",
  "message": "4 objek",
  "objects": [
    {
      "object_reference": "simpleIOGenericIO/GGIO1.AnIn1",
      "fc": "MX",
      "cdc": "MV"
    },
    {
      "object_reference": "simpleIOGenericIO/GGIO1.Ind1",
      "fc": "ST",
      "cdc": "SPS"
    }
  ],
  "part": 1,
  "total_parts": 2
}
```

`docs/examples/produce/IEC61850_BROWSE_RESPONSE__multipart_2.json`

```json
{
  "key": "browse-9001-02",
  "id_rtu": 9001,
  "status": "SUCCESS",
  "message": "4 objek",
  "objects": [
    {
      "object_reference": "simpleIOGenericIO/GGIO1.SPCSO1",
      "fc": "ST",
      "cdc": "SPC"
    },
    {
      "object_reference": "simpleIOGenericIO/GGIO1.SPCSO1",
      "fc": "CO",
      "cdc": "SPC"
    }
  ],
  "part": 2,
  "total_parts": 2
}
```

`docs/examples/produce/IEC61850_BROWSE_RESPONSE__failed.json`

```json
{
  "key": "browse-9002-01",
  "id_rtu": 9002,
  "status": "FAILED",
  "message": "IED tidak terkoneksi",
  "objects": [],
  "part": 1,
  "total_parts": 1
}
```

---

## 4. Perbandingan dengan FEP IEC 104

Berdasarkan kode `iconics-fep-iec104` (`src/kafka/models.rs`, `src/kafka/handlers/rtu_event/command/*`,
`src/kafka/publisher/message_builder.rs`, `src/iec104/asdu/cot_mapper.rs`).

Sampel asli `RTU_DATAS` FEP IEC 104 (test `rtu_datas_sama_bentuk_dengan_sampel_iec104`: nama & tipe field sama dengan
`RTU_DATAS` FEP IEC 61850):

`docs/examples/iec104/RTU_DATAS__iec104_real.json`

```json
{
  "ip_address": "192.168.212.11",
  "protocol": "IEC104",
  "id_rtu": 83020254,
  "action": "SPONTANEOUS",
  "tag": "",
  "value": 218.3000030517578,
  "address": 5,
  "ca": 1,
  "asdu": 36,
  "type": "M_ME_TF_1",
  "quality": "GOOD",
  "flag": "GOOD",
  "group": "7fc706fc-1c7a-423c-9a9c-aa627f653611",
  "general_action": 0,
  "time": 1794796746325,
  "time_desc": "RTU"
}
```

### 4.1 Yang sama

- Nama topic consume/produce (kecuali `IEC61850_BROWSE_RESPONSE`, hanya 61850).
- Bentuk dan urutan field `RTU_DATAS` / `DATAPOINTS`, `RTU_STATUS`, `RTU_LOGGING`, `MANUAL_SET_EVENT_RESPONSE`, `RTU_COMMAND`.
- Key pesan: `id_rtu` (data/status/logging), `key` payload (RTU_COMMAND), protocol (manual set).
- Aturan `quality` (`INVALID` > `SUBSTITUTED` > `QUESTIONABLE` > `GOOD`), `flag` dipisah `|`, `time_desc` `RTU`/`SERVER`,
  `general_action` dari `master_general_actions`, `tag` kosong di `RTU_DATAS`, sanitasi tag.
- `CHANNEL` dan `RTU_COMMAND_CONFIG_REQUEST` hanya dicatat.
- IOA dari akhiran tag `_<angka>`, `data` diutamakan atas field datar.
- `msg` RTU_COMMAND `<EVENT> success RTU status is Active` / `<EVENT> failed: ... - RTU status is Inactive`.

### 4.2 Yang berbeda

| Hal | FEP IEC 104 | FEP IEC 61850 |
|---|---|---|
| `protocol` | `IEC104` (env `PROTOCOL`) | `IEC61850` |
| `ip_address` tingkat atas RTU_EVENTS | **wajib** (tanpa itu parse error) | opsional |
| COMMAND `data.ip_address`, `data.port` | **wajib** di dalam `data`; tanpa itu `data` gagal dibaca lalu jatuh ke field datar | opsional (`port` default 0) |
| COMMAND port default | 2404 | port konfigurasi IED (IED tidak dikenal: `port` payload, lalu 102) |
| COMMAND `type` | **wajib**, tipe ASDU (`C_SC_NA_1`, `C_DC_NA_1`, ...), menentukan perintah | opsional, hanya disalin ke respons; CDC dari mapping DB |
| COMMAND `value` | **integer** (`i32`) | angka (integer/float), string angka, boolean |
| COMMAND `action` | `SELECT` (lewat `url`), `SELECT_RTU`, `OPERATE_RTU` | `SELECT_RTU`, `OPERATE_RTU`, `CANCEL_RTU` |
| COMMAND `user` | tidak ada | `orIdent` (default `FEP-IEC61850`) |
| COMMAND `qualifier` | dikirim ke RTU (default 0) | tidak dikirim ke IED, dikembalikan di respons (default 0) |
| COMMAND pencarian RTU | dari `ip_address` | `id_rtu` dulu, baru `ip_address` |
| `RTU_DATAS.action` | COT: `SPONTANEOUS`, `PERIODIC`, `BACKGROUND_SCAN`, `INTERROGATED_BY_STATION`, `INTERROGATED_BY_GROUP_n`, ... | reason report: `DATA_CHANGE`, `QUALITY_CHANGE`, `DATA_UPDATE`, `INTEGRITY`, `GENERAL_INTERROGATION`, `POLLING` |
| `RTU_DATAS.type` / `asdu` / `ca` | nama tipe ASDU / type id / common address | CDC / `0` / `0` |
| `RTU_DATAS.time_desc = RTU` | waktu RTU dikoreksi `TIMEZONE` | waktu `t` sudah UTC, tidak dikoreksi |
| `MASTER_RTU_EVENTS.id_station` | wajib | diabaikan |

`action` beda nama antara 104 dan 61850, jadi konsumen sebaiknya memakai `general_action` (kode dari
`master_general_actions` per protocol): isi baris `IEC61850` dengan `name_action` = action 61850 dan `code` yang sama dengan
padanan 104 (mis. `DATA_CHANGE` ↔ `SPONTANEOUS`, `GENERAL_INTERROGATION` ↔ `INTERROGATED_BY_STATION`, `INTEGRITY` ↔ `PERIODIC`).

### 4.3 Payload COMMAND "aman untuk keduanya"

Bentuk yang terbaca kedua FEP: `ip_address` dan `port` di tingkat atas **dan** di dalam `data`, `value` integer,
`type` terisi, `key` terisi. Field `protocol` (dan `type`) tetap sesuai FEP tujuan: untuk 104 `protocol: "IEC104"` dan
`type` ASDU (`C_DC_NA_1`); untuk 61850 seperti di bawah. `id_rtu` dan `user` diabaikan FEP IEC 104.

`docs/examples/consume/RTU_EVENTS__COMMAND.compat104.json`

```json
{
  "event": "COMMAND",
  "protocol": "IEC61850",
  "ip_address": "10.10.1.21",
  "port": 102,
  "data": {
    "action": "OPERATE_RTU",
    "ip_address": "10.10.1.21",
    "port": 102,
    "id_rtu": 9001,
    "ioa": 3005,
    "tag": "BAY1_CB_3005",
    "type": "DPC",
    "value": 2,
    "key": "cmd-20261115-0005",
    "user": "operator1"
  }
}
```

---

## 5. Catatan ketidaksesuaian

Ditemukan saat menulis dokumen ini. **Kode tidak diubah**; perlu keputusan tim.

1. ~~CDC: `record` vs `data`~~ — **diperbaiki**: FEP membaca `record` (cadangan `data`), UPDATE kolom status `iec61850s` tidak lagi memicu reload, tabel lain diabaikan.
2. **README `group`.** README menulis `group = master_data_objects.initial = CDC`. Di kode `group` berisi `id_group` (UUID)
   dari baris `master_data_objects` yang `initial`-nya = CDC, bukan CDC itu sendiri.
3. ~~AddCause di `RTU_COMMAND` hanya kode~~ — **diperbaiki**: `addCause=10 (blocked-by-interlocking)`.
4. ~~COMMAND `data` gagal dibaca → diam-diam pakai field datar~~ — **diperbaiki**: ditolak `inactive` `field data tidak valid: ...`.
5. ~~Default port COMMAND tidak seragam~~ — **diperbaiki**: port konfigurasi IED (IED tidak dikenal: `port` payload, lalu 102).
6. ~~`qualifier` payload diabaikan~~ — **diperbaiki**: dikembalikan di respons (default 0).
7. **Tanpa balasan** (sisa): COMMAND dengan JSON tidak valid **tanpa** `key` yang terbaca, `MANUAL_SET_EVENT` untuk `id_rtu`
   yang tidak berjalan (sengaja: FEP lain yang menjawab) atau event selain `TOGGLE_MANUAL_SET`. COMMAND dengan `action` tidak
   dikenal / field salah tipe / JSON tidak valid ber-`key` **sudah dibalas** `inactive`.
8. **Cek protocol tidak berlaku** untuk `MASTER_DATA_OBJECT_EVENT` (pesan IEC104 pun memicu reload group 61850) dan
   `CAPTURE_DATA_CHANGE`. Perbandingan protocol tidak peka huruf besar/kecil, README menulis "≠ `IEC61850`".
9. ~~BROWSE tidak mencari `id_rtu` dari `ip_address`~~ — **diperbaiki**: sama dengan COMMAND.
10. **CREATE/ADD/UPDATE dengan `ip_address` saja** hanya berhasil untuk IED yang sudah berjalan; RTU baru wajib `id_rtu`.
11. **`MASTER_DATAPOINT_EVENT.ioa` harus integer** (string → parse error), padahal `RTU_EVENTS.ioa` menerima string.
    `REMOVE` tidak dikenal di topic ini (hanya `DELETE`).
12. ~~Event GI pada mode POLLING menghasilkan `INTEGRITY`~~ — **diperbaiki**: `GENERAL_INTERROGATION`.
13. ~~`CONTROL TX` ditulis walau tidak ada request ke IED~~ — **diperbaiki**: SELECT direct → `CONTROL SKIP`, value tidak valid / status-only → baris `[ERROR] ... tidak dikirim`.
14. **Baris ringkas `[INFO] <n> log data dilewati ...` tanpa timestamp**, sementara baris lalu lintas lain diawali timestamp.
15. **README fase 8** menyebut `RTU_LOGGING` hanya "konek, putus, gagal konek, RCB gagal"; kode juga mengirim hasil command,
    liveness/reconnect, EntryID ditolak, parameter OSI, peringatan konfigurasi, RCB pulih, dan baris lalu lintas/CONTROL.
16. **Path lama di README**: `src/repository.rs`, `src/manager.rs`, `src/handlers.rs`, `src/command.rs` sudah dipecah ke
    `src/repository/`, `src/connection/manager.rs`, `src/kafka/handlers/`, `src/kafka/handlers/rtu_event/command.rs` +
    `src/domain/command.rs`.
17. **Komentar `models.rs`** "nama & bentuk field sama dengan IEC 104": `RtuEventMessage` 61850 menambah `user` dan membuat
    `ip_address` opsional; `MasterRtuEventMessage` 61850 tidak punya `id_station`.
18. ~~`GROUP_ID` tidak seragam~~ — **diperbaiki**: `IEC61850_FEP_GROUP` di kode, `.env.example`, dan configmap.
19. **BROWSE / COMMAND dengan `key` kosong**: key Kafka `"unknown"`, tetapi field `key` di isi pesan `""`.
