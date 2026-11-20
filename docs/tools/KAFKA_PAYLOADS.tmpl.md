# Payload Kafka FEP IEC 61850

Daftar lengkap pesan Kafka yang **dikonsumsi** dan **diproduksi** FEP IEC 61850 (`fep-iec61850`).
Sumber kebenaran: **kode** di branch `iec61850`, bukan README lama. Kalau kode berubah, dokumen ini ikut diubah.

> File ini **dihasilkan**: edit [docs/tools/KAFKA_PAYLOADS.tmpl.md](tools/KAFKA_PAYLOADS.tmpl.md) (teks) atau
> [scripts/gen_kafka_docs.py](../scripts/gen_kafka_docs.py) (contoh JSON), lalu jalankan `python3 scripts/gen_kafka_docs.py`.

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
| `RTU_EVENTS` | consume | (tidak dibaca) | Event RTU dari backend/GRITA: CREATE/ADD/UPDATE, DELETE/REMOVE, STATE, RESET, GI, CHANNEL, COMMAND, BROWSE, SCL_IMPORT | [event_handler.rs](../src/kafka/event_handler.rs) → [rtu_event/mod.rs](../src/kafka/handlers/rtu_event/mod.rs), [rtu_event/command.rs](../src/kafka/handlers/rtu_event/command.rs), [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs), [rtu_event/scl_import.rs](../src/kafka/handlers/rtu_event/scl_import.rs) |
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
| `IEC61850_BROWSE_RESPONSE` | produce | `key` payload (`"unknown"` kalau kosong) | Hasil event `BROWSE` / `SCL_IMPORT` (bisa beberapa bagian) | [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs), [rtu_event/scl_import.rs](../src/kafka/handlers/rtu_event/scl_import.rs) |

### 1.2 Aturan umum

- **Protocol.** FEP IEC 104 dan FEP IEC 61850 membaca topic yang sama. Pesan dengan `protocol` selain `IEC61850`
  **diabaikan tanpa log** (`is_ours`, perbandingan tidak peka huruf besar/kecil: `iec61850` juga diterima).
  Pengecualian: `CAPTURE_DATA_CHANGE` (tidak punya `protocol`; disaring per tabel, `master_data_objects` /
  `master_general_actions` per `record.protocol`) dan `MASTER_DATA_OBJECT_EVENT` (isi pesan tidak dibaca).
- **Consumer group.** Env `GROUP_ID`, default `IEC61850_FEP_GROUP` ([config/kafka.rs](../src/config/kafka.rs)), sama dengan
  `.env.example` dan `kubernetes/configmap.yaml` (dicek test `group_id_seragam`). Harus berbeda dengan group FEP IEC 104.
  **Hanya satu instance FEP IEC 61850 aktif per lingkungan**: dua instance = setiap COMMAND terkirim dua kali ke IED
  (group beda) atau event terbagi antar-instance (group sama).
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
| CREATE/ADD/UPDATE | `id_rtu` tingkat atas → `data.id_rtu` → dicari dari `ip_address` di antara IED yang **sedang berjalan** → dicari di tabel `iec61850s` (`ip_address` + `port` kalau ada, `is_deleted = false`, harus tepat satu baris) |
| DELETE/REMOVE, STATE, RESET, GI, CHANNEL | `id_rtu` tingkat atas → `data.id_rtu` → dicari dari `ip_address` di antara IED yang **sedang berjalan** |
| COMMAND | `data.id_rtu` → `id_rtu` tingkat atas → dicari dari `data.ip_address` / `ip_address` di antara IED yang berjalan |
| BROWSE | `id_rtu` tingkat atas → `data.id_rtu` → dicari dari `data.ip_address` / `ip_address` di antara IED yang berjalan |

IP / hostname dibandingkan **apa adanya** (trim, tidak peka huruf besar/kecil, tidak ada karakter yang dibuang): `ied-sim-1`,
`192.168.212.25`, `IED-SIM-1` = `ied-sim-1`. Tidak ketemu → event tidak dijalankan (log `WARN`), kecuali COMMAND/BROWSE yang tetap dibalas `FAILED`/`inactive`.

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
DB tidak tersedia → log `WARN`.

Tanpa `id_rtu` (dan IED dengan IP itu tidak sedang berjalan): `id_rtu` dicari di tabel `iec61850s` dengan `ip_address` (trim,
tidak peka huruf besar/kecil), `port` kalau ada di payload (atas → `data`), dan `is_deleted = false`. Tidak ada baris →
`WARN ... tidak ada baris iec61850s untuk ip ...`; **lebih dari satu baris** (mis. dua IED di IP sama beda port, payload tanpa
`port`) → `WARN ... N baris iec61850s cocok ..., tidak dipilih; kirim id_rtu` dan event tidak dijalankan.
**Backend sebaiknya selalu mengirim `id_rtu`.**

Minimal:

@@consume/RTU_EVENTS__CREATE@@

Lengkap:

@@consume/RTU_EVENTS__UPDATE@@

Hanya IP (RTU yang sudah berjalan):

@@consume/RTU_EVENTS__ADD.by_ip@@

Log FEP: `INFO [RTU_EVENTS] event=UPDATE ip=10.10.1.21 id_rtu=Some(9001)`.

#### DELETE / REMOVE

Efek: hentikan thread IED (koneksi MMS ditutup). Tidak ada balasan; `RTU_STATUS` 0 dikirim thread saat sesi berakhir
(kalau sedang terkoneksi).

@@consume/RTU_EVENTS__DELETE@@

@@consume/RTU_EVENTS__REMOVE.data@@

#### STATE

`state_rtu` wajib. `false`: hentikan thread + `RTU_STATUS` 0, tidak reconnect. `true`: baca ulang dari DB lalu konek.
**Idempoten**: STATE true untuk IED yang sudah jalan dan STATE false untuk IED yang sudah berhenti tidak melakukan apa pun
(log `INFO STATE sama dengan kondisi IED, tidak ada aksi`). Tanpa `state_rtu`: log
`WARN [RTU_EVENTS] STATE RTU 9001: STATE tanpa state_rtu`.

Alur `state_rtu` / `status_rtu` di backend (**DB adalah sumber kebenaran `state_rtu`**):

```text
frontend -> backend UPDATE iec61850s.state_rtu -> CDC iec61850s (state_rtu berubah)   ┐ dua pesan untuk
                                               -> RTU_EVENTS STATE                    ┘ perubahan yang sama
FEP start/stop IED -> RTU_STATUS -> backend UPDATE status_rtu / rtu_online_at / rtu_offline_at
                                 -> CDC iec61850s (hanya kolom status) = "gema", diabaikan FEP
```

- STATE dan CDC untuk perubahan yang sama = **satu** start/stop. Urutan STATE dulu: CDC dibandingkan dengan salinan DB yang
  sudah dimuat STATE → sama → tidak ada aksi. Urutan CDC dulu: CDC menjadwalkan cek ulang baris (debounce); STATE
  menjalankan/menghentikan IED dan memuat baris; saat cek ulang baris sudah sama → tidak ada aksi.
- IED yang dihentikan lewat STATE false **tidak** dijalankan lagi oleh CDC yang membawa `state_rtu = true` yang tidak berubah
  dari salinan DB (mis. gema kolom status). Jalan lagi hanya lewat STATE true atau `state_rtu` di DB berubah false → true.
- Backend sebaiknya selalu menyimpan `state_rtu` ke DB sebelum mengirim STATE.

@@consume/RTU_EVENTS__STATE.off@@

@@consume/RTU_EVENTS__STATE.on@@

#### RESET

`RTU_STATUS` 0 (kalau IED berjalan), lalu sama dengan UPDATE (reload + konek ulang).

@@consume/RTU_EVENTS__RESET@@

#### GI

Mode `REPORT`/`HYBRID`: GI ke semua RCB yang aktif → data `GENERAL_INTERROGATION` di `RTU_DATAS`.
Mode `POLLING`: siklus polling penuh sekarang juga → semua titik dengan action `GENERAL_INTERROGATION` (`general_action` 1, sama dengan mode REPORT).
Mode `HYBRID`: GI ke RCB + polling penuh titik `MX`, keduanya `GENERAL_INTERROGATION`.
IED tidak berjalan: log `WARN [RTU_EVENTS] GI RTU 9001: IED tidak berjalan`. Kalau `LOG_DATA_TRAFFIC=true`, baris
`GI TX` / `GI RX` muncul di `RTU_LOGGING`.

@@consume/RTU_EVENTS__GI@@

Diabaikan (protocol lain):

@@consume/RTU_EVENTS__GI.ignored_iec104@@

#### CHANNEL

Hanya dicatat di log (sama dengan FEP IEC 104). Isi `data` tidak dibaca.

@@consume/RTU_EVENTS__CHANNEL@@

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
`orCat` = env `CONTROL_OR_CAT` (default 3 = remote-control), bisa ditimpa per titik dengan kolom opsional
`iec61850_command_formulas.or_cat` (migrations/0008). `ctlNum` dicatat FEP per objek kontrol: SELECT/SBOw dan OPERATE direct
menaikkan ctlNum, OPERATE SBO dan CANCEL memakai ctlNum yang sama; CANCEL juga membawa ctlVal SELECT terakhir.

Env terkait command:

| Env | Default | Arti |
|---|---|---|
| `CONTROL_OR_CAT` | `3` | orCat (0..8) untuk semua command; tidak valid → 3 |
| `CONTROL_PRECHECK_POSITION` | `false` | `true`: sebelum SELECT titik DPC, baca `stVal` & `q` DO kontrol; posisi 00/11 atau q invalid → ditolak tanpa dikirim ke IED |
| `CONTROL_DEBUG` | `false` | `true`: log ringkas struktur yang dikirim (ctlModel, ctlVal, origin, ctlNum, Check) + AddCause yang diterima |

Minimal:

@@consume/RTU_EVENTS__COMMAND.min@@

Lengkap (DPC, SBO: kirim SELECT lalu OPERATE dengan `key` sendiri-sendiri):

@@consume/RTU_EVENTS__COMMAND.full@@

Format datar (tanpa `data`, IOA dari tag, value string):

@@consume/RTU_EVENTS__COMMAND.flat@@

CANCEL (tanpa `value`):

@@consume/RTU_EVENTS__COMMAND.cancel@@

APC (float) dan BSC (Tcmd higher):

@@consume/RTU_EVENTS__COMMAND.apc@@

@@consume/RTU_EVENTS__COMMAND.bsc@@

Aman untuk FEP IEC 104 dan 61850 (lihat [bagian 4](#4-perbandingan-dengan-fep-iec-104)):

@@consume/RTU_EVENTS__COMMAND.compat104@@

Log FEP: `INFO [COMMAND] OPERATE ip=10.10.1.21 id_rtu=Some(9001) ioa=Some(3005) value=Some(2.0) key=Some("cmd-20261115-0005")`,
lalu `INFO [COMMAND] OPERATE sukses ioa=Some(3005)` atau `ERROR [COMMAND] OPERATE gagal ioa=Some(3005): <alasan>`.
Di `RTU_LOGGING`: baris `CONTROL TX/RX` + `[INFO] Operate ioa 3005 sukses` / `[WARN] Operate ioa 3005 gagal: <alasan>`.

Gagal / ditolak (semua dibalas `RTU_COMMAND` `status: "inactive"`, alasan di `msg`):

| Kondisi | Alasan di `msg` |
|---|---|
| JSON tidak terbaca struct (salah tipe di tingkat atas / JSON rusak) tapi `event` = COMMAND, `protocol` = IEC61850, dan `key` terbaca | `payload tidak valid: <error serde>`. `event` respons dari `action` kalau terbaca, selain itu `UNKNOWN`. Tanpa `key` → **sengaja** tidak dibalas (tidak bisa dicocokkan GRITA): log `WARN` + `RTU_LOGGING` `[WARN] COMMAND tidak valid tanpa key, tidak dibalas: ...` (kalau IED diketahui dari `id_rtu` / IP) |
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
| `CONTROL_PRECHECK_POSITION=true` dan posisi tidak valid | `posisi tidak valid di IED (stVal=00 (intermediate), q=INVALID\|FAILURE), SELECT tidak dikirim` |
| IED menolak (negative response) | `<select\|operate\|cancel> gagal: <teks IedClientError atau "ditolak IED"> (LastApplError error=<0..3>, addCause=<kode> (<nama>), ctlNum=<n>)`. LastApplError diambil dari InformationReport dengan **ctlNum yang sama** (objek kontrol, lalu level koneksi), ditunggu maks 300 ms; tidak ada → `(LastApplError tidak diterima)` |
| CommandTermination negatif | `CommandTermination- gagal: ... (LastApplError error=..., addCause=<kode> (<nama>), ctlNum=<n>)` |
| CommandTermination tidak datang | `CommandTermination tidak diterima (timeout)` |
| Tidak ada hasil dalam 30 detik | `timeout menunggu hasil command` |
| Thread IED berhenti di tengah | `thread IED berhenti sebelum command selesai` |

#### BROWSE

Handler: [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs), browse: [iec61850/browse.rs](../src/iec61850/browse.rs).
Dijalankan di thread IED **bertahap** (satu LN per langkah, jatah 200 ms per putaran), diselingi polling dan command, dengan
koneksi MMS yang sama. LD → LN → DO → FC (`ST`, `MX`, `CO`, `SP`):

1. Satu `getVariableSpecification` per LN + FC.
2. Gagal (mis. `malformed-message`: respons type spec LN terlalu besar untuk PDU atau tidak bisa di-parse libIEC61850) →
   struktur FC itu dibangun dari **GetNameList** (`getLogicalDeviceVariables`, nama MMS `LN$FC$DO$DA`).
   Tipe `stVal` (untuk CDC) dibaca dari nilainya kalau `BROWSE_READ_VALUES=true` (default).
3. Objek FC `CO`: `control` (SBOw/Oper/Cancel dari struktur, `ctlModel` & `sboTimeout` dari CF kalau `BROWSE_READ_VALUES`).
4. RCB tiap LN (`.BR.` / `.RP.`): `RptEna`, `DatSet`, `RptID`, `TrgOps`, `IntgPd`, `ConfRev`, `BufTm`, `OptFlds`, dan anggota
   dataset → `rcbs`.

Kegagalan nyata satu LN / DO / RCB **tidak** menggagalkan browse (dicatat di `errors`); LN+FC yang memang tidak ada
(`malformed-message` + GetNameList tanpa FC itu) bukan error. Batas total `BROWSE_TIMEOUT_MS`
(default 300000): tercapai → `SUCCESS` dengan hasil sebagian + error `{ref: "browse"}`. Timeout per request = `request_timeout_ms`.
Max PDU hasil negosiasi ada di log koneksi (`RTU_LOGGING` `terkoneksi MMS ..., max PDU N`); usulan FEP bisa diatur dengan
`MMS_MAX_PDU_SIZE`. Debug satu objek: `cargo run --bin varspec -- <host> <port> <LD/LN.DO>` (lihat README).
Balasan ke `IEC61850_BROWSE_RESPONSE` (bagian [3.7](#37-iec61850_browse_response)).

| Field | Tipe JSON | Wajib | Default | Posisi | Arti |
|---|---|---|---|---|---|
| `id_rtu` | integer | ya* | - | atas → `data` | IED yang di-browse |
| `ip_address` | string | ya* | - | `data` → atas | *Dipakai mencari `id_rtu` kalau `id_rtu` kosong (IED yang sedang berjalan) |
| `key` | string | tidak | `""` | atas → `data` | Disalin ke respons + key Kafka respons |

@@consume/RTU_EVENTS__BROWSE@@

@@consume/RTU_EVENTS__BROWSE.data@@

Gagal: tanpa `id_rtu` dan `ip_address` → `FAILED` `id_rtu atau ip_address wajib diisi`; IP tidak cocok IED yang berjalan →
`FAILED` `RTU tidak ditemukan untuk IP <ip>`; IED tidak berjalan / belum terkoneksi → `FAILED` `IED tidak terkoneksi`;
browse lain untuk IED yang sama masih berjalan → `FAILED` `browse lain masih berjalan untuk IED ini`; daftar LD tidak terbaca /
koneksi putus di tengah → `FAILED` dengan teks error; tidak ada jawaban thread IED dalam `BROWSE_TIMEOUT_MS` + 30 detik →
`FAILED` `timeout menunggu hasil browse`.

#### SCL_IMPORT

Handler: [rtu_event/scl_import.rs](../src/kafka/handlers/rtu_event/scl_import.rs), parser: [scl.rs](../src/scl.rs) (`catalog`).
Backend menyimpan file SCL (ICD/CID/SCD) di tabel `iec61850_scl_files`; FEP membacanya dan mengirim **katalog model IED
dengan format yang sama dengan BROWSE** ke `IEC61850_BROWSE_RESPONSE` (`source: "SCL"`, bagian [3.7](#37-iec61850_browse_response)).
FEP **tidak** konek / menulis ke IED, tidak menyentuh koneksi, polling, RCB aktif, atau `RTU_STATUS`, jadi event ini
diproses walau RTU OFF / tidak berjalan / tidak terkoneksi. FEP tidak menulis tabel konfigurasi (mapping dihitung backend).

| Field | Tipe JSON | Wajib | Default | Posisi | Arti |
|---|---|---|---|---|---|
| `id_rtu` | integer | ya | - | atas → `data` | Filter `iec61850_scl_files.id_rtu` (tanpa pencarian dari IP) |
| `key` | string | tidak | `""` | atas → `data` | Disalin ke respons + key Kafka respons. Key sama dalam 10 menit terakhir → event **diabaikan** (hanya log) |
| `id_scl_file` | string (UUID) | ya | - | atas | `iec61850_scl_files.id` |
| `ied_name` | string / null | tidak | kolom `ied_name` | atas | IED yang dipilih di file. null + kolom kosong + file 1 IED → IED itu |

@@consume/RTU_EVENTS__SCL_IMPORT@@

@@consume/RTU_EVENTS__SCL_IMPORT.ied_name@@

Query: `SELECT file_name, content, ied_name, is_deleted FROM iec61850_scl_files WHERE id = $1 AND id_rtu = $2`
(`content` TEXT, hanya ditarik kalau ≤ `SCL_MAX_BYTES`). Tabel ini tidak punya trigger CDC (kalau ada, handler CDC
mengabaikannya). Dikerjakan di task sendiri (parsing di `spawn_blocking`), batas total `SCL_IMPORT_TIMEOUT_MS`.
`RTU_LOGGING`: `Import SCL mulai: file <id>`, `Import SCL selesai: <n> objek, <m> RCB, <k> error (<ms> ms)` /
`Import SCL gagal: <pesan> (<ms> ms)`.

Isi katalog (per `AccessPoint/Server/LDevice`, LN0 + LN):

- Nama LD: `ldName` kalau ada, kalau tidak `<IED name><LDevice inst>`. LN = prefix + lnClass + inst.
- `objects`: DO dari `LNodeType` → `DOType`, satu objek per FC (`ST`, `MX`, `CO`, `SP`) yang punya DA, urut nama DO; DO
  tanpa DA di FC itu (mis. `WYE`) → SDO (`LD/LN.DO.SDO`), sama dengan BROWSE. `cdc` = atribut `cdc` DOType (**asli**, jadi
  ENS/ENC tetap ENS/ENC, tidak seperti BROWSE). `control` (FC `CO`): `sbow`/`oper`/`cancel` dari DA, `ctl_model` &
  `sbo_timeout_ms` dari `DOI/DAI` (instance) yang menimpa `Val` default di DOType.
- `rcbs`: `ReportControl` per instance (indexed, default: `<nama>01`..`<nama><RptEnabled max>`; `indexed="false"`: nama apa
  adanya). `dataset` `LD/LN.<datSet>`, `members` dari FCDA (`LD/LN.DO[.DA]` + `fc`), `trg_ops` dari `TrgOps`
  (`period` → `integrity`, `gi` default true), `intg_pd_ms` `intgPd`, `rpt_id` `rptID` (kosong → null), `rpt_ena` null,
  plus `conf_rev` (`confRev`), `buf_tm_ms` (`bufTime`), `opt_flds` (atribut `OptFields` yang true, dipisah koma).
- `config_revs`: per LD, `LLN0` `DOI NamPlt / DAI configRev` → `{ld, reference: "<LD>/LLN0.NamPlt.configRev", value}`.
- Tipe (`LNodeType`/`DOType`) tidak ada → objek dilewati + `errors` `{ref: <objek>, error: "DOType \"X\" tidak ada di DataTypeTemplates"}`
  (tetap SUCCESS). DataSet RCB tidak ada → `members: []` + error `{ref: "LD/LN.BR.<nama>"}`.

Gagal (`FAILED`, `errors: [{ref: "scl", error: <pesan>}]`):

| `message` | Kapan |
|---|---|
| `id_rtu wajib diisi` / `id_scl_file wajib diisi` | Field kosong |
| `File SCL tidak ditemukan` | Tidak ada baris (`id` + `id_rtu`), `is_deleted = true`, atau `id_scl_file` bukan UUID |
| `File SCL terlalu besar` | `octet_length(content)` > `SCL_MAX_BYTES` |
| `File bukan SCL yang valid: <detail>` | XML rusak, ada DTD, atau root bukan `<SCL>` |
| `File SCL tidak berisi IED` | Tidak ada `<IED>` |
| `Pilih IED` | `ied_name` kosong dan file berisi lebih dari satu IED; `ied_names: [{name, ip}]` |
| `IED <x> tidak ada di file` | `ied_name` tidak ada di file; `ied_names: [{name, ip}]` |
| `Import SCL timeout` | Lebih dari `SCL_IMPORT_TIMEOUT_MS` |
| `DB tidak tersedia: ...` / `query iec61850_scl_files: ...` | DB error |

### 2.2 `MASTER_DATAPOINT_EVENT`

Struct: `MasterDatapointEventMessage`. Handler: [master_datapoint.rs](../src/kafka/handlers/master_datapoint.rs).

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `id_rtu` | integer | ya | RTU pemilik datapoint |
| `protocol` | string | ya | `IEC61850` |
| `ioa` | integer / string angka | ya | IOA virtual (`2001` atau `"2001"`) |
| `event` | string | ya | `ADD`, `CREATE`, `UPDATE`, `DELETE` |
| `data.tag` | string | ADD/CREATE/UPDATE: ya | Tag baru |

Efek: ADD/CREATE/UPDATE → tag `(id_rtu, ioa)` diganti di TagResolver; DELETE → tag dihapus. Lalu, kalau IED berjalan,
reload IED digabung selama `RELOAD_DEBOUNCE_MS` (mapping bisa ikut berubah). Tidak ada balasan.
Log: `INFO [MASTER_DATAPOINT] UPDATE id_rtu=9001 ioa=2001`. Event lain / tanpa `data.tag`:
`WARN [MASTER_DATAPOINT] event <x> tanpa tag / tidak dikenal` (tag tidak diubah, tidak reload).

@@consume/MASTER_DATAPOINT_EVENT__UPDATE@@

`ioa` sebagai string:

@@consume/MASTER_DATAPOINT_EVENT__UPDATE.ioa_string@@

@@consume/MASTER_DATAPOINT_EVENT__DELETE@@

### 2.3 `MASTER_DATA_OBJECT_EVENT`

Handler: [master_data_object.rs](../src/kafka/handlers/master_data_object.rs) (keputusan di `group_plan`). Group di-update
**per baris**, tanpa reconnect IED. GroupResolver menyimpan `id` tiap baris `master_data_objects` IEC61850: peta
`id → (initial, id_group)` dan `initial → id_group`.

| Pesan | Efek |
|---|---|
| CREATE/UPDATE, `protocol` bukan `IEC61850` | diabaikan (DEBUG) |
| CREATE/UPDATE, `protocol` `IEC61850` | entri baris itu diperbarui; `initial` berubah → entri initial lama dihapus |
| CREATE/UPDATE `is_deleted = true` | entri baris itu dihapus |
| DELETE (hanya `id`) | `id` ada di peta → entri dihapus; tidak ada → diabaikan (baris milik protokol lain) |
| Field tidak terbaca (bukan JSON, tanpa `id`, tanpa `protocol`, tanpa `initial`/`id_group`) | muat ulang semua group + `WARN` |

Baris dibaca dari `data` / `record` / `new_data`, kalau tidak ada dari pesan itu sendiri; `event` dari `event` / `action` /
`operation`. Tidak ada balasan Kafka.

> Contoh di bawah adalah **bentuk contoh** sesuai deskripsi backend (CREATE/UPDATE = baris lengkap, DELETE = `id` saja),
> **belum** sampel asli dari KaDeck. Ganti dengan sampel asli begitu tersedia.

@@consume/MASTER_DATA_OBJECT_EVENT__CREATE@@

@@consume/MASTER_DATA_OBJECT_EVENT__UPDATE.iec104@@

@@consume/MASTER_DATA_OBJECT_EVENT__DELETE@@

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
| `iec61850s` | `record.id_rtu`. `DELETE` atau `is_deleted = true` → hentikan IED. Selain itu `record` dibandingkan dengan **salinan terakhir baris DB** IED itu (bukan dengan status jalan/berhenti): kolom konfigurasi berubah → baca ulang baris dari DB setelah `RELOAD_DEBOUNCE_MS`, reload IED itu kalau masih beda dengan salinan (sudah ditangani event STATE → tidak ada aksi); hanya `channel_number` berubah → channel status ping diperbarui **tanpa reconnect MMS**; hanya kolom status → tidak ada aksi. IED belum dikenal (INSERT) → dijalankan kalau `state_rtu` tidak `false` |
| `iec61850_report_controls` | reload IED `record.id_rtu` |
| `master_datapoint_iec61850s`, `iec61850_command_formulas` | `id_rtu` dicari dari `record.id_master_datapoint` lewat `master_datapoints` → reload IED itu. Tidak ketemu (mis. DELETE dan datapoint sudah hilang) → reload **semua** IED + WARN |
| `master_data_objects` | muat ulang group (tanpa reconnect). `record.protocol` ada dan bukan `IEC61850` → diabaikan |
| `master_general_actions` | muat ulang general action (tanpa reconnect). `record.protocol` ada dan bukan `IEC61850` → diabaikan |
| `formulas` | muat ulang formula (tanpa reconnect) |
| `manual_sets` | muat ulang daftar tag manual set (tanpa reconnect) |
| lainnya (`dnp3s`, `iec104s`, ...) | diabaikan, hanya log DEBUG |

Kolom konfigurasi `iec61850s` (dipakai `RtuConfig`): `ip_address`, `port`, `local_ip_address`, `acquisition_mode`,
`poll_interval_ms`, `general_interrogation_interval`, `connect_timeout_ms`, `request_timeout_ms`, `reconnect_time_ms`,
`reconnect_retry_count`, `state_rtu`, `ap_title`, `ae_qualifier`, `p_selector`, `s_selector`, `t_selector` (+ `is_deleted`) →
reload; `channel_number` → hanya channel ping. Kolom lain **diabaikan**, termasuk kolom status yang di-UPDATE backend dari
`RTU_STATUS` FEP sendiri (`status_rtu`, `rtu_online_at`, `rtu_offline_at`, `status_channel`, `created_at`, `updated_at`, `name`, ...).
Kolom yang tidak ada di `record` dianggap tidak berubah. Angka negatif dianggap 0 (sama dengan saat dimuat).

Log: `INFO [CDC] UPDATE iec61850s: konfigurasi berubah, cek ulang baris DB lalu reload`, `INFO [CDC] UPDATE iec61850s: channel_number berubah, tanpa reconnect MMS`,
`DEBUG [CDC] UPDATE iec61850s diabaikan: hanya kolom status / non-konfigurasi yang berubah`,
`WARN [CDC] DELETE master_datapoint_iec61850s: id_rtu datapoint <uuid> tidak ditemukan, reload semua IED`. Tidak ada balasan Kafka.

Sampel asli dari cdc_listener (tabel lain, diabaikan FEP; `...` apa adanya dari sampel):

@@consume/CAPTURE_DATA_CHANGE__dnp3s.real@@

`iec61850s`, hanya kolom status berubah → diabaikan (konfigurasi sama dengan yang berjalan):

@@consume/CAPTURE_DATA_CHANGE__iec61850s.status_only@@

`iec61850_command_formulas` → reload IED pemilik datapoint:

@@consume/CAPTURE_DATA_CHANGE__iec61850_command_formulas@@

Bentuk lama (`data`), masih diterima:

@@consume/CAPTURE_DATA_CHANGE__master_datapoint_iec61850s.data@@

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

@@consume/MASTER_RTU_EVENTS__UPDATE_ID_RTU@@

### 2.6 `MANUAL_SET_EVENT`

Struct: `ManualSetEventMessage`. Handler: [manual_set.rs](../src/kafka/handlers/manual_set.rs).

| Field | Tipe JSON | Wajib | Arti |
|---|---|---|---|
| `event` | string | ya | `TOGGLE_MANUAL_SET` (tidak peka huruf besar/kecil) |
| `protocol` | string | ya | `IEC61850` |
| `tag` | string | ya | Tag datapoint |
| `manual_set` | boolean | ya | `true` = berhenti kirim ke `DATAPOINTS`, `false` = kirim lagi |
| `id_rtu` | integer | ya | IED di FEP ini; tidak berjalan / tidak dikenal → `FAILED` |
| `user` | string | tidak | Disalin ke respons |

Efek + balasan `MANUAL_SET_EVENT_RESPONSE` (bagian [3.6](#36-manual_set_event_response)). IED tidak berjalan (ada di
`iec61850s` tapi thread-nya tidak jalan) → `FAILED` `IED tidak berjalan`; `id_rtu` tidak ada di `iec61850s` →
`FAILED` `IED tidak dikenal` (pesan protocol lain sudah disaring sebelumnya). Event lain → `WARN [MANUAL_SET] event tidak
dikenal`, tanpa balasan.
Log: `INFO [MANUAL_SET] STOPPED tag=BAY1_ANIN1_2001 user=operator1`.

@@consume/MANUAL_SET_EVENT__TOGGLE_MANUAL_SET@@

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

@@consume/RTU_COMMAND_CONFIG_REQUEST__GET@@

---

## 3. Topic yang diproduksi

### 3.1 `RTU_DATAS` dan `DATAPOINTS`

Struct: `DataMessage` ([models.rs](../src/kafka/models.rs)), dibuat di
[message_builder.rs](../src/kafka/publisher/message_builder.rs). Key = `id_rtu`. Urutan field sama dengan FEP IEC 104;
tambahan IEC 61850 `source` dan `rcb` **selalu ada** dan diletakkan tepat sesudah `general_action` (field IEC 104 lain tidak
bergeser, dicek test `rtu_datas_sama_bentuk_dengan_sampel_iec104`).

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
| `group` | string | `master_data_objects.id_group` (UUID) untuk `initial` = CDC dan `protocol = 'IEC61850'`; `""` kalau tidak ada. Sama dengan FEP IEC 104 (lihat sampel di bagian 4) |
| `general_action` | integer / null | `master_general_actions.code` **apa adanya** untuk `name_action` = `action` (`protocol = 'IEC61850'`); `null` kalau tidak ada |
| `source` | string | **Tambahan IEC 61850.** `REPORT` = dari report RCB; `POLLING` = dari polling, termasuk polling penuh karena event GI dan siklus integrity polling |
| `rcb` | string / null | **Tambahan IEC 61850.** `rcb_reference` lengkap RCB yang **mengirim** report ini : nilai kolom `iec61850_report_controls.rcb_reference` **apa adanya** (`LD/LLN0.BR.<nama>` / `LD/LLN0.RP.<nama>`, bukan format MMS `LLN0$BR$...`), jadi bisa langsung di-join ke tabel itu; `null` untuk `POLLING` |
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

`general_action`: `master_general_actions.code` apa adanya dari DB, **kode internal GRITA, bukan nomor COT**. FEP tidak punya
konstanta sendiri; nilainya sepenuhnya bergantung isi tabel. Isi tabel di server (dipakai di semua contoh dokumen ini):

| `protocol` | `name_action` = `code` | Arti |
|---|---|---|
| `IEC104` | `SPONTANEOUS` = 0, `INTERROGATED_BY_STATION` = 1, `PERIODIC` = 2, `ACTIVATION_CON` = 3 | |
| `IEC61850` | `DATA_CHANGE` = 0, `QUALITY_CHANGE` = 0, `DATA_UPDATE` = 0 | 0 = spontan |
| `IEC61850` | `GENERAL_INTERROGATION` = 1 | 1 = GI |
| `IEC61850` | `INTEGRITY` = 2, `POLLING` = 2 | 2 = periodik |

Jadi kode IEC 61850 sejajar dengan IEC 104 (0 spontan, 1 GI, 2 periodik). Cek isi tabel di lingkungan masing-masing:

```text
SELECT protocol, name_action, code
FROM master_general_actions
WHERE is_deleted = FALSE AND protocol IN ('IEC61850', 'IEC104')
ORDER BY protocol, code, name_action;
```

`action` IEC 61850 yang perlu ada barisnya (`protocol = 'IEC61850'`): `GENERAL_INTERROGATION`, `INTEGRITY`, `DATA_CHANGE`,
`QUALITY_CHANGE`, `DATA_UPDATE`, `POLLING`. Action tanpa baris → `general_action: null`. Dimuat saat start dan saat CDC
`master_general_actions`.

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

`source` / `rcb`:

- `rcb` diambil dari konteks callback report (RCB yang menerima report), **bukan** ditebak dari dataset. Titik yang ada di
  dataset dua RCB → `rcb` = RCB yang benar-benar mengirim report tersebut.
- Beberapa anggota dataset untuk satu titik (`stVal` + `q` + `t`) tetap **satu** pesan dengan `rcb` yang sama.
- Mode `HYBRID`: titik ST lewat report (`REPORT` + `rcb`), titik MX lewat polling (`POLLING`, `rcb` null).
- Hanya ada dua jalur yang membuat pesan data titik: report dan polling. Manual set tidak membuat pesan sendiri (hanya
  menahan `DATAPOINTS` untuk tag itu); `DATAPOINTS` membawa `source` / `rcb` yang sama dengan `RTU_DATAS`-nya.

`DATA_CHANGE` (DPS dari BRCB, waktu dari `t`):

@@produce/RTU_DATAS__DATA_CHANGE@@

`QUALITY_CHANGE` (MV invalid):

@@produce/RTU_DATAS__QUALITY_CHANGE@@

`INTEGRITY`:

@@produce/RTU_DATAS__INTEGRITY@@

`GENERAL_INTERROGATION` (jam IED tidak sinkron):

@@produce/RTU_DATAS__GENERAL_INTERROGATION@@

`POLLING` (tanpa `t` → waktu server):

@@produce/RTU_DATAS__POLLING@@

Polling penuh karena event GI (mode POLLING):

@@produce/RTU_DATAS__GENERAL_INTERROGATION.polling@@

`DATAPOINTS` (tag terisi; nilai MV setelah formula):

@@produce/DATAPOINTS__DATA_CHANGE@@

@@produce/DATAPOINTS__INTEGRITY@@

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

@@produce/RTU_STATUS__connection@@

@@produce/RTU_STATUS__channel@@

@@produce/RTU_STATUS__system@@

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
| `terkoneksi MMS <ip>:<port> mode <REPORT\|POLLING\|HYBRID>, max PDU <n>[ (diminta <MMS_MAX_PDU_SIZE>)]` | INFO |
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
bersama baris data pertama di detik berikutnya dikirim baris ringkas `<timestamp> [INFO] <n> log data dilewati (batas
LOG_DATA_MAX_PER_SEC)`. Format ([traffic.rs](../src/iec61850/traffic.rs)):

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

@@produce/RTU_LOGGING__event_connect@@

@@produce/RTU_LOGGING__event_disconnect@@

@@produce/RTU_LOGGING__traffic_report@@

@@produce/RTU_LOGGING__traffic_gi_tx@@

@@produce/RTU_LOGGING__traffic_gi_rx@@

@@produce/RTU_LOGGING__traffic_poll@@

@@produce/RTU_LOGGING__traffic_dropped@@

@@produce/RTU_LOGGING__control_tx@@

@@produce/RTU_LOGGING__control_rx_positive@@

@@produce/RTU_LOGGING__control_rx_negative@@

@@produce/RTU_LOGGING__control_termination@@

@@produce/RTU_LOGGING__control_skip@@

@@produce/RTU_LOGGING__control_rejected@@

@@produce/RTU_LOGGING__control_rejected_position@@

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

@@produce/RTU_COMMAND__active@@

@@produce/RTU_COMMAND__active_cancel@@

Ditolak IED dengan AddCause 10 (`blocked-by-interlocking`):

@@produce/RTU_COMMAND__inactive_addcause@@

@@produce/RTU_COMMAND__inactive_not_control@@

@@produce/RTU_COMMAND__inactive_not_connected@@

Ditolak FEP sebelum dikirim karena posisi saklar tidak valid (`CONTROL_PRECHECK_POSITION=true`, kasus 00 + `INVALID|FAILURE`):

@@produce/RTU_COMMAND__inactive_precheck_position@@

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
| `error` | string | FAILED | `Datapoint not found`, `IED tidak berjalan`, `IED tidak dikenal` |
| `timestamp` | integer | selalu | ms epoch UTC |

@@produce/MANUAL_SET_EVENT_RESPONSE__success@@

@@produce/MANUAL_SET_EVENT_RESPONSE__failed@@

@@produce/MANUAL_SET_EVENT_RESPONSE__failed_ied@@

### 3.7 `IEC61850_BROWSE_RESPONSE`

Dibuat di [rtu_event/browse.rs](../src/kafka/handlers/rtu_event/browse.rs). Key = `key` payload (`"unknown"` kalau kosong).

Dipakai dua event: `BROWSE` (model dibaca dari IED, `source: "BROWSE"`) dan `SCL_IMPORT` (model dari file SCL di DB,
`source: "SCL"`, lihat [SCL_IMPORT](#scl_import)). `objects` / `rcbs` / `members` berformat sama.

**Skema stabil** (untuk consumer GRITA): semua field di bawah **selalu ada** dengan tipe yang sama, urutan field JSON tetap
`key, id_rtu, status, source, message, objects, rcbs, errors, part, total_parts` (dijaga test `urutan_field_json_tetap`).
`SCL_IMPORT` menambah field di antara `source` dan `message`: `file_name`, `id_scl_file`, `ied`, `config_revs` (SUCCESS),
`ied_names` (FAILED pilih IED); field yang tidak ada nilainya tidak dikirim. BROWSE tidak pernah mengirim field ini.

| Field | Tipe | Arti |
|---|---|---|
| `key` | string | `key` payload (`""` kalau tidak ada) |
| `id_rtu` | integer / null | `id_rtu` payload / hasil pencarian dari IP (`null` kalau tidak ketemu) |
| `status` | string | `SUCCESS` / `FAILED` |
| `source` | string | `BROWSE` / `SCL` |
| `file_name` | string | Hanya SCL: `iec61850_scl_files.file_name` (kalau sudah terbaca) |
| `id_scl_file` | string | Hanya SCL: `id_scl_file` event |
| `ied` | object | Hanya SCL SUCCESS: `{name, manufacturer, type, config_version, ip}` (atribut `<IED>`, IP dari `ConnectedAP`); null kalau tidak ada |
| `config_revs` | array | Hanya SCL SUCCESS: `[{ld, reference, value}]` (`LLN0.NamPlt.configRev` per LD), `[]` kalau tidak ada |
| `ied_names` | array | Hanya SCL FAILED `Pilih IED` / `IED <x> tidak ada di file`: `[{name, ip}]` |
| `message` | string | SUCCESS: `<n> objek, <m> RCB` (total semua bagian), ditambah `, <k> error` kalau `errors` tidak kosong; SCL diawali `Import SCL selesai: `. FAILED: penyebab |
| `objects` | array | Objek di bagian ini (`[]` kalau FAILED) |
| `objects[].object_reference` | string | `LD/LN.DO` atau `LD/LN.DO.SDO` (untuk `master_datapoint_iec61850s.object_reference`) |
| `objects[].fc` | string | `ST`, `MX`, `CO`, `SP` |
| `objects[].cdc` | string / null | CDC tebakan dari struktur DO: `SPS DPS INS SPC DPC INC ACT ACD BCR BSC APC MV CMV`; `null` = tidak dikenali (juga fallback GetNameList dengan `BROWSE_READ_VALUES=false`). ENS/ENC terbaca INS/INC |
| `objects[].control` | object | **Hanya ada untuk `fc = "CO"`**; objek FC lain tidak punya field ini (tidak dikirim, bukan `null`). Isi: `ctl_model` (string / null: teks kolom `ctl_model`, null = tidak dibaca), `sbo_timeout_ms` (integer / null: CF `sboTimeout`), `sbow` / `oper` / `cancel` (boolean: ada di struktur CO) |
| `rcbs` | array | RCB + dataset, **lengkap di bagian 1**, `[]` di bagian lain dan saat FAILED |
| `rcbs[].reference` | string | `LD/LN.BR.<nama>` (BRCB) / `LD/LN.RP.<nama>` (URCB), untuk `iec61850_report_controls.rcb_reference` |
| `rcbs[].buffered` | boolean | BRCB = `true` |
| `rcbs[].dataset` | string / null | `DatSet`, format ACSI `LD/LN.DataSet` (untuk `dataset_reference`) |
| `rcbs[].rpt_id` | string / null | `RptID` |
| `rcbs[].rpt_ena` | boolean / null | `RptEna` saat dibaca (`true` = sedang dipakai client lain / FEP). SCL: `null` (tidak diketahui) |
| `rcbs[].trg_ops` | string | `TrgOps`, format kolom `trg_ops` (`dchg,qchg,dupd,integrity,gi`) |
| `rcbs[].intg_pd_ms` | integer | `IntgPd` (ms) |
| `rcbs[].conf_rev`, `rcbs[].buf_tm_ms`, `rcbs[].opt_flds` | integer / integer / string | `ConfRev`, `BufTm` (ms), `OptFlds` yang aktif. BROWSE: selalu ada (dari IED). SCL: `confRev`, `bufTime`, atribut `OptFields` yang true; tidak dikirim kalau tidak ada di file. Teks `opt_flds` sama untuk keduanya: nama atribut SCL dipisah koma, urutan tetap `seqNum,timeStamp,dataSet,reasonCode,dataRef,bufOvfl,entryID,configRef,segmentation` (`""` = tidak ada yang aktif) |
| `rcbs[].members` | array | Anggota dataset `[{object_reference, fc}]`, bentuk sama dengan `objects[]` (`fc` = `""` kalau IED tidak menyertakannya) |
| `errors` | array | Error **nyata** `[{ref, error}]`, **lengkap di bagian 1**, `[]` di bagian lain. FAILED: `[{ref: "browse", error: <penyebab>}]` |
| `part` | integer | Nomor bagian, mulai 1 |
| `total_parts` | integer | Jumlah bagian |

Isi `errors` (selain itu tidak dilaporkan):

| `ref` | Kapan |
|---|---|
| `LD/LN[FC]` | `getVariableSpecification` gagal **dan** fallback GetNameList juga gagal (`getVariableSpecification: <err>; GetNameList: <err>`) |
| `LD/LN.DO.stVal[ST]` | Variabel ada di GetNameList tapi nilainya tidak bisa dibaca (tipe / CDC tidak bisa ditentukan) |
| `LD/LN.DO.ctlModel[CF]`, `LD/LN.DO.sboTimeout[CF]` | Atribut kontrol ada tapi tidak bisa dibaca (`sboTimeout` yang tidak ada bukan error) |
| `LD/LN.BR.<rcb>`, `LD/LN.RP.<rcb>`, `LD/LN (BR)`, `<dataset>` | RCB / daftar RCB / anggota dataset gagal dibaca |
| `<LD>` | Daftar LN satu LD gagal dibaca |
| `browse` | `BROWSE_TIMEOUT_MS` tercapai (hasil sebagian), atau penyebab FAILED |

**Bukan error**: LN+FC yang memang tidak ada. IED membalasnya `object-non-existent`, tapi sebagian IED (mis. Schneider Baseline:
`MainXSWI1[MX]`, `PMr_MMXU1[CO]`, `LLN0[MX]`) membalas `malformed-message`. Kalau GetNameList juga tidak punya variabel FC
itu, FC dianggap tidak ada dan hanya dicatat di log DEBUG FEP. Kalau GetNameList punya variabelnya, struktur diambil dari
GetNameList dan objeknya tetap masuk `objects` tanpa error.

Satu DO bisa muncul beberapa kali (satu per FC). JSON tiap bagian ≤ ~900 KB (di bawah `message.max.bytes` default 1 MB);
lebih → beberapa pesan dengan `key` dan `message` sama, `part` 1..`total_parts`. Gabungkan `objects` semua bagian; ambil
`rcbs` dan `errors` dari bagian 1.

SUCCESS tanpa RCB / error:

@@produce/IEC61850_BROWSE_RESPONSE__success@@

SUCCESS dengan info kontrol, RCB, dan satu error nyata (**bentuk contoh**, nama objek ilustrasi untuk IED seperti Schneider
Baseline):

@@produce/IEC61850_BROWSE_RESPONSE__success_with_errors@@

Multi-part (di sini dibagi dengan batas kecil supaya contoh pendek; FEP memakai ~900 KB). `rcbs` / `errors` di bagian 2 = `[]`:

@@produce/IEC61850_BROWSE_RESPONSE__multipart_1@@

@@produce/IEC61850_BROWSE_RESPONSE__multipart_2@@

FAILED:

@@produce/IEC61850_BROWSE_RESPONSE__failed@@

SCL_IMPORT SUCCESS (dipotong: 3 dari 9 objek, 1 dari 3 RCB fixture `tests/fixtures/scl/baseline_1ied.cid`):

@@produce/IEC61850_BROWSE_RESPONSE__scl_success@@

SCL_IMPORT FAILED (SCD dua IED tanpa `ied_name`):

@@produce/IEC61850_BROWSE_RESPONSE__scl_failed@@

---

## 4. Perbandingan dengan FEP IEC 104

Berdasarkan kode `iconics-fep-iec104` (`src/kafka/models.rs`, `src/kafka/handlers/rtu_event/command/*`,
`src/kafka/publisher/message_builder.rs`, `src/iec104/asdu/cot_mapper.rs`).

Sampel asli `RTU_DATAS` FEP IEC 104 (test `rtu_datas_sama_bentuk_dengan_sampel_iec104`: nama & tipe field sama dengan
`RTU_DATAS` FEP IEC 61850):

@@iec104/RTU_DATAS__iec104_real@@

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
| COMMAND `data.ip_address`, `data.port` | **wajib** di dalam `data`; tanpa itu `data` gagal dibaca lalu jatuh ke field datar | opsional; `data` salah tipe → ditolak `inactive` |
| COMMAND port default | 2404 | port konfigurasi IED (IED tidak dikenal: `port` payload, lalu 102) |
| COMMAND `type` | **wajib**, tipe ASDU (`C_SC_NA_1`, `C_DC_NA_1`, ...), menentukan perintah | opsional, hanya disalin ke respons; CDC dari mapping DB |
| COMMAND `value` | **integer** (`i32`) | angka (integer/float), string angka, boolean |
| COMMAND `action` | `SELECT` (lewat `url`), `SELECT_RTU`, `OPERATE_RTU` | `SELECT_RTU`, `OPERATE_RTU`, `CANCEL_RTU` |
| COMMAND `user` | tidak ada | `orIdent` (default `FEP-IEC61850`) |
| COMMAND `qualifier` | dikirim ke RTU (default 0) | tidak dikirim ke IED, dikembalikan di respons (default 0) |
| COMMAND pencarian RTU | dari `ip_address` | `id_rtu` dulu, baru `ip_address` |
| `RTU_DATAS.action` | COT: `SPONTANEOUS`, `PERIODIC`, `BACKGROUND_SCAN`, `INTERROGATED_BY_STATION`, `INTERROGATED_BY_GROUP_n`, ... | reason report: `DATA_CHANGE`, `QUALITY_CHANGE`, `DATA_UPDATE`, `INTEGRITY`, `GENERAL_INTERROGATION`, `POLLING` |
| `RTU_DATAS.type` / `asdu` / `ca` | nama tipe ASDU / type id / common address (sampel: `M_ME_TF_1` / `36` / `1`) | CDC / `0` / `0` (sampel: `DPS` / `0` / `0`) |
| `RTU_DATAS.general_action` | kode dari `master_general_actions` protocol `IEC104` (sampel: `0` untuk `SPONTANEOUS`) | kode dari `master_general_actions` protocol `IEC61850` |
| `RTU_DATAS.time_desc = RTU` | waktu RTU dikoreksi `TIMEZONE` | waktu `t` sudah UTC, tidak dikoreksi |
| `MASTER_RTU_EVENTS.id_station` | wajib | diabaikan |

**Perlu dikonfirmasi backend** (dibandingkan dengan sampel asli IEC 104 di atas):

1. `action`: IEC 104 `SPONTANEOUS` / `PERIODIC` / `INTERROGATED_BY_STATION` ..., IEC 61850 `DATA_CHANGE` / `INTEGRITY` /
   `GENERAL_INTERROGATION` .... **Apakah backend membaca `action` sebagai teks?** Kalau ya, backend harus mengenal nama
   IEC 61850 juga. `general_action` sudah sejajar (0 spontan, 1 GI, 2 periodik, tabel di 3.1), jadi konsumen yang memakai
   `general_action` tidak perlu perubahan.
2. `ca` / `asdu`: IEC 104 `1` / `36`, IEC 61850 selalu `0` / `0`. Apakah ada konsumen yang memakai `ca`/`asdu` sebagai kunci?
3. `type`: IEC 104 nama tipe ASDU (`M_ME_TF_1`), IEC 61850 CDC (`MV`, `DPS`, ...). Apakah tampilan / alarm memetakan `type`?

### 4.3 Payload COMMAND "aman untuk keduanya"

Bentuk yang terbaca kedua FEP: `ip_address` dan `port` di tingkat atas **dan** di dalam `data`, `value` integer,
`type` terisi, `key` terisi. Field `protocol` (dan `type`) tetap sesuai FEP tujuan: untuk 104 `protocol: "IEC104"` dan
`type` ASDU (`C_DC_NA_1`); untuk 61850 seperti di bawah. `id_rtu` dan `user` diabaikan FEP IEC 104.

@@consume/RTU_EVENTS__COMMAND.compat104@@

---

## 5. Catatan ketidaksesuaian

Ditemukan saat menulis dokumen ini (2026-11-15), lalu sebagian diperbaiki (2026-11-16). Semua butir sudah diperbaiki atau
ditandai sengaja (2026-11-16).

1. ~~CDC: `record` vs `data`~~ — **diperbaiki**: FEP membaca `record` (cadangan `data`), UPDATE kolom status `iec61850s` tidak lagi memicu reload, tabel lain diabaikan.
2. ~~README `group`~~ — **diperbaiki di README**: `group` = `master_data_objects.id_group` (UUID), sama dengan IEC 104.
3. ~~AddCause di `RTU_COMMAND` hanya kode~~ — **diperbaiki**: `addCause=10 (blocked-by-interlocking)`.
4. ~~COMMAND `data` gagal dibaca → diam-diam pakai field datar~~ — **diperbaiki**: ditolak `inactive` `field data tidak valid: ...`.
5. ~~Default port COMMAND tidak seragam~~ — **diperbaiki**: port konfigurasi IED (IED tidak dikenal: `port` payload, lalu 102).
6. ~~`qualifier` payload diabaikan~~ — **diperbaiki**: dikembalikan di respons (default 0).
7. ~~Tanpa balasan~~ — **diperbaiki / sengaja**: (a) COMMAND tidak valid **tanpa** `key` sengaja tidak dibalas (tidak bisa dicocokkan), cukup log + `RTU_LOGGING` WARN; (b) `MANUAL_SET_EVENT` untuk IED tidak berjalan / tidak dikenal → `FAILED` `IED tidak berjalan` / `IED tidak dikenal`. Sisa: event selain `TOGGLE_MANUAL_SET` tanpa balasan (sama dengan IEC 104).
8. ~~Cek protocol tidak berlaku untuk `MASTER_DATA_OBJECT_EVENT`~~ — **diperbaiki**: group di-update per baris; `protocol` bukan IEC61850 diabaikan, DELETE (hanya `id`) hanya menghapus `id` yang dikenal, field tidak terbaca → muat ulang semua + WARN. Contoh pesan masih bentuk contoh, menunggu sampel asli KaDeck.
9. ~~BROWSE tidak mencari `id_rtu` dari `ip_address`~~ — **diperbaiki**: sama dengan COMMAND.
10. ~~CREATE/ADD/UPDATE dengan `ip_address` saja hanya untuk IED yang berjalan~~ — **diperbaiki**: `id_rtu` dicari di `iec61850s` (`ip_address` + `port`, `is_deleted = false`); lebih dari satu baris → WARN, tidak dipilih. Backend sebaiknya selalu mengirim `id_rtu`.
11. ~~`MASTER_DATAPOINT_EVENT.ioa` harus integer~~ — **diperbaiki**: integer maupun string angka. (Tetap: `REMOVE` tidak dikenal di topic ini, hanya `DELETE`.)
12. ~~Event GI pada mode POLLING menghasilkan `INTEGRITY`~~ — **diperbaiki**: `GENERAL_INTERROGATION`.
13. ~~`CONTROL TX` ditulis walau tidak ada request ke IED~~ — **diperbaiki**: SELECT direct → `CONTROL SKIP`, value tidak valid / status-only → baris `[ERROR] ... tidak dikirim`.
14. ~~Baris ringkas `[INFO] <n> log data dilewati ...` tanpa timestamp~~ — **diperbaiki**: diawali timestamp UTC ISO-8601 seperti baris data lain.
15. ~~README fase 8: daftar `RTU_LOGGING` tidak lengkap~~ — **diperbaiki** di README.
16. ~~Path lama di README~~ — **diperbaiki** (`src/repository/`, `src/connection/manager.rs`, `src/kafka/handlers/`, `src/domain/command.rs`).
17. ~~Komentar `models.rs` "bentuk field sama dengan IEC 104"~~ — **diperbaiki**: komentar menjelaskan beda pesan consume.
18. ~~`GROUP_ID` tidak seragam~~ — **diperbaiki**: `IEC61850_FEP_GROUP` di kode, `.env.example`, dan configmap.
19. **Sengaja, tidak diubah**: BROWSE / COMMAND dengan `key` kosong → key Kafka `"unknown"`, field `key` di isi pesan `""`. Sama dengan FEP IEC 104 (`command/response.rs` memakai `"unknown"`).
20. ~~Pencarian `id_rtu` dari IP membuang `-`~~ — **diperbaiki**: dibandingkan apa adanya (trim + huruf kecil), hostname seperti `ied-sim-1` cocok.
21. ~~CDC `iec61850s` tidak membandingkan `channel_number`~~ — **diperbaiki**: perubahan `channel_number` memperbarui channel status ping tanpa reconnect MMS; pembanding CDC = salinan terakhir baris DB per IED.
22. ~~CDC menjalankan IED yang dihentikan lewat STATE false~~ — **diperbaiki**: CDC dibandingkan dengan salinan baris DB, STATE idempoten; alur `state_rtu` didokumentasikan di 2.1 STATE.
