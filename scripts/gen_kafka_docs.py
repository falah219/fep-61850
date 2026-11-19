#!/usr/bin/env python3
"""Generator docs/KAFKA_PAYLOADS.md + docs/examples/**.json.

Contoh JSON didefinisikan di file ini; teks dokumen ada di docs/tools/KAFKA_PAYLOADS.tmpl.md dengan placeholder
@@<sub>/<NAMA>@@ (sub = consume | produce | iec104). Skrip menulis ulang SEMUA file docs/examples/<sub>/*.json lalu
merender template ke docs/KAFKA_PAYLOADS.md. Gagal kalau ada placeholder tanpa contoh atau contoh yang tidak dipakai.

Pemakaian (dari root repo, Python 3.9+ tanpa paket tambahan):
    python3 scripts/gen_kafka_docs.py
Setelah itu jalankan cargo test (kafka::payload_examples memastikan contoh cocok dengan kode).
"""
import json, os, sys, re
from datetime import datetime, timezone

repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
template = os.path.join(repo, "docs", "tools", "KAFKA_PAYLOADS.tmpl.md")

def ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)

T = ms("2026-11-15T03:04:05.123Z")
IP = "10.10.1.21"
G_DPS = "6f1c2a9e-3b7d-4c11-9a52-0d8e4b7f2a10"
G_MV = "a2b4c6d8-1e3f-4a5b-8c7d-9e0f1a2b3c4d"
G_SPS = "0c9b8a7f-6e5d-4c3b-2a19-08f7e6d5c4b3"

SCL_FILE = "0f8e2c1a-5b6d-4e7f-9a0b-1c2d3e4f5a6b"
SCL_SCD = "3a4b5c6d-7e8f-4a0b-9c1d-2e3f4a5b6c7d"

BRCB_ST = "BAY1LD0/LLN0.BR.brcbST01"
URCB_MX = "BAY1LD0/LLN0.RP.urcbMX01"


def data(**kw):
    base = {"ip_address": IP, "protocol": "IEC61850", "id_rtu": 9001, "action": "DATA_CHANGE", "tag": "",
            "value": 2, "address": 1001, "ca": 0, "asdu": 0, "type": "DPS", "quality": "GOOD", "flag": "GOOD",
            "group": G_DPS, "general_action": 0, "source": "REPORT", "rcb": BRCB_ST, "time": T, "time_desc": "RTU"}
    base.update(kw)
    return base

def log(line, t=T):
    return {"ip_address": IP, "protocol": "IEC61850", "id_rtu": 9001, "logging": line, "time": t}

def cmd(**kw):
    base = {"hostname": IP, "port": 102, "protocol": "IEC61850", "tag": "BAY1_CB_3005", "command_type": "DPC",
            "command_value": 2, "qualifier": 0, "event": "OPERATE", "status": "active",
            "msg": "OPERATE success RTU status is Active", "key": "cmd-20261115-0001", "time": T}
    base.update(kw)
    return base

def bo(r, fc, cdc):
    return {"object_reference": r, "fc": fc, "cdc": cdc}

BROWSE_ALL = [
    bo("simpleIOGenericIO/GGIO1.AnIn1", "MX", "MV"),
    bo("simpleIOGenericIO/GGIO1.Ind1", "ST", "SPS"),
    bo("simpleIOGenericIO/GGIO1.SPCSO1", "ST", "SPC"),
    bo("simpleIOGenericIO/GGIO1.SPCSO1", "CO", "SPC"),
]

def browse(key, id_rtu, message, objects, rcbs=(), errors=(), part=1, total=1):
    """IEC61850_BROWSE_RESPONSE, urutan field sama dengan FEP."""
    return {"key": key, "id_rtu": id_rtu, "status": "SUCCESS", "source": "BROWSE", "message": message, "objects": list(objects),
            "rcbs": list(rcbs), "errors": list(errors), "part": part, "total_parts": total}

consume = {
    "RTU_EVENTS__CREATE": {"event": "CREATE", "protocol": "IEC61850", "id_rtu": 9001},
    "RTU_EVENTS__UPDATE": {"event": "UPDATE", "protocol": "IEC61850", "ip_address": IP, "port": 102, "id_rtu": 9001,
                           "data": {"id_rtu": 9001, "name": "GI BAY 1"}},
    "RTU_EVENTS__ADD.by_ip": {"event": "ADD", "protocol": "IEC61850", "ip_address": IP},
    "RTU_EVENTS__DELETE": {"event": "DELETE", "protocol": "IEC61850", "id_rtu": 9001},
    "RTU_EVENTS__REMOVE.data": {"event": "REMOVE", "protocol": "IEC61850", "data": {"id_rtu": 9001}},
    "RTU_EVENTS__STATE.off": {"event": "STATE", "protocol": "IEC61850", "ip_address": IP, "id_rtu": 9001, "state_rtu": False},
    "RTU_EVENTS__STATE.on": {"event": "STATE", "protocol": "IEC61850", "id_rtu": 9001, "state_rtu": True},
    "RTU_EVENTS__RESET": {"event": "RESET", "protocol": "IEC61850", "id_rtu": 9001},
    "RTU_EVENTS__GI": {"event": "GI", "protocol": "IEC61850", "ip_address": IP, "id_rtu": 9001},
    "RTU_EVENTS__GI.ignored_iec104": {"event": "GI", "protocol": "IEC104", "ip_address": "10.10.2.5", "port": 2404},
    "RTU_EVENTS__CHANNEL": {"event": "CHANNEL", "protocol": "IEC61850", "ip_address": IP, "id_rtu": 9001,
                            "data": {"channel": 1}},
    "RTU_EVENTS__COMMAND.min": {"event": "COMMAND", "protocol": "IEC61850",
                                "data": {"action": "OPERATE_RTU", "id_rtu": 9001, "ioa": 3001, "value": 1}},
    "RTU_EVENTS__COMMAND.full": {"event": "COMMAND", "protocol": "IEC61850", "ip_address": IP,
                                 "data": {"action": "SELECT_RTU", "tag": "BAY1_CB_3005", "type": "DPC", "value": 2,
                                          "ioa": 3005, "id_rtu": 9001, "key": "cmd-20261115-0001", "user": "operator1",
                                          "ip_address": IP, "port": 102}},
    "RTU_EVENTS__COMMAND.flat": {"event": "COMMAND", "protocol": "IEC61850", "ip_address": IP, "port": 102,
                                 "id_rtu": 9001, "action": "OPERATE_RTU", "tag": "BAY1_CB_3005", "type": "DPC",
                                 "value": "2", "key": "cmd-20261115-0002", "user": "operator1"},
    "RTU_EVENTS__COMMAND.cancel": {"event": "COMMAND", "protocol": "IEC61850", "ip_address": IP,
                                   "data": {"action": "CANCEL_RTU", "ioa": 3005, "id_rtu": 9001,
                                            "key": "cmd-20261115-0003", "user": "operator1"}},
    "RTU_EVENTS__COMMAND.apc": {"event": "COMMAND", "protocol": "IEC61850",
                                "data": {"action": "OPERATE_RTU", "ioa": 3010, "type": "APC", "value": 12.5,
                                         "id_rtu": 9001, "key": "cmd-20261115-0004"}},
    "RTU_EVENTS__COMMAND.bsc": {"event": "COMMAND", "protocol": "IEC61850",
                                "data": {"action": "OPERATE_RTU", "ioa": 3011, "type": "BSC", "value": 2,
                                         "id_rtu": 9001, "key": "cmd-20261115-0006"}},
    "RTU_EVENTS__COMMAND.compat104": {"event": "COMMAND", "protocol": "IEC61850", "ip_address": IP, "port": 102,
                                      "data": {"action": "OPERATE_RTU", "ip_address": IP, "port": 102, "id_rtu": 9001,
                                               "ioa": 3005, "tag": "BAY1_CB_3005", "type": "DPC", "value": 2,
                                               "key": "cmd-20261115-0005", "user": "operator1"}},
    "RTU_EVENTS__BROWSE": {"event": "BROWSE", "protocol": "IEC61850", "id_rtu": 9001, "key": "browse-9001-01"},
    "RTU_EVENTS__BROWSE.data": {"event": "BROWSE", "protocol": "IEC61850",
                                "data": {"id_rtu": 9001, "key": "browse-9001-02"}},
    "RTU_EVENTS__SCL_IMPORT": {"event": "SCL_IMPORT", "protocol": "IEC61850", "id_rtu": 9399, "key": "scl-9399-" + SCL_FILE,
                               "id_scl_file": SCL_FILE, "ied_name": None},
    "RTU_EVENTS__SCL_IMPORT.ied_name": {"event": "SCL_IMPORT", "protocol": "IEC61850", "id_rtu": 9399,
                                        "key": "scl-9399-7d3e9b20-4c1a-4f8e-b6d2-5a9c0e1f2b3d", "id_scl_file": SCL_SCD,
                                        "ied_name": "IED2"},
    "MASTER_DATAPOINT_EVENT__UPDATE": {"id_rtu": 9001, "protocol": "IEC61850", "ioa": 2001, "event": "UPDATE",
                                       "data": {"tag": "BAY1_ANIN1_2001", "description": "Arus fasa R"}},
    "MASTER_DATAPOINT_EVENT__UPDATE.ioa_string": {"id_rtu": 9001, "protocol": "IEC61850", "ioa": "2001", "event": "UPDATE",
                                                  "data": {"tag": "BAY1_ANIN1_2001"}},
    "MASTER_DATAPOINT_EVENT__DELETE": {"id_rtu": 9001, "protocol": "IEC61850", "ioa": 2001, "event": "DELETE"},
    # Bentuk CONTOH (bukan sampel asli KaDeck): CREATE/UPDATE = baris lengkap master_data_objects, DELETE = id saja.
    "MASTER_DATA_OBJECT_EVENT__CREATE": {"event": "CREATE", "protocol": "IEC61850",
                                         "data": {"id": "4e5f6a7b-8c9d-4e0f-a1b2-c3d4e5f6a7b8", "protocol": "IEC61850", "code": 9,
                                                  "initial": "MV", "information": "Measured value", "id_group": G_MV,
                                                  "is_deleted": False}},
    "MASTER_DATA_OBJECT_EVENT__UPDATE.iec104": {"event": "UPDATE", "protocol": "IEC104",
                                                "data": {"id": "5f6a7b8c-9d0e-4f1a-b2c3-d4e5f6a7b8c9", "protocol": "IEC104",
                                                         "code": 13, "initial": "M_ME_NC_1", "information": "Measured value, float",
                                                         "id_group": G_MV, "is_deleted": False}},
    "MASTER_DATA_OBJECT_EVENT__DELETE": {"event": "DELETE", "data": {"id": "4e5f6a7b-8c9d-4e0f-a1b2-c3d4e5f6a7b8"}},
    "CAPTURE_DATA_CHANGE__iec61850s.status_only": {
        "table": "iec61850s", "schema": "public", "operation": "UPDATE",
        "record": {"id": "5d0f3c1a-7b2e-4f60-9a11-2c3d4e5f6a7b", "id_rtu": 9001, "name": "GI BAY 1",
                   "ip_address": IP, "port": 102, "local_ip_address": "0.0.0.0", "acquisition_mode": "REPORT",
                   "poll_interval_ms": 1000, "general_interrogation_interval": 0, "connect_timeout_ms": 1000,
                   "request_timeout_ms": 1000, "reconnect_time_ms": 1000, "reconnect_retry_count": 0,
                   "state_rtu": True, "channel_number": 1, "ap_title": "1.1.999.1", "ae_qualifier": 12,
                   "p_selector": "00 00 00 01", "s_selector": "00 01", "t_selector": "00 01",
                   "status_rtu": "red", "rtu_online_at": "1794785701262", "rtu_offline_at": "1794791384243",
                   "status_channel": False, "is_deleted": False,
                   "created_at": "2026-11-01T02:00:00.000+00:00", "updated_at": "2026-11-16T01:09:44.737+00:00"},
        "timestamp": "2026-11-16T01:09:44.760Z", "source": "cdc_listener"},
    "CAPTURE_DATA_CHANGE__dnp3s.real": {
        "table": "dnp3s", "schema": "public", "operation": "UPDATE",
        "record": {"id": "30a0f2b7-...", "id_rtu": 83020012, "protocol": "DNP3", "tcp_server_ip": "192.168.212.12",
                   "tcp_port": 20000, "state_rtu": True, "rtu_online_at": "1794785701262",
                   "rtu_offline_at": "1794791384243", "status_rtu": "red", "status_channel": False,
                   "channel_number": 104, "is_deleted": False, "updated_at": "2026-11-16T01:09:44.737+00:00", "...": "..."},
        "timestamp": "2026-11-16T01:09:44.760Z", "source": "cdc_listener"},
    "CAPTURE_DATA_CHANGE__iec61850_command_formulas": {
        "table": "iec61850_command_formulas", "schema": "public", "operation": "UPDATE",
        "record": {"id": "2b3c4d5e-6f70-4a81-9b2c-3d4e5f6a7b8c", "id_master_datapoint": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
                   "type": "DPC", "ctl_model": "sbo-with-enhanced-security", "value": None, "interlock_check": True,
                   "synchro_check": False, "is_deleted": False,
                   "created_at": "2026-11-01T02:00:00.000+00:00", "updated_at": "2026-11-16T01:10:00.000+00:00"},
        "timestamp": "2026-11-16T01:10:00.020Z", "source": "cdc_listener"},
    "CAPTURE_DATA_CHANGE__master_datapoint_iec61850s.data": {
        "table": "master_datapoint_iec61850s", "schema": "public", "operation": "UPDATE",
        "data": {"id": "9e8d7c6b-5a49-4382-a1b0-c9d8e7f6a5b4", "id_master_datapoint": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
                 "object_reference": "simpleIOGenericIO/GGIO1.AnIn1", "functional_constraint": "MX"}},
    "MASTER_RTU_EVENTS__UPDATE_ID_RTU": {"id_rtu": 9001, "id_station": "7c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f",
                                         "protocol": "IEC61850", "event": "UPDATE_ID_RTU",
                                         "new_data": {"id_rtu": 9011}},
    "MANUAL_SET_EVENT__TOGGLE_MANUAL_SET": {"event": "TOGGLE_MANUAL_SET", "protocol": "IEC61850",
                                            "tag": "BAY1_ANIN1_2001", "manual_set": True, "id_rtu": 9001,
                                            "user": "operator1"},
    "RTU_COMMAND_CONFIG_REQUEST__GET": {"action": "GET", "protocol": "IEC61850", "ip_address": IP, "ioa": 3005,
                                        "value": 1},
}

produce = {
    "RTU_DATAS__DATA_CHANGE": data(),
    "RTU_DATAS__QUALITY_CHANGE": data(action="QUALITY_CHANGE", value=0.0, address=2001, type="MV", quality="INVALID",
                                      flag="INVALID|FAILURE", group=G_MV, general_action=0, rcb=URCB_MX),
    "RTU_DATAS__INTEGRITY": data(action="INTEGRITY", value=231.4, address=2001, type="MV", group=G_MV, general_action=2,
                                 rcb=URCB_MX),
    "RTU_DATAS__GENERAL_INTERROGATION": data(action="GENERAL_INTERROGATION", value=1, address=1002, type="SPS",
                                             group=G_SPS, general_action=1, flag="CLOCK_NOT_SYNC"),
    "RTU_DATAS__POLLING": data(ip_address="10.10.1.22", id_rtu=9002, action="POLLING", value=49.98, address=2003,
                               type="MV", group=G_MV, general_action=2, source="POLLING", rcb=None, time=T + 877,
                               time_desc="SERVER"),
    # Polling penuh karena event GI (mode POLLING / HYBRID MX): source POLLING, rcb null
    "RTU_DATAS__GENERAL_INTERROGATION.polling": data(ip_address="10.10.1.22", id_rtu=9002, action="GENERAL_INTERROGATION",
                                                     value=50.01, address=2003, type="MV", group=G_MV, general_action=1,
                                                     source="POLLING", rcb=None, time=T + 1877, time_desc="SERVER"),
    "DATAPOINTS__DATA_CHANGE": data(tag="BAY1_CB_1001"),
    "DATAPOINTS__INTEGRITY": data(action="INTEGRITY", tag="BAY1_ANIN1_2001", value=2.314, address=2001, type="MV",
                                  group=G_MV, general_action=2, rcb=URCB_MX),
    "RTU_STATUS__connection": {"ip_address": IP, "id_rtu": 9001, "protocol": "IEC61850", "status": 1, "time": T},
    "RTU_STATUS__channel": {"ip_address": IP, "id_rtu": 9001, "protocol": "IEC61850", "channel": 1, "status": 0, "time": T},
    "RTU_STATUS__system": {"ip_address": "system", "id_rtu": 0, "protocol": "IEC61850", "status": 1, "time": T},
    "RTU_LOGGING__event_connect": log("[INFO] terkoneksi MMS 10.10.1.21:102 mode REPORT, max PDU 65000"),
    "RTU_LOGGING__event_disconnect": log("[WARN] koneksi MMS terputus"),
    "RTU_LOGGING__traffic_report": log("2026-11-15T03:04:05.123456Z REPORT RX - RCB: brcbST01, Dataset: dsStatus, "
                                       "Reason: dchg, Entries: 2, SeqNum: 15, EntryID: 0a00000000000000"),
    "RTU_LOGGING__traffic_gi_tx": log("2026-11-15T03:04:05.123456Z GI TX - RCB: brcbST01"),
    "RTU_LOGGING__traffic_gi_rx": log("2026-11-15T03:04:05.223456Z GI RX - RCB: brcbST01, Entries: 12"),
    "RTU_LOGGING__traffic_poll": log("2026-11-15T03:04:06.000000Z POLL RX - Group: simpleIOGenericIO/GGIO1[MX], Elements: 4"),
    "RTU_LOGGING__traffic_dropped": log("2026-11-15T03:04:06.000000Z [INFO] 12 log data dilewati (batas LOG_DATA_MAX_PER_SEC)"),
    "RTU_LOGGING__control_tx": log("2026-11-15T03:04:07.000000Z CONTROL TX - SELECT BAY1LD0/CSWI1.Pos ctlVal=true orIdent=operator1"),
    "RTU_LOGGING__control_rx_positive": log("2026-11-15T03:04:07.050000Z CONTROL RX - SELECT BAY1LD0/CSWI1.Pos positive"),
    "RTU_LOGGING__control_rx_negative": log("2026-11-15T03:04:08.000000Z CONTROL RX - OPERATE BAY1LD0/CSWI1.Pos negative AddCause=blocked-by-interlocking"),
    "RTU_LOGGING__control_termination": log("2026-11-15T03:04:09.000000Z CONTROL RX - CommandTermination BAY1LD0/CSWI1.Pos positive"),
    "RTU_LOGGING__control_rejected_position": log("[ERROR] CONTROL SELECT BaselineLD0/XSWI1.Pos tidak dikirim: posisi tidak valid di IED (stVal=00 (intermediate), q=INVALID|FAILURE), SELECT tidak dikirim"),
    "RTU_LOGGING__control_skip": log("2026-11-15T03:04:07.000000Z CONTROL SKIP - SELECT BAY1LD0/CSWI1.Pos (direct, tidak dikirim)"),
    "RTU_LOGGING__control_rejected": log("[ERROR] CONTROL OPERATE BAY1LD0/CSWI1.Pos tidak dikirim: value DPC harus 1 (OPEN) atau 2 (CLOSE), bukan 0"),
    "RTU_COMMAND__active": cmd(),
    "RTU_COMMAND__active_cancel": cmd(tag="", command_type="", command_value=None, event="CANCEL",
                                      msg="CANCEL success RTU status is Active", key="cmd-20261115-0003"),
    "RTU_COMMAND__inactive_addcause": cmd(status="inactive",
                                          msg="OPERATE failed: operate gagal: ditolak IED (LastApplError error=1, addCause=10 (blocked-by-interlocking), ctlNum=5) - RTU status is Inactive"),
    "RTU_COMMAND__inactive_precheck_position": cmd(hostname="192.168.212.14", event="SELECT", status="inactive", key="cmd-20261117-0001",
                                                   msg="SELECT failed: posisi tidak valid di IED (stVal=00 (intermediate), q=INVALID|FAILURE), SELECT tidak dikirim - RTU status is Inactive"),
    "RTU_COMMAND__inactive_not_control": cmd(tag="", command_type="", command_value=1, event="SELECT", status="inactive",
                                             msg="SELECT failed: IOA 1001 bukan titik kontrol (FC CO) di RTU ini - RTU status is Inactive",
                                             key="cmd-20261115-0007"),
    "RTU_COMMAND__inactive_not_connected": cmd(status="inactive",
                                               msg="OPERATE failed: IED tidak terkoneksi - RTU status is Inactive"),
    "MANUAL_SET_EVENT_RESPONSE__success": {"status": "SUCCESS", "protocol": "IEC61850", "tag": "BAY1_ANIN1_2001",
                                           "manual_set": True, "user": "operator1", "timestamp": T},
    "MANUAL_SET_EVENT_RESPONSE__failed": {"status": "FAILED", "protocol": "IEC61850", "tag": "TAG_TIDAK_ADA",
                                          "error": "Datapoint not found", "timestamp": T},
    "MANUAL_SET_EVENT_RESPONSE__failed_ied": {"status": "FAILED", "protocol": "IEC61850", "tag": "BAY1_ANIN1_2001",
                                              "error": "IED tidak berjalan", "timestamp": T},
    "IEC61850_BROWSE_RESPONSE__success": browse("browse-9001-01", 9001, "4 objek, 0 RCB", BROWSE_ALL),
    "IEC61850_BROWSE_RESPONSE__multipart_1": browse("browse-9001-02", 9001, "4 objek, 0 RCB", BROWSE_ALL[:2], part=1, total=2),
    "IEC61850_BROWSE_RESPONSE__multipart_2": browse("browse-9001-02", 9001, "4 objek, 0 RCB", BROWSE_ALL[2:], part=2, total=2),
    # Bentuk CONTOH (nama objek ilustrasi). Error nyata saja: RCB yang tidak bisa dibaca. LN+FC yang tidak ada
    # (IED membalas malformed-message, GetNameList juga tidak punya FC itu) TIDAK masuk errors.
    "IEC61850_BROWSE_RESPONSE__success_with_errors": browse(
        "browse-9302-01", 9302, "3 objek, 1 RCB, 1 error",
        [
            bo("BaselineLD0/XSWI1.OpCnt", "ST", "INS"),
            bo("BaselineLD0/XSWI1.Pos", "ST", "DPC"),
            dict(bo("BaselineLD0/XSWI1.Pos", "CO", "DPC"),
                 control={"ctl_model": "sbo-with-enhanced-security", "sbo_timeout_ms": 30000,
                          "sbow": True, "oper": True, "cancel": True}),
        ],
        rcbs=[{"reference": "BaselineLD0/LLN0.BR.brcbST01", "buffered": True, "dataset": "BaselineLD0/LLN0.dsStatus",
               "rpt_id": "BaselineLD0/LLN0$BR$brcbST01", "rpt_ena": False, "trg_ops": "dchg,qchg,gi", "intg_pd_ms": 0,
               "members": [{"object_reference": "BaselineLD0/XSWI1.Pos.stVal", "fc": "ST"},
                           {"object_reference": "BaselineLD0/XSWI1.Pos.q", "fc": "ST"}]}],
        errors=[{"ref": "BaselineLD0/LLN0.BR.brcbMX01", "error": "timeout"}]),
    "IEC61850_BROWSE_RESPONSE__failed": {"key": "browse-9002-01", "id_rtu": 9002, "status": "FAILED", "source": "BROWSE",
                                         "message": "IED tidak terkoneksi", "objects": [], "rcbs": [],
                                         "errors": [{"ref": "browse", "error": "IED tidak terkoneksi"}],
                                         "part": 1, "total_parts": 1},
    # SCL_IMPORT: format objects/rcbs sama dengan BROWSE (nama dari fixture tests/fixtures/scl/baseline_1ied.cid, dipotong)
    "IEC61850_BROWSE_RESPONSE__scl_success": {
        "key": "scl-9399-" + SCL_FILE, "id_rtu": 9399, "status": "SUCCESS", "source": "SCL",
        "file_name": "baseline_1ied.cid", "id_scl_file": SCL_FILE,
        "ied": {"name": "Baseline", "manufacturer": "Schneider Electric", "type": "Baseline", "config_version": "1.0",
                "ip": "10.10.1.21"},
        "config_revs": [{"ld": "BaselineLD0", "reference": "BaselineLD0/LLN0.NamPlt.configRev", "value": "Rev 2026-11"}],
        "message": "Import SCL selesai: 3 objek, 1 RCB",
        "objects": [
            bo("BaselineLD0/XSWI1.OpCnt", "ST", "INS"),
            bo("BaselineLD0/XSWI1.Pos", "ST", "DPC"),
            dict(bo("BaselineLD0/XSWI1.Pos", "CO", "DPC"),
                 control={"ctl_model": "sbo-with-enhanced-security", "sbo_timeout_ms": 30000,
                          "sbow": True, "oper": True, "cancel": True}),
        ],
        "rcbs": [{"reference": "BaselineLD0/LLN0.BR.brcbST01", "buffered": True, "dataset": "BaselineLD0/LLN0.dsStatus",
                  "rpt_id": None, "rpt_ena": None, "trg_ops": "dchg,qchg,gi", "intg_pd_ms": 0, "conf_rev": 1,
                  "buf_tm_ms": 100, "opt_flds": "seqNum,timeStamp,dataSet,reasonCode,entryID,configRef",
                  "members": [{"object_reference": "BaselineLD0/XSWI1.Pos.stVal", "fc": "ST"},
                              {"object_reference": "BaselineLD0/XSWI1.Pos.q", "fc": "ST"}]}],
        "errors": [], "part": 1, "total_parts": 1},
    "IEC61850_BROWSE_RESPONSE__scl_failed": {
        "key": "scl-9399-9e1f2a3b-0c4d-4e5f-8a6b-7c8d9e0f1a2b", "id_rtu": 9399, "status": "FAILED", "source": "SCL",
        "file_name": "station_2ied.scd", "id_scl_file": SCL_SCD,
        "ied_names": [{"name": "IED1", "ip": "10.0.0.1"}, {"name": "IED2", "ip": "10.0.0.2"}],
        "message": "Pilih IED", "objects": [], "rcbs": [], "errors": [{"ref": "scl", "error": "Pilih IED"}],
        "part": 1, "total_parts": 1},
}

# Simulasi split_parts (browse.rs) untuk batas test 160 byte
def split(objs, limit):
    parts, start, size = [], 0, 2
    for i, o in enumerate(objs):
        n = len(json.dumps(o, separators=(",", ":"))) + 1
        if size + n > limit and i > start:
            parts.append(objs[start:i]); start, size = i, 2
        size += n
    parts.append(objs[start:])
    return parts
p = split(BROWSE_ALL, 160)
assert [len(x) for x in p] == [2, 2], [len(x) for x in p]

iec104 = {
    "RTU_DATAS__iec104_real": {"ip_address": "192.168.212.11", "protocol": "IEC104", "id_rtu": 83020254,
                               "action": "SPONTANEOUS", "tag": "", "value": 218.3000030517578, "address": 5, "ca": 1,
                               "asdu": 36, "type": "M_ME_TF_1", "quality": "GOOD", "flag": "GOOD",
                               "group": "7fc706fc-1c7a-423c-9a9c-aa627f653611", "general_action": 0,
                               "time": 1794796746325, "time_desc": "RTU"},
}

rendered = {}
for sub, exs in (("consume", consume), ("produce", produce), ("iec104", iec104)):
    d = os.path.join(repo, "docs", "examples", sub)
    os.makedirs(d, exist_ok=True)
    for f in os.listdir(d):
        os.remove(os.path.join(d, f))
    for name, v in exs.items():
        text = json.dumps(v, indent=2, ensure_ascii=False) + "\n"
        with open(os.path.join(d, name + ".json"), "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
        rendered[f"{sub}/{name}"] = f"`docs/examples/{sub}/{name}.json`\n\n```json\n{text}```"

md = open(template, encoding="utf-8").read()
used = set()
def sub_fn(m):
    used.add(m.group(1))
    if m.group(1) not in rendered:
        raise SystemExit(f"placeholder tanpa contoh: {m.group(1)}")
    return rendered[m.group(1)]
md = re.sub(r"@@([a-z0-9]+/[A-Za-z0-9_.]+)@@", sub_fn, md)
missing = set(rendered) - used
if missing:
    raise SystemExit(f"contoh tidak dipakai di md: {sorted(missing)}")
with open(os.path.join(repo, "docs", "KAFKA_PAYLOADS.md"), "w", encoding="utf-8", newline="\n") as fh:
    fh.write(md)
print(f"docs/KAFKA_PAYLOADS.md + {len(consume)} consume, {len(produce)} produce, {len(iec104)} iec104 contoh")
