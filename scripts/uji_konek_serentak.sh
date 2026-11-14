#!/bin/bash
# Uji koneksi serentak: start FEP berulang kali, lalu pastikan tidak ada IED yang "terkoneksi"
# tanpa RCB aktif dalam 60 detik (temuan di docs/CATATAN_TERBUKA.md).
#
# Jalankan DI container dev (butuh Postgres + Kafka dev di .env, seed migrations/seed_sim20.sql,
# dan simulator dari docker-compose.sim20.yml):
#   scripts/uji_konek_serentak.sh [jumlah_run=10] [detik_per_run=90]
set -euo pipefail

RUNS="${1:-10}"
SECS="${2:-90}"
WINDOW=60
OUT="logs/uji_konek_serentak"
mkdir -p "$OUT"

cargo build --release --bin fep-iec61850

fail=0
for run in $(seq 1 "$RUNS"); do
  log="$OUT/run-$run.log"
  echo "== run $run/$RUNS (${SECS}s) -> $log"
  NO_COLOR=1 RUST_LOG=info ./target/release/fep-iec61850 >"$log" 2>&1 &
  pid=$!
  sleep "$SECS"
  kill -INT "$pid" 2>/dev/null || true
  wait "$pid" || true

  # Per id_rtu: waktu konek terakhir yang belum diikuti "RCB ... aktif". Putus / gagal = reset.
  # IED yang konek < WINDOW detik sebelum log berakhir tidak dinilai (belum cukup waktu).
  if ! awk -v window="$WINDOW" '
    # Console logger (src/logger.rs) memakai jam lokal "HH:MM:SS.mmm" di kolom pertama.
    function secs(ts,   h, m, s) { h = substr(ts, 1, 2); m = substr(ts, 4, 2); s = substr(ts, 7, 6); return h * 3600 + m * 60 + s }
    {
      gsub(/\x1b\[[0-9;]*m/, "")
      # Mulai shutdown: log sesudahnya (semua thread dihentikan) tidak dinilai.
      if ($0 ~ /berhenti\.\.\./) { last = secs($1); exit }
      if (!match($0, /id_rtu=[0-9]+/)) next
      id = substr($0, RSTART + 7, RLENGTH - 7)
      t = secs($1); last = t
      if ($0 ~ /terkoneksi MMS/) { conn[id] = t; ok[id] = 0; seen[id] = 1 }
      else if ($0 ~ /RCB .* aktif/) { ok[id] = 1 }
      else if ($0 ~ /koneksi ke IED terputus|sesi IED gagal|thread IED dihentikan/) { delete conn[id] }
    }
    END {
      bad = 0
      for (id in conn) if (!ok[id] && last - conn[id] >= window) { print "  GAGAL: id_rtu=" id " terkoneksi " int(last - conn[id]) "s tanpa RCB aktif"; bad = 1 }
      n = 0; for (id in seen) n++
      print "  IED pernah terkoneksi: " n
      exit bad
    }' "$log"; then
    fail=1
  fi
done

if [ "$fail" -eq 0 ]; then echo "LULUS: semua run OK"; else echo "GAGAL: lihat log di $OUT"; exit 1; fi
