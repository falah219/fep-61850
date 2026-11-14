#!/usr/bin/env bash
# Uji urutan stub backend -> migrasi di PostgreSQL 16 KOSONG (container sementara, dihapus di akhir).
# Tidak menyentuh DB lain. Pemakaian: scripts/uji_migrasi.sh
set -euo pipefail
cd "$(dirname "$0")/.."

name="fep61850-uji-migrasi-$$"
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=uji -e POSTGRES_DB=uji postgres:16 >/dev/null
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT

until docker exec "$name" pg_isready -U postgres -d uji >/dev/null 2>&1; do sleep 1; done

run() {
    echo "== $1"
    docker exec -i "$name" psql -U postgres -d uji -v ON_ERROR_STOP=1 -q <"$1"
}

run test-env/sql/00_stub_backend.sql
run migrations/0001_iec61850.up.sql
run migrations/0002_iec61850_telco_status.sql
run migrations/seed_simulator.sql
run migrations/0001_iec61850.down.sql
run migrations/0001_iec61850.up.sql   # up lagi setelah down
echo "OK: stub -> 0001 -> 0002 -> seed -> down -> up berhasil di PostgreSQL 16"
