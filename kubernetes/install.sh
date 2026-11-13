#!/bin/bash
# Build, push, dan deploy FEP IEC 61850 (pola sama dengan kubernetes/232/install.sh FEP IEC 104).
# Pemakaian: kubernetes/install.sh [tag]   (default: latest)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

REGISTRY="${REGISTRY:-10.14.152.233:8080}"
IMAGE_NAME="scada/iconics-fep-iec61850"
NAMESPACE="fep-iec61850"
TAG="${1:-latest}"
IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"

if [ ! -f kubernetes/secret.yaml ]; then
  echo "kubernetes/secret.yaml belum ada. Salin dari secret.example.yaml lalu isi." >&2
  exit 1
fi

echo "[1/4] Build ${IMAGE}"
DOCKER_BUILDKIT=1 docker build -t "${IMAGE}" .

echo "[2/4] Push ${IMAGE}"
docker push "${IMAGE}"

echo "[3/4] Apply manifest ke namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/secret.yaml
sed "s|image: .*|image: ${IMAGE}|" kubernetes/statefulset.yaml | kubectl apply -f -

echo "[4/4] Restart & tunggu rollout"
kubectl rollout restart statefulset/fep-iec61850 -n "${NAMESPACE}"
kubectl rollout status statefulset/fep-iec61850 -n "${NAMESPACE}" --timeout=300s

echo "Selesai. Log: kubectl logs -f -l app=fep-iec61850 -n ${NAMESPACE}"
