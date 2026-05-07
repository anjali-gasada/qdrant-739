#!/usr/bin/env bash
# Download SIFT-128-Euclidean from ann-benchmarks.
# 1M base vectors, 10K query vectors, 128 dim, L2 distance, ~501MB.

set -euo pipefail

DATA_DIR="${1:-./data}"
URL="http://ann-benchmarks.com/sift-128-euclidean.hdf5"
OUT="${DATA_DIR}/sift-128-euclidean.hdf5"
EXPECTED_MIN_BYTES=$((400 * 1024 * 1024))   # ~501MB; treat <400MB as a partial download

mkdir -p "${DATA_DIR}"

if [[ -f "${OUT}" ]]; then
  size=$(stat -c%s "${OUT}" 2>/dev/null || stat -f%z "${OUT}" 2>/dev/null || echo 0)
  if (( size > EXPECTED_MIN_BYTES )); then
    echo "[download] ${OUT} already present (${size} bytes), skipping."
    exit 0
  else
    echo "[download] ${OUT} looks truncated (${size} bytes), re-downloading."
    rm -f "${OUT}"
  fi
fi

echo "[download] fetching ${URL} -> ${OUT}"
if command -v curl >/dev/null 2>&1; then
  curl -fL --retry 3 --retry-delay 2 -o "${OUT}.part" "${URL}"
elif command -v wget >/dev/null 2>&1; then
  wget --tries=3 -O "${OUT}.part" "${URL}"
else
  echo "[download] error: neither curl nor wget is installed" >&2
  exit 1
fi
mv "${OUT}.part" "${OUT}"

size=$(stat -c%s "${OUT}" 2>/dev/null || stat -f%z "${OUT}" 2>/dev/null || echo 0)
echo "[download] done: ${OUT} (${size} bytes)"
