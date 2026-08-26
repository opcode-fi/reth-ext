#!/bin/bash
# Create the host data dirs, the JWT secret shared by reth and lighthouse, and
# the deploy/.env that docker compose reads DATA_DIR from.
set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-${HOME}/reth-ext-data}"

mkdir -p "${DATA_DIR}/reth" "${DATA_DIR}/lighthouse" "${DATA_DIR}/jwt"

JWT_FILE="${DATA_DIR}/jwt/jwt.hex"
if [ ! -f "${JWT_FILE}" ]; then
  openssl rand -hex 32 > "${JWT_FILE}"
  chmod 600 "${JWT_FILE}"
  echo "JWT secret created at ${JWT_FILE}"
else
  echo "JWT secret already exists at ${JWT_FILE}"
fi

if [ ! -f "${HERE}/.env" ]; then
  echo "DATA_DIR=${DATA_DIR}" > "${HERE}/.env"
  echo "Wrote ${HERE}/.env"
else
  echo "${HERE}/.env already exists, leaving it alone"
fi

cat <<MSG

Data directory ready: ${DATA_DIR}
  reth/        execution client database (pruned config in deploy/reth.toml)
  lighthouse/  consensus client database
  jwt/         shared engine-API secret

Start the node:
  docker compose -f deploy/docker-compose.yml up -d --build
MSG
