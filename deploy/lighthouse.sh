#!/bin/bash
set -e

if [ -f "/.dockerenv" ] || [ -d "/root/.lighthouse" ]; then
  DATADIR="/root/.lighthouse"
  JWT="/jwt/jwt.hex"
  EXEC_ENDPOINT="http://reth:8551"
  BIND="0.0.0.0"
else
  DATA_DIR="${DATA_DIR:-${HOME}/reth-ext-data}"
  DATADIR="${DATA_DIR}/lighthouse"
  JWT="${DATA_DIR}/jwt/jwt.hex"
  EXEC_ENDPOINT="http://127.0.0.1:8551"
  BIND="127.0.0.1"
fi

exec lighthouse bn \
  --network=mainnet \
  --datadir="$DATADIR" \
  --execution-endpoint="$EXEC_ENDPOINT" \
  --execution-jwt="$JWT" \
  --checkpoint-sync-url=https://mainnet.checkpoint.sigp.io \
  --http \
  --http-address="$BIND" \
  --http-port=5052 \
  --http-allow-origin=* \
  --port=9000 \
  --quic-port=9002
