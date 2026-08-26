#!/bin/bash
set -e

# Inside the container reth's datadir and the shared JWT are bind-mounted at fixed
# paths and we bind on 0.0.0.0; run natively (no /.dockerenv) and everything moves
# under $DATA_DIR and binds loopback-only.
if [ -f "/.dockerenv" ] || [ -d "/root/.local/share/reth" ]; then
  DATADIR="/root/.local/share/reth"
  JWT="/jwt/jwt.hex"
  BIND="0.0.0.0"
else
  DATA_DIR="${DATA_DIR:-${HOME}/reth-ext-data}"
  DATADIR="${DATA_DIR}/reth"
  JWT="${DATA_DIR}/jwt/jwt.hex"
  BIND="127.0.0.1"
fi

CONFIG="${RETH_CONFIG:-/config/reth.toml}"
CHAIN="${RETH_CHAIN:-mainnet}"

exec reth-rex node \
  --config="$CONFIG" \
  --chain="$CHAIN" \
  --log.stdout.filter="info,exex::notifications=debug" \
  --datadir="$DATADIR" \
  --authrpc.addr="$BIND" \
  --authrpc.port=8551 \
  --authrpc.jwtsecret="$JWT" \
  --http \
  --http.addr="$BIND" \
  --http.port=8545 \
  --http.api=eth,net,web3,txpool,debug,trace \
  --http.corsdomain=* \
  --ws \
  --ws.addr="$BIND" \
  --ws.port=8546 \
  --ws.api=eth,net,web3,txpool,debug,trace \
  --ws.origins=* \
  --port=30303 \
  --discovery.port=30303
