#!/usr/bin/env bash
set -euo pipefail

CONNECT_HOST="${CONNECT_HOST:-http://localhost:8083}"

echo "⏳ Waiting for Kafka Connect to be ready..."
until curl -sf "${CONNECT_HOST}/connectors" > /dev/null; do
  sleep 3
done
echo "✅ Kafka Connect is up!"

# ────────────────────────────────────────────────────────────────
# SOURCE CONNECTORS
# ────────────────────────────────────────────────────────────────

echo ""
echo "📡 [1/4] Registering Debezium MySQL Source..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-source.json | python3 -m json.tool

echo ""
echo "🐘 [2/4] Registering Debezium PostgreSQL Source..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres-source.json | python3 -m json.tool

echo ""
echo "🍃 [3/4] Registering Debezium MongoDB Source..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-mongodb-source.json | python3 -m json.tool

echo ""
echo "🏛️  [4/4] Registering Debezium Oracle Source..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-oracle-source.json | python3 -m json.tool

# ────────────────────────────────────────────────────────────────
# SINK CONNECTORS
# ────────────────────────────────────────────────────────────────

echo ""
echo "🧊 [5/8] Registering Iceberg Sink – MySQL orders..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/iceberg-sink.json | python3 -m json.tool

echo ""
echo "🧊 [6/8] Registering Iceberg Sink – PostgreSQL inventory..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/iceberg-sink-postgres.json | python3 -m json.tool

echo ""
echo "🧊 [7/8] Registering Iceberg Sink – MongoDB products..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/iceberg-sink-mongodb.json | python3 -m json.tool

echo ""
echo "🧊 [8/8] Registering Iceberg Sink – Oracle transactions..."
curl -sf -X POST "${CONNECT_HOST}/connectors" \
  -H "Content-Type: application/json" \
  -d @connectors/iceberg-sink-oracle.json | python3 -m json.tool

echo ""
echo "══════════════════════════════════════════════════════"
echo "  ✅ All 8 connectors registered!"
echo "══════════════════════════════════════════════════════"
echo ""
echo "📋 Connector status:"
curl -sf "${CONNECT_HOST}/connectors?expand=status" | python3 -m json.tool
