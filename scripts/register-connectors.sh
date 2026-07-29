#!/usr/bin/env bash
set -euo pipefail

CONNECT_HOST="${CONNECT_HOST:-http://localhost:8083}"

if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: Node.js is required to read connector JSON files." >&2
  exit 1
fi

echo "Waiting for Kafka Connect..."
until curl -sf "${CONNECT_HOST}/connectors" >/dev/null; do
  sleep 3
done

echo "Kafka Connect is ready."

register_connector() {
  local file="$1"
  local name
  name="$(node -e 'const fs=require("fs"); console.log(JSON.parse(fs.readFileSync(process.argv[1],"utf8")).name)' "$file")"

  if curl -sf "${CONNECT_HOST}/connectors/${name}" >/dev/null; then
    echo "Updating ${name} from ${file}"
    node -e 'const fs=require("fs"); console.log(JSON.stringify(JSON.parse(fs.readFileSync(process.argv[1],"utf8")).config))' "$file" |
      curl -sf -X PUT "${CONNECT_HOST}/connectors/${name}/config" \
        -H "Content-Type: application/json" -d @-
  else
    echo "Creating ${name} from ${file}"
    curl -sf -X POST "${CONNECT_HOST}/connectors" \
      -H "Content-Type: application/json" -d "@${file}"
  fi
  echo ""
}

files=(
  connectors/debezium-mysql-raw-source.json
  connectors/debezium-postgres-raw-source.json
  connectors/debezium-mongodb-raw-source.json
  connectors/debezium-oracle-raw-source.json
  connectors/iceberg-sink-raw-mysql-orders.json
  connectors/iceberg-sink-raw-postgres-inventory.json
  connectors/iceberg-sink-raw-mongodb-products.json
  connectors/iceberg-sink-raw-oracle-transactions.json
)

for file in "${files[@]}"; do
  register_connector "$file"
done

echo "All eight connector configurations are applied."
curl -sf "${CONNECT_HOST}/connectors?expand=status"
echo ""