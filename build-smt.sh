#!/usr/bin/env bash
# Prepare the custom SMT and the pinned Iceberg Sink fork used by this demo.
set -euo pipefail

ICEBERG_PLUGIN_DIR="kafka-connect/plugins/iceberg-kafka-connect"
ICEBERG_FORK_JAR="${ICEBERG_PLUGIN_DIR}/lib/iceberg-kafka-connect-custom-pipeline-meta.jar"
ICEBERG_FORK_COMMIT="1f8e11c4a9de6f78d76a17e16927b23fb8baf527"
ICEBERG_FORK_SHA256="7ec26e0cccf06c293f2dca133b29be6b22c01c71154254d357d76c66a77ab792"
ICEBERG_FORK_URL="https://raw.githubusercontent.com/Trantuan24/kafka-to-iceberg-connector/${ICEBERG_FORK_COMMIT}/plugins/iceberg-kafka-connect/lib/iceberg-kafka-connect-custom-pipeline-meta.jar"

echo "[1/4] Building custom sink-side CDC SMTs..."
(
  cd smt
  mvn clean package -q
)
mkdir -p kafka-connect/plugins/custom-smt
cp smt/target/debezium-op-mapper-1.0.jar kafka-connect/plugins/custom-smt/
echo "      Custom CDC SMTs ready"

echo "[2/4] Preparing the pinned Iceberg Sink fork..."
mkdir -p "${ICEBERG_PLUGIN_DIR}"
if [ -d "../iceberg-kafka-connect-demo/plugins/iceberg-kafka-connect" ]; then
  # Keep the dependency bundle already used by the original demo. -n prevents
  # it from overwriting files that are already present in this project.
  cp -rn ../iceberg-kafka-connect-demo/plugins/iceberg-kafka-connect/. "${ICEBERG_PLUGIN_DIR}/"
fi
if [ ! -d "${ICEBERG_PLUGIN_DIR}/lib" ]; then
  echo "ERROR: Iceberg connector dependency bundle is missing at ${ICEBERG_PLUGIN_DIR}/lib" >&2
  exit 1
fi
curl -fsSL "${ICEBERG_FORK_URL}" -o "${ICEBERG_FORK_JAR}.tmp"
echo "${ICEBERG_FORK_SHA256} *${ICEBERG_FORK_JAR}.tmp" | sha256sum -c -
mv "${ICEBERG_FORK_JAR}.tmp" "${ICEBERG_FORK_JAR}"
echo "      Iceberg fork pinned to ${ICEBERG_FORK_COMMIT}"

echo "[3/4] Checking MongoDB connector plugin..."
if [ ! -d "kafka-connect/plugins/debezium-connector-mongodb" ]; then
  echo "ERROR: debezium-connector-mongodb 2.5.4.Final is missing" >&2
  exit 1
fi

echo "[4/4] Checking Oracle connector plugin and JDBC driver..."
if [ ! -d "kafka-connect/plugins/debezium-connector-oracle" ]; then
  echo "ERROR: debezium-connector-oracle 2.5.4.Final is missing" >&2
  exit 1
fi
if [ ! -f "kafka-connect/plugins/debezium-connector-oracle/ojdbc11.jar" ]; then
  echo "ERROR: ojdbc11.jar is missing; see scripts/download-oracle-driver.sh" >&2
  exit 1
fi

echo "Plugins are ready. Next: docker compose up --build -d"