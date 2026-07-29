#!/usr/bin/env bash
# build-smt.sh – Chuẩn bị tất cả plugins trước khi docker-compose up
set -e

echo "🔨 [1/4] Building DebeziumOpMapper SMT..."
cd smt && mvn clean package -q && cd ..
mkdir -p kafka-connect/plugins/custom-smt
cp smt/target/debezium-op-mapper-1.0.jar kafka-connect/plugins/custom-smt/
echo "    ✓ SMT JAR → kafka-connect/plugins/custom-smt/"

echo ""
echo "📦 [2/4] Copying Iceberg plugin JARs from old project..."
mkdir -p kafka-connect/plugins/iceberg-kafka-connect
cp -rn ../iceberg-kafka-connect-demo/plugins/iceberg-kafka-connect/. kafka-connect/plugins/iceberg-kafka-connect/
echo "    ✓ Tabular Iceberg JARs copied"

echo ""
echo "🍃 [3/4] Checking MongoDB connector plugin..."
if [ ! -d "kafka-connect/plugins/debezium-connector-mongodb" ]; then
  echo "    ⚠️  debezium-connector-mongodb chưa có."
  echo "    Download tại: https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/2.5.4.Final/"
  echo "    Giải nén vào: kafka-connect/plugins/debezium-connector-mongodb/"
else
  echo "    ✓ MongoDB connector plugin found"
fi

echo ""
echo "🏛️  [4/4] Checking Oracle connector plugin + JDBC driver..."
if [ ! -d "kafka-connect/plugins/debezium-connector-oracle" ]; then
  echo "    ⚠️  debezium-connector-oracle chưa có."
  echo "    Download tại: https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/2.5.4.Final/"
  echo "    Giải nén vào: kafka-connect/plugins/debezium-connector-oracle/"
else
  echo "    ✓ Oracle connector plugin found"
fi

if [ ! -f "kafka-connect/plugins/debezium-connector-oracle/ojdbc11.jar" ]; then
  echo ""
  echo "    ❌ ojdbc11.jar MISSING! Chạy: bash scripts/download-oracle-driver.sh"
  echo "    (Oracle yêu cầu download thủ công do license)"
fi

echo ""
echo "✅ Done! Tiếp theo:"
echo "  docker-compose up --build -d"
echo "  bash scripts/register-connectors.sh"
