FROM confluentinc/cp-kafka-connect-base:7.7.1

USER root

# ── Debezium MySQL Connector 2.5.4 ───────────────────────────
COPY kafka-connect/plugins/debezium-connector-mysql /usr/share/java/debezium-connector-mysql

# ── Debezium Postgres Connector 2.5.4 ────────────────────────
COPY kafka-connect/plugins/debezium-connector-postgres /usr/share/java/debezium-connector-postgres

# ── Debezium MongoDB Connector 2.5.4 ─────────────────────────
COPY kafka-connect/plugins/debezium-connector-mongodb /usr/share/java/debezium-connector-mongodb

# ── Debezium Oracle Connector 2.5.4 + ojdbc11.jar ────────────
# NOTE: ojdbc11.jar phải download thủ công (Oracle license)
#       Chạy: bash scripts/download-oracle-driver.sh để hướng dẫn
COPY kafka-connect/plugins/debezium-connector-oracle /usr/share/java/debezium-connector-oracle

# ── Tabular Iceberg Sink v0.6.19 ─────────────────────────────
COPY kafka-connect/plugins/iceberg-kafka-connect /usr/share/java/iceberg-kafka-connect

# ── Custom SMT (DebeziumOpMapper) ────────────────────────────
COPY kafka-connect/plugins/custom-smt /usr/share/java/custom-smt

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
