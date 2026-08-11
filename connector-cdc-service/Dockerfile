# Kafka Connect worker dedicated to the Debezium -> Kafka -> Iceberg pipeline.
FROM confluentinc/cp-kafka-connect-base:7.7.1

LABEL org.opencontainers.image.title="connector-cdc-service"
LABEL org.opencontainers.image.description="Kafka Connect with four Debezium sources, Iceberg sink, and CDC SMTs"

# Debezium source connectors (2.5.4.Final).
COPY plugins/debezium-connector-mysql /usr/share/java/debezium-connector-mysql
COPY plugins/debezium-connector-postgres /usr/share/java/debezium-connector-postgres
COPY plugins/debezium-connector-mongodb /usr/share/java/debezium-connector-mongodb
COPY plugins/debezium-connector-oracle /usr/share/java/debezium-connector-oracle

# Verified custom Iceberg sink fork and local Debezium SMT bundle.
COPY plugins/iceberg-kafka-connect /usr/share/java/iceberg-kafka-connect
COPY plugins/custom-smt /usr/share/java/custom-smt

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

EXPOSE 8083
