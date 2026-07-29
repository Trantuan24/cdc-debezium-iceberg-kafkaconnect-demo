# Multi-source CDC to Apache Iceberg

A local end-to-end CDC lab for four source databases. Debezium source connectors run in distributed Kafka Connect, publish normalized change records to Kafka, and the Apache Iceberg Kafka Connect sink applies inserts, updates, and deletes to Iceberg format-v2 tables stored in MinIO.

## Sources and topics

| Source | CDC mechanism | Kafka topic | Iceberg table |
|---|---|---|---|
| MySQL 8 | Row-based binlog | `mysql.mydb.orders` | `default.orders_cdc` |
| PostgreSQL 14 | WAL logical decoding (`pgoutput`) | `pg.public.inventory` | `default.inventory_cdc` |
| MongoDB 7 | Change Streams on replica set `rs0` | `mongo.mydb_mongo.products` | `default.products_cdc` |
| Oracle XE 21c | Redo/archive logs through LogMiner | `oracle.DEBEZIUM.TRANSACTIONS` | `default.transactions_cdc` |

Each source performs an initial snapshot and then streams committed changes. The bundled Debezium 2.5 connectors provide at-least-once delivery; downstream CDC application must be idempotent by identifier key.

See [SOURCE_CDC_GUIDE.md](SOURCE_CDC_GUIDE.md) for database privileges, replication objects, delivery semantics, production caveats, and official references.

## Pipeline

```text
Database replication log / Change Streams
  -> Debezium source connector
  -> ExtractNewRecordState / ExtractNewDocumentState
  -> DebeziumOpMapper: c,r -> I; u -> U; d -> D
  -> Kafka table topic
  -> Apache Iceberg Kafka Connect sink (CDC field: __op)
  -> Iceberg format-v2 table on MinIO + Hive Metastore
  -> Trino
```

## Components

- Debezium source connectors `2.5.4.Final` currently bundled in `kafka-connect/plugins`
- Confluent Kafka and Kafka Connect `7.7.1`, KRaft mode
- Apache Iceberg Kafka Connect sink (local bundled build)
- MinIO, Hive Metastore with PostgreSQL backend, and Trino 468
- Custom Java SMT `DebeziumOpMapper`

The local connector binaries are intentionally kept as-is for reproducibility. Before production use, pin a supported Debezium release and a clean Apache Iceberg Kafka Connect release/build, then run upgrade compatibility tests.

## Start

Prerequisites: Docker Desktop with enough memory for Oracle and Kafka Connect; Java 11+ and Maven only if rebuilding the custom SMT.

```powershell
# Keep existing database volumes
.\start-e2e.ps1

# Recreate all local demo databases and rerun init scripts (destroys demo data)
.\start-e2e.ps1 -Reset
```

Or run the steps manually:

```bash
bash build-smt.sh
docker compose up --build -d
bash scripts/register-connectors.sh
```

Initialization SQL/JS only runs when its database volume is new. Source privilege, publication, or supplemental-logging changes therefore require a deliberate demo-volume reset.

## Verify

```bash
curl http://localhost:8083/connectors?expand=status
```

Expected connector names:

```text
debezium-mysql-source
debezium-postgres-source
debezium-mongodb-source
debezium-oracle-source
iceberg-sink-mysql-orders
iceberg-sink-postgres-inventory
iceberg-sink-mongodb-products
iceberg-sink-oracle-transactions
```

The existing automated data test covers MySQL only:

```bash
pip install mysql-connector-python trino
python scripts/test_pipeline.py
```

Manual Trino examples:

```sql
SELECT * FROM iceberg.default.orders_cdc;
SELECT * FROM iceberg.default.inventory_cdc;
SELECT * FROM iceberg.default.products_cdc;
SELECT * FROM iceberg.default.transactions_cdc;
```

## Ports

| Service | Host port |
|---|---:|
| MySQL | 3306 |
| PostgreSQL source | 5433 |
| MongoDB | 27017 |
| Oracle | 1521 |
| Kafka external listener | 29092 |
| Kafka Connect REST | 8083 |
| MinIO API / console | 9000 / 9001 |
| Hive Metastore | 9083 |
| Trino | 8080 |

## Important limitations

- This is a single-broker/single-worker local lab, not a highly available deployment.
- Credentials are demo-only and stored in configuration files.
- MongoDB authentication is intentionally disabled locally.
- Oracle can take several minutes to become healthy.
- The test suite does not yet exercise PostgreSQL, MongoDB, or Oracle mutations.
- PostgreSQL slots, MySQL binlogs, MongoDB oplog history, and Oracle archive logs require retention monitoring in production.