# Multi-source CDC to Apache Iceberg

A local end-to-end CDC lab for MySQL, PostgreSQL, MongoDB, and Oracle. Debezium source connectors publish the original schema-aware Debezium envelopes to raw Kafka topics. Iceberg sink connectors perform the flattening and operation mapping, then apply inserts, updates, and deletes to Iceberg format-v2 tables in MinIO.

## Sources and destinations

| Source | Native CDC mechanism | Raw Kafka topic | Iceberg table |
|---|---|---|---|
| MySQL 8 | Row-based binlog | `raw.mysql.mydb.orders` | `default.orders_cdc` |
| PostgreSQL 14 | WAL logical decoding (`pgoutput`) | `raw.pg.public.inventory` | `default.inventory_cdc` |
| MongoDB 7 | Change Streams on replica set `rs0` | `raw.mongo.mydb_mongo.products` | `default.products_cdc` |
| Oracle XE 21c | Redo/archive logs through LogMiner | `raw.oracle.DEBEZIUM.TRANSACTIONS` | `default.transactions_cdc` |

Each source performs an initial snapshot and then streams committed changes. The raw Kafka contract keeps Debezium `before`, `after`, `source`, and official `op=c/r/u/d` fields. No source-side SMT rewrites those events.

## Pipeline

```text
Database log / MongoDB Change Stream
  -> Debezium source connector (no transforms)
  -> raw Kafka topic (schema + Debezium envelope; op=c/r/u/d)
  -> sink-side ExtractNewRecordState / ExtractNewDocumentState
  -> sink-side DebeziumOpMapper: c,r -> I; u -> U; d -> D
  -> temporary __op CDC field
  -> custom Iceberg Kafka Connect sink fork
  -> Iceberg format-v2 table on MinIO + Hive Metastore
  -> Trino
```

`__op` is an internal sink record field. It controls equality deletes/updates and is not a business column in the final Iceberg tables.

See [SOURCE_CDC_GUIDE.md](SOURCE_CDC_GUIDE.md) for source setup, [SINK_CDC_GUIDE.md](SINK_CDC_GUIDE.md) for the downstream contract, and [TESTING.md](TESTING.md) for copy-paste tests covering `r/c/u/d`, raw Kafka topics, Trino queries, snapshots, and restart recovery.

## Components

- Debezium source connectors `2.5.4.Final` bundled under `kafka-connect/plugins`
- Confluent Kafka and Kafka Connect `7.7.1`, KRaft mode
- Apache Iceberg Kafka Connect sink fork pinned to commit `1f8e11c4a9de6f78d76a17e16927b23fb8baf527`
- Pinned sink JAR SHA-256: `7ec26e0cccf06c293f2dca133b29be6b22c01c71154254d357d76c66a77ab792`
- MinIO, Hive Metastore with PostgreSQL backend, and Trino 468
- Custom sink-side Java SMT `DebeziumOpMapper`
- CDC snapshot lineage: `task.engine`, `consumer.typeingest`, `consumer.connectorname`, `consumer.ingest.time`, and `consumer.vtts.time`; `cdcID` stays in connector configuration only

`build-smt.sh` downloads the unchanged pinned sink JAR and verifies its checksum. It builds only the small operation-mapper SMT.

## Start

Prerequisites: Docker Desktop with enough memory for Oracle and Kafka Connect; Java 11+ and Maven for the custom SMT build; Node.js for the Bash registration helper.

```powershell
# Keep existing database volumes
.\start-e2e.ps1

# Recreate every local demo volume and rerun init scripts (destructive)
.\start-e2e.ps1 -Reset
```

Manual equivalent:

```bash
bash build-smt.sh
docker compose up --build -d
bash scripts/register-connectors.sh
```

Initialization SQL/JS runs only when a database volume is new. Changes to users, publications, table DDL, or supplemental logging require a deliberate demo-volume reset or a controlled migration.

## Verify

```powershell
Invoke-RestMethod http://127.0.0.1:8083/connectors?expand=status |
  ConvertTo-Json -Depth 8
```

Expected connector names:

```text
debezium-mysql-raw-source
debezium-postgres-raw-source
debezium-mongodb-raw-source
debezium-oracle-raw-source
iceberg-sink-raw-mysql-orders
iceberg-sink-raw-postgres-inventory
iceberg-sink-raw-mongodb-products
iceberg-sink-raw-oracle-transactions
```

Inspect a raw topic from the beginning. `--timeout-ms` prevents an idle consumer from appearing to hang:

```powershell
docker exec kafka-cdc kafka-console-consumer `
  --bootstrap-server kafka-cdc:9092 `
  --topic raw.mysql.mydb.orders `
  --from-beginning `
  --timeout-ms 10000 `
  --property print.offset=true `
  --property print.key=true
```

Query final state with Trino:

```sql
SELECT * FROM iceberg.default.orders_cdc;
SELECT * FROM iceberg.default.inventory_cdc;
SELECT * FROM iceberg.default.products_cdc;
SELECT * FROM iceberg.default.transactions_cdc;
```

Run the complete four-database acceptance procedure in [TESTING.md](TESTING.md). It covers initial snapshot `r`, streaming `c/u/d`, raw Kafka inspection, Iceberg schemas and rows, snapshot metadata, and Connect restart recovery.

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

## Scope and production cautions

- This is a single-broker/single-worker lab, not a highly available deployment.
- Debezium 2.5 source delivery is at-least-once; recovery can replay records, so identifier keys and sink application must remain idempotent.
- Credentials are demo-only; MongoDB authentication is intentionally disabled locally.
- PostgreSQL WAL slots, MySQL binlogs, MongoDB oplog/pre-images, and Oracle redo/archive logs require capacity and retention monitoring.
- Auto-created schemas are appropriate for this lab. Production should govern Iceberg schemas and compatibility explicitly.