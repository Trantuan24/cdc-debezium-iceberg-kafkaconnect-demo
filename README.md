# Multi-source CDC to Apache Iceberg

A local end-to-end CDC lab for MySQL, PostgreSQL, MongoDB, and Oracle. Debezium source connectors publish the original schema-aware Debezium envelopes to raw Kafka topics. Each raw topic feeds two independent Iceberg sinks: a current-state sink applies inserts, updates, and deletes, while an append-only sink stores the unchanged raw Kafka value for audit and replay.

## Sources and destinations

| Source | Native CDC mechanism | Raw Kafka topic | Current table | Raw append table |
|---|---|---|---|---|
| MySQL 8 (`mydb.orders`) | Row-based binlog | `raw.mysql.mydb.orders` | `mysql_mydb.orders_current` | `mysql_mydb.orders_cdc` |
| MySQL 8 (`mydb.customers`; same source connector) | Row-based binlog | `raw.mysql.mydb.customers` | `mysql_mydb.customers_current` | `mysql_mydb.customers_cdc` |
| PostgreSQL 14 | WAL logical decoding (`pgoutput`) | `raw.pg.public.inventory` | `postgres_mydb_pg_public.inventory_current` | `postgres_mydb_pg_public.inventory_cdc` |
| MongoDB 7 | Change Streams on replica set `rs0` | `raw.mongo.mydb_mongo.products` | `mongo_mydb_mongo.products_current` | `mongo_mydb_mongo.products_cdc` |
| Oracle XE 21c | Redo/archive logs through LogMiner | `raw.oracle.DEBEZIUM.TRANSACTIONS` | `oracle_xepdb1_debezium.transactions_current` | `oracle_xepdb1_debezium.transactions_cdc` |

The MySQL source connector intentionally captures both `orders` and `customers`. Each table has one raw topic and two independent sinks: one `*_current` table plus one append-only `*_cdc` table.

Each source performs an initial snapshot and then streams committed changes. The raw Kafka contract keeps Debezium `before`, `after`, `source`, and official `op=c/r/u/d` fields. No source-side SMT rewrites those events.

## Pipeline

```text
Database log / MongoDB Change Stream
  -> Debezium source connector (no transforms)
  -> raw Kafka topic (schema + Debezium envelope; op=c/r/u/d)
       |-> current-state sink: unwrap -> op map -> time normalization -> *_current
       `-> append sink: StringConverter -> RawAppendEnvelope -> *_cdc
  -> Iceberg format-v2 tables on MinIO + Hive Metastore
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
- Custom sink-side Java SMTs `DebeziumOpMapper`, `IsoTimestampNormalizer`, and `RawAppendEnvelope`
- CDC snapshot lineage: `task.engine`, `consumer.typeingest`, `consumer.connectorname`, `consumer.ingest.time`, and `consumer.vtts.time`; `cdcID` stays in connector configuration only

`build-smt.sh` downloads the unchanged pinned sink JAR and verifies its checksum. It builds the small sink-side SMT jar used for operation mapping, timestamp normalization, and the three-column raw append envelope.

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


### Existing-volume migration

Before applying this version to a stack whose dual-mode tables are still under `iceberg.default`, stop Connect, create the four namespaces, and rename the ten tables. Iceberg performs metadata renames, so rows and snapshots are preserved:

```powershell
docker compose stop connect

docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.mysql_mydb; CREATE SCHEMA IF NOT EXISTS iceberg.postgres_mydb_pg_public; CREATE SCHEMA IF NOT EXISTS iceberg.mongo_mydb_mongo; CREATE SCHEMA IF NOT EXISTS iceberg.oracle_xepdb1_debezium;"

docker exec trino trino --execute "ALTER TABLE iceberg.default.orders_current RENAME TO iceberg.mysql_mydb.orders_current; ALTER TABLE iceberg.default.orders_cdc RENAME TO iceberg.mysql_mydb.orders_cdc; ALTER TABLE iceberg.default.customers_current RENAME TO iceberg.mysql_mydb.customers_current; ALTER TABLE iceberg.default.customers_cdc RENAME TO iceberg.mysql_mydb.customers_cdc; ALTER TABLE iceberg.default.inventory_current RENAME TO iceberg.postgres_mydb_pg_public.inventory_current; ALTER TABLE iceberg.default.inventory_cdc RENAME TO iceberg.postgres_mydb_pg_public.inventory_cdc; ALTER TABLE iceberg.default.products_current RENAME TO iceberg.mongo_mydb_mongo.products_current; ALTER TABLE iceberg.default.products_cdc RENAME TO iceberg.mongo_mydb_mongo.products_cdc; ALTER TABLE iceberg.default.transactions_current RENAME TO iceberg.oracle_xepdb1_debezium.transactions_current; ALTER TABLE iceberg.default.transactions_cdc RENAME TO iceberg.oracle_xepdb1_debezium.transactions_cdc;"

.\start-e2e.ps1
```

A fresh `-Reset` run does not need this migration.

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
iceberg-sink-raw-mysql-customers
iceberg-sink-raw-postgres-inventory
iceberg-sink-raw-mongodb-products
iceberg-sink-raw-oracle-transactions
iceberg-append-raw-mysql-orders
iceberg-append-raw-mysql-customers
iceberg-append-raw-postgres-inventory
iceberg-append-raw-mongodb-products
iceberg-append-raw-oracle-transactions
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

Query current state and raw append history with Trino:

```sql
SELECT * FROM iceberg.mysql_mydb.orders_current;
SELECT * FROM iceberg.mysql_mydb.customers_current;
SELECT * FROM iceberg.postgres_mydb_pg_public.inventory_current;
SELECT * FROM iceberg.mongo_mydb_mongo.products_current;
SELECT * FROM iceberg.oracle_xepdb1_debezium.transactions_current;

-- Raw append table: exactly id, record, ngay_cap_nhat
SELECT id, record, ngay_cap_nhat FROM iceberg.mysql_mydb.orders_cdc;
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