# CDC source configuration guide

The four Debezium source connectors are intentionally raw producers. They capture the database-native change stream and publish schema-aware Debezium envelopes to Kafka. Flattening, operation mapping, and removal of helper fields happen only in the matching Iceberg sink connector.

## Source matrix

| Source | Native change stream | Required source setup | Raw Kafka topic |
|---|---|---|---|
| MySQL 8 | Row-based binary log | `binlog_format=ROW`, `binlog_row_image=FULL`, unique server ID, snapshot/replication grants | `raw.mysql.mydb.orders` |
| PostgreSQL 14 | WAL logical decoding through `pgoutput` | `wal_level=logical`, replication role, slot `dbz_inventory_slot`, publication `dbz_inventory_publication` | `raw.pg.public.inventory` |
| MongoDB 7 | Change Streams backed by the oplog | Replica set `rs0`; collection pre-images for reliable delete keys | `raw.mongo.mydb_mongo.products` |
| Oracle XE 21c | Online/archived redo through LogMiner | ARCHIVELOG, minimal database logging, ALL-column supplemental logging on the captured table, common mining user | `raw.oracle.DEBEZIUM.TRANSACTIONS` |

## Raw Kafka contract

Every source config follows the same rules:

- no `transforms` property on the source connector;
- schema-aware `JsonConverter` for both key and value (`schemas.enable=true`);
- original Debezium envelope retained, including `before`, `after`, `source`, timestamps, transaction metadata where available, and `op`;
- Debezium operation values remain official `c`, `r`, `u`, and `d`;
- each source has an isolated `raw.*` topic prefix;
- relational decimal handling is `precise`, not `double`;
- schema history topics are separate from data topics and use the `schema-history.raw.*` namespace.

Keeping the envelope in Kafka makes the topic replayable by different consumers and preserves audit context. The Iceberg-specific `I/U/D` mapping is a downstream concern and must not replace the canonical Debezium operation in the raw topic.

## Operation semantics

| Debezium `op` | Meaning | Iceberg sink mapping |
|---|---|---|
| `r` | row/document read during initial or ad-hoc snapshot | `I` |
| `c` | committed insert/create during streaming | `I` |
| `u` | committed update | `U` |
| `d` | committed delete | `D` |

The raw topic always stores the left-hand values. The mapping on the right exists temporarily only after the sink begins processing a record.

## Delivery semantics

The bundled Debezium `2.5.4.Final` connectors provide at-least-once delivery. A committed source change is not intentionally lost, but a failure boundary can replay a Kafka record. Tables therefore require stable identifier columns, and downstream processing must be idempotent.

Do not set `exactly.once.support=required` on this connector bundle. MySQL, MongoDB, and Oracle 2.5 plugins in this runtime do not implement the required exactly-once source preflight API. Upgrade and retest every Debezium plugin and Kafka Connect worker together before enabling source EOS.

## Database-specific setup

### MySQL

The `debezium` account has snapshot privileges (`SELECT`, `LOCK TABLES`) and binlog privileges (`RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT`). Binlog retention must exceed the longest expected connector outage. `DECIMAL` values use the Debezium precise logical type and the JSON converter emits numeric values.

### PostgreSQL

Debezium connects as a dedicated `LOGIN REPLICATION` role. Initialization creates the publication, and `publication.autocreate.mode=disabled` avoids granting broad object-creation rights. Monitor `pg_replication_slots`: an inactive slot retains WAL until the connector resumes or the slot is removed deliberately.

### MongoDB

Debezium consumes MongoDB Change Streams; a standalone MongoDB instance cannot provide this CDC stream. The demo uses a one-node replica set and intentionally disables authentication.

`changeStreamPreAndPostImages` is enabled on `mydb_mongo.products`, and the connector uses `change_streams_update_full_with_pre_image`. This preserves `_id` for deletes so the sink can generate a valid equality delete. The raw MongoDB `after`/`before` document representation is left in Debezium's native envelope; `ExtractNewDocumentState` runs at the sink.

### Oracle

The connector authenticates as common LogMiner user `C##DBZUSER`; application data remains in PDB schema `DEBEZIUM`. The captured `TRANSACTIONS` table uses explicit numeric definitions:

- `ID NUMBER(18,0)`
- `ACCOUNT_ID NUMBER(18,0)`
- `AMOUNT NUMBER(15,2)`

Explicit precision and scale prevent Oracle's unconstrained `NUMBER` from becoming Debezium `VariableScaleDecimal`, which is unsuitable for stable automatic Iceberg schema creation. Supplemental logging is enabled at database level and as ALL columns only on the captured table.

## Reinitializing source databases

Init scripts run only for new database volumes. `./start-e2e.ps1 -Reset` destroys all local demo volumes and recreates them; never use that option against data that must be preserved. For a non-demo environment, apply controlled database migrations instead.

## Official references

- [Debezium change-event format](https://debezium.io/documentation/reference/stable/connectors/index.html)
- [Debezium MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium MongoDB connector](https://debezium.io/documentation/reference/stable/connectors/mongodb.html)
- [Debezium Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
- [Debezium event flattening SMT](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)
- [Debezium MongoDB event flattening](https://debezium.io/documentation/reference/stable/transformations/mongodb-event-flattening.html)