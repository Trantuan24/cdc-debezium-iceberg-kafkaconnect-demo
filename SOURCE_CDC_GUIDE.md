# CDC source configuration guide

This project uses log-based Debezium source connectors running in distributed Kafka Connect. Each connector performs an initial snapshot and then streams committed row/document changes from the database-native replication mechanism.

## Source matrix

| Source | Native change stream | Required source setup | Kafka topic |
|---|---|---|---|
| MySQL 8 | Row-based binary log | `binlog_format=ROW`, `binlog_row_image=FULL`, unique server ID, snapshot/replication grants | `mysql.mydb.orders` |
| PostgreSQL 14 | WAL logical decoding via `pgoutput` | `wal_level=logical`, replication user, slot `dbz_inventory_slot`, publication `dbz_inventory_publication` | `pg.public.inventory` |
| MongoDB 7 | MongoDB Change Streams backed by the oplog | Replica set `rs0`; `change_streams_update_full` supplies complete update documents | `mongo.mydb_mongo.products` |
| Oracle XE 21c | Online/archived redo via LogMiner | ARCHIVELOG, database minimal supplemental logging, table ALL-column supplemental logging, common mining user | `oracle.DEBEZIUM.TRANSACTIONS` |

## Delivery semantics

The bundled Debezium `2.5.4.Final` connectors use the standard at-least-once guarantee: committed database changes are not intentionally lost, but a failure/recovery boundary can replay records. Downstream CDC handling must therefore be idempotent by primary/identifier key.

Debezium added exactly-once source support for all core connectors in 3.3. Do not set `exactly.once.support=required` on this bundled connector set: runtime validation confirms that the 2.5 MySQL, MongoDB, and Oracle plugins do not implement the required preflight API. To enable EOS for all four sources, upgrade all Debezium plugins together to a supported 3.3+ release, enable `exactly.once.source.support=enabled` on every worker, and then set `exactly.once.support=required` with `transaction.boundary=poll` on every source connector.

## Database-specific decisions

### MySQL

The `debezium` account receives `SELECT` and `LOCK TABLES` for a consistent initial snapshot, plus `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT` for binlog capture. Binlog retention is three days in this demo. Production retention must exceed the longest expected connector outage.

### PostgreSQL

The container administrative account is `postgres`; Debezium connects as a dedicated `LOGIN REPLICATION` role. The publication is created by initialization SQL and `publication.autocreate.mode=disabled` prevents the connector from requiring table ownership or database-wide CREATE privileges. Monitor `pg_replication_slots` because an inactive slot retains WAL.

### MongoDB

Debezium consumes Change Streams rather than reading `local.oplog.rs` directly. A standalone MongoDB server cannot provide this CDC stream. This local demo intentionally runs without authentication; production must use authentication, TLS, a multi-member replica set, and a dedicated Debezium user.

`change_streams_update_full` performs a lookup to return a full `after` document for updates. A later update can win that lookup race, so workloads that require exact before/after images should use MongoDB pre/post images with a compatible Debezium capture mode.

### Oracle

The connector authenticates as common user `C##DBZUSER`, not `SYSTEM` and not the application schema owner. Application data remains in local PDB schema `DEBEZIUM`. Minimal supplemental logging is enabled at database level and ALL-column supplemental logging only on the captured table to avoid unnecessary redo growth.

Oracle startup and initial LogMiner snapshots are slower than the other demo sources. Monitor archive destinations, redo sizing, long-running transactions, connector heap, and Oracle `UNDO_RETENTION`.

## Reinitializing source databases

Database initialization scripts run only when their data volume is first created. After changing an init script, an existing volume keeps the old users, grants, publication, and supplemental logging. To rebuild demo data from scratch, explicitly remove the Compose volumes before starting again. This destroys local demo data and must not be used against a real environment.

## Official references

- [Debezium MySQL connector](https://debezium.io/documentation/reference/3.5/connectors/mysql.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.5/connectors/postgresql.html)
- [Debezium MongoDB connector](https://debezium.io/documentation/reference/3.5/connectors/mongodb.html)
- [Debezium Oracle connector](https://debezium.io/documentation/reference/3.5/connectors/oracle.html)
- [Debezium exactly-once delivery](https://debezium.io/documentation/reference/3.5/configuration/eos.html)
- [Debezium event flattening SMT](https://debezium.io/documentation/reference/3.5/transformations/event-flattening.html)