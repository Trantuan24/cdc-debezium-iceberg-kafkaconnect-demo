# Kafka to Iceberg CDC sink guide

The downstream pipeline starts from canonical raw Debezium events:

```text
raw Kafka topic (schema + Debezium envelope; op=c/r/u/d)
  |-> current sink: unwrap -> op map -> timestamp normalization -> *_current
  `-> append sink: StringConverter -> RawAppendEnvelope -> *_cdc
       (id, unchanged record string, ngay_cap_nhat)
  -> Iceberg v2 tables in MinIO -> Hive Metastore -> Trino
```

## Custom artifacts

Kafka Connect loads two separate artifacts:

1. `debezium-op-mapper-1.0.jar` contains `DebeziumOpMapper`, `IsoTimestampNormalizer`, and `RawAppendEnvelope`. The first two materialize current state; the third creates the independent three-column raw append record.
2. `iceberg-kafka-connect-custom-pipeline-meta.jar` is used unchanged from commit `1f8e11c4a9de6f78d76a17e16927b23fb8baf527` of the user's fork. Its pinned SHA-256 is `7ec26e0cccf06c293f2dca133b29be6b22c01c71154254d357d76c66a77ab792`.

The fork's API-oriented `CustomCDCTransform` is not used. It expects an API payload shaped like `{data,key,type,version,...}`, while this pipeline receives Debezium envelopes. `DebeziumOpMapper` is the narrow adapter needed for CDC events.

## Current-state sinks

| Connector | Raw topic | Iceberg table | Identifier column |
|---|---|---|---|
| `iceberg-sink-raw-mysql-orders` | `raw.mysql.mydb.orders` | `mysql_mydb.orders_current` | `id` |
| `iceberg-sink-raw-mysql-customers` | `raw.mysql.mydb.customers` | `mysql_mydb.customers_current` | `id` |
| `iceberg-sink-raw-postgres-inventory` | `raw.pg.public.inventory` | `postgres_mydb_pg_public.inventory_current` | `id` |
| `iceberg-sink-raw-mongodb-products` | `raw.mongo.mydb_mongo.products` | `mongo_mydb_mongo.products_current` | `_id` |
| `iceberg-sink-raw-oracle-transactions` | `raw.oracle.DEBEZIUM.TRANSACTIONS` | `oracle_xepdb1_debezium.transactions_current` | `ID` |

Every current-state sink uses schema-aware JSON keys/values and the same transform order:

```json
{
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "transforms": "unwrap,opMap,normalizeTime,dropFields",
  "transforms.normalizeTime.type": "com.example.smt.IsoTimestampNormalizer",
  "transforms.normalizeTime.fields": "created_at,updated_at",
  "iceberg.tables.cdc-field": "__op",
  "iceberg.tables.auto-create-enabled": "true",
  "iceberg.tables.evolve-schema-enabled": "true",
  "task.engine": "consumer",
  "consumer.typeingest": "CDC",
  "cdcID": "one-stable-UUID-per-pipeline"
}
```

Relational sources use `io.debezium.transforms.ExtractNewRecordState`; MongoDB uses `io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`. `DebeziumOpMapper` must run after unwrap. The sink removes Debezium helper fields after mapping.

Timestamp fields are normalized after unwrap and before Iceberg schema inference. The destination contract is a UTC ISO string in the form `yyyy-MM-ddTHH:mm:ssZ` for configured time fields. For Oracle the configured field names are uppercase (`CREATED_AT,UPDATED_AT`) because that is the Connect record schema before Iceberg lowercases table columns.

`__op` is not an Iceberg business column. It is consumed as the CDC discriminator while the connector creates data and equality-delete files.

## Destination columns

The destination receives the current business row/document fields, not the whole Debezium envelope:

| Table | Business columns |
|---|---|
| `orders_current` | `id`, `customer_name`, `amount`, `status`, `updated_at` |
| `customers_current` | `id`, `full_name`, `email`, `status`, `updated_at` |
| `inventory_current` | source `inventory` columns, including precise decimal `price` |
| `products_current` | flattened MongoDB document fields, including `_id` and `price` |
| `transactions_current` | Oracle transaction fields; `ID`/`ACCOUNT_ID` become Iceberg `BIGINT`, `AMOUNT` remains decimal |

Fields such as `before`, `after`, `source`, raw `op`, `__deleted`, and temporary `__op` do not become destination business columns.


## Append-only raw CDC contract

A second sink connector consumes each raw topic with its own consumer group and writes the matching `*_cdc` table. It does not unwrap or map `r/c/u/d`.

| Raw topic | Append connector | Iceberg table |
|---|---|---|
| `raw.mysql.mydb.orders` | `iceberg-append-raw-mysql-orders` | `mysql_mydb.orders_cdc` |
| `raw.mysql.mydb.customers` | `iceberg-append-raw-mysql-customers` | `mysql_mydb.customers_cdc` |
| `raw.pg.public.inventory` | `iceberg-append-raw-postgres-inventory` | `postgres_mydb_pg_public.inventory_cdc` |
| `raw.mongo.mydb_mongo.products` | `iceberg-append-raw-mongodb-products` | `mongo_mydb_mongo.products_cdc` |
| `raw.oracle.DEBEZIUM.TRANSACTIONS` | `iceberg-append-raw-oracle-transactions` | `oracle_xepdb1_debezium.transactions_cdc` |

Every raw append table has exactly:

| Column | Contract |
|---|---|
| `id` | Deterministic `topic-partition-offset`, for example `raw.mysql.mydb.orders-0-12` |
| `record` | Unchanged UTF-8 Kafka value received through `StringConverter` |
| `ngay_cap_nhat` | UTC processing time generated by the sink SMT |

`RawAppendEnvelope` skips Debezium tombstones because their Kafka value is null. Records with a non-null value, including `r`, `c`, `u`, and `d`, are append-only. The raw JSON is never parsed or reformatted; runtime verification compares its SHA-256 with the corresponding Kafka value.

## MongoDB delete requirement

MongoDB pre-images must be enabled so a delete still carries `_id`. The source uses `capture.mode=change_streams_update_full_with_pre_image`; the sink's MongoDB unwrap transform exposes the previous document for a delete and the mapper produces `__op=D`. Without the identifier, an equality delete cannot target the existing Iceberg row.

Schema-aware key conversion is required. A `StringConverter` key bypasses the MongoDB unwrap transform's expected key structure and can leave an unflattened envelope at the sink.

## Snapshot metadata contract

Each normal CDC commit contains these custom business lineage entries:

| Snapshot summary key | Value |
|---|---|
| `task.engine` | `consumer` |
| `consumer.typeingest` | `CDC` |
| `consumer.connectorname` | sink connector name |
| `consumer.ingest.time` | epoch milliseconds when the sink commits |
| `consumer.vtts.time` | valid-through Kafka record timestamp watermark |

`cdcID` remains in Kafka Connect configuration only because the pinned fork does not write that property into snapshot summaries. `pipeline.source-type`, `pipeline.snapshot-uuid`, and `pipeline.topic` from the old API path are not configured.

Iceberg still writes its required internal summary entries, including `operation`, file/row counts, `kafka.connect.commit-id`, and checkpoint offsets. “Only five fields” refers only to custom business-lineage fields.

### Watermark semantics

For one partition, `consumer.vtts.time` is the latest processed Kafka record timestamp. For multiple assigned partitions, it is the minimum of each partition's latest timestamp. That minimum is a safe valid-through watermark because every partition has progressed through it. A startup/partial commit can omit the field until all assigned partitions have a timestamp.

## Schema handling

The source and sink both use schema-aware JSON. Verified destination types include:

- MySQL `amount`: `DECIMAL(38,2)`
- PostgreSQL `price`: `DECIMAL(38,2)`
- MongoDB `price`: `DOUBLE`
- Oracle `ID`/`ACCOUNT_ID`: `BIGINT`; `AMOUNT`: `DECIMAL(38,2)`
- Destination time fields across MySQL, PostgreSQL, MongoDB, and Oracle: UTC ISO strings such as `2026-07-29T09:51:08Z`

Oracle source `NUMBER` columns use explicit precision/scale. This prevents `VariableScaleDecimal` and avoids first-record type inference errors. Auto-create/evolution is convenient for the lab; production should predefine and govern schemas.

## Build and registration

```bash
bash build-smt.sh
docker compose build connect
docker compose up -d --no-deps --force-recreate connect
bash scripts/register-connectors.sh
```

For an existing connector, use Kafka Connect `PUT /connectors/{name}/config`; POST is only for creation.

## Verification

All connector and task states must be `RUNNING`:

```powershell
Invoke-RestMethod http://127.0.0.1:8083/connectors?expand=status |
  ConvertTo-Json -Depth 8
```

Inspect snapshot metadata with Trino:

```sql
SELECT
  snapshot_id,
  committed_at,
  operation,
  element_at(summary, 'task.engine') AS task_engine,
  element_at(summary, 'consumer.typeingest') AS type_ingest,
  element_at(summary, 'consumer.connectorname') AS connector_name,
  element_at(summary, 'consumer.ingest.time') AS ingest_time,
  element_at(summary, 'consumer.vtts.time') AS vtts_time,
  element_at(summary, 'pipeline.source-type') AS must_be_null,
  element_at(summary, 'cdcID') AS must_be_null
FROM iceberg.mysql_mydb."orders_current$snapshots"
ORDER BY committed_at DESC;
```

Repeat for `customers_current`, `inventory_current`, `products_current`, and `transactions_current`. Raw `*_cdc` snapshot operations are append-only.

## References

- [Custom fork used by this project](https://github.com/Trantuan24/kafka-to-iceberg-connector/tree/main/iceberg-kafka-connect-fork)
- [Pinned repository commit](https://github.com/Trantuan24/kafka-to-iceberg-connector/commit/1f8e11c4a9de6f78d76a17e16927b23fb8baf527)
- [Upstream Iceberg Kafka Connect CDC configuration](https://github.com/databricks/iceberg-kafka-connect#change-data-capture)
- [Apache Iceberg snapshot specification](https://iceberg.apache.org/spec/#snapshots)
- [Debezium event flattening](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)
- [Debezium MongoDB event flattening](https://debezium.io/documentation/reference/transformations/mongodb-event-flattening.html)