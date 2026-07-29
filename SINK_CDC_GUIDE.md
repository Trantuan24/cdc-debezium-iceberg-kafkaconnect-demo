# Kafka to Iceberg CDC sink guide

The downstream pipeline starts from canonical raw Debezium events:

```text
raw Kafka topic (schema + Debezium envelope; op=c/r/u/d)
  -> sink-side Debezium unwrap SMT
  -> sink-side DebeziumOpMapper (c,r -> I; u -> U; d -> D)
  -> sink-side helper-field cleanup
  -> custom IcebergSinkConnector in CDC mode
  -> Iceberg v2 table in MinIO
  -> Hive Metastore -> Trino
```

## Custom artifacts

Kafka Connect loads two separate artifacts:

1. `debezium-op-mapper-1.0.jar` maps Debezium operations to the sink CDC contract in temporary field `__op`.
2. `iceberg-kafka-connect-custom-pipeline-meta.jar` is used unchanged from commit `1f8e11c4a9de6f78d76a17e16927b23fb8baf527` of the user's fork. Its pinned SHA-256 is `7ec26e0cccf06c293f2dca133b29be6b22c01c71154254d357d76c66a77ab792`.

The fork's API-oriented `CustomCDCTransform` is not used. It expects an API payload shaped like `{data,key,type,version,...}`, while this pipeline receives Debezium envelopes. `DebeziumOpMapper` is the narrow adapter needed for CDC events.

## Four sinks, one contract

| Connector | Raw topic | Iceberg table | Identifier column |
|---|---|---|---|
| `iceberg-sink-raw-mysql-orders` | `raw.mysql.mydb.orders` | `default.orders_cdc` | `id` |
| `iceberg-sink-raw-postgres-inventory` | `raw.pg.public.inventory` | `default.inventory_cdc` | `id` |
| `iceberg-sink-raw-mongodb-products` | `raw.mongo.mydb_mongo.products` | `default.products_cdc` | `_id` |
| `iceberg-sink-raw-oracle-transactions` | `raw.oracle.DEBEZIUM.TRANSACTIONS` | `default.transactions_cdc` | `ID` |

Every sink uses schema-aware JSON keys/values and the same transform order:

```json
{
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "transforms": "unwrap,opMap,dropFields",
  "iceberg.tables.cdc-field": "__op",
  "iceberg.tables.auto-create-enabled": "true",
  "iceberg.tables.evolve-schema-enabled": "true",
  "task.engine": "consumer",
  "consumer.typeingest": "CDC",
  "cdcID": "one-stable-UUID-per-pipeline"
}
```

Relational sources use `io.debezium.transforms.ExtractNewRecordState`; MongoDB uses `io.debezium.connector.mongodb.transforms.ExtractNewDocumentState`. `DebeziumOpMapper` must run after unwrap. The sink removes Debezium helper fields after mapping.

`__op` is not an Iceberg business column. It is consumed as the CDC discriminator while the connector creates data and equality-delete files.

## Destination columns

The destination receives the current business row/document fields, not the whole Debezium envelope:

| Table | Business columns |
|---|---|
| `orders_cdc` | `id`, `customer_name`, `amount`, `status`, `updated_at` |
| `inventory_cdc` | source `inventory` columns, including precise decimal `price` |
| `products_cdc` | flattened MongoDB document fields, including `_id` and `price` |
| `transactions_cdc` | Oracle transaction fields; `ID`/`ACCOUNT_ID` become Iceberg `BIGINT`, `AMOUNT` remains decimal |

Fields such as `before`, `after`, `source`, raw `op`, `__deleted`, and temporary `__op` do not become destination business columns.

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
FROM iceberg.default."orders_cdc$snapshots"
ORDER BY committed_at DESC;
```

Repeat for `inventory_cdc`, `products_cdc`, and `transactions_cdc`.

## References

- [Custom fork used by this project](https://github.com/Trantuan24/kafka-to-iceberg-connector/tree/main/iceberg-kafka-connect-fork)
- [Pinned repository commit](https://github.com/Trantuan24/kafka-to-iceberg-connector/commit/1f8e11c4a9de6f78d76a17e16927b23fb8baf527)
- [Upstream Iceberg Kafka Connect CDC configuration](https://github.com/databricks/iceberg-kafka-connect#change-data-capture)
- [Apache Iceberg snapshot specification](https://iceberg.apache.org/spec/#snapshots)
- [Debezium event flattening](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)
- [Debezium MongoDB event flattening](https://debezium.io/documentation/reference/transformations/mongodb-event-flattening.html)