# CDC end-to-end test commands

All commands below are PowerShell commands executed from the repository root.

## 1. Check the stack and connectors

```powershell
docker compose ps

$status = Invoke-RestMethod 'http://127.0.0.1:8083/connectors?expand=status'
$status.PSObject.Properties | ForEach-Object {
  $s = $_.Value.status
  [pscustomobject]@{
    Name      = $_.Name
    Connector = $s.connector.state
    Tasks     = (($s.tasks | ForEach-Object state) -join ',')
  }
} | Sort-Object Name | Format-Table -AutoSize
```

Expected: all fourteen connectors and all fourteen tasks are `RUNNING`.

## 2. Test snapshot operation `r`

`r` is emitted only when Debezium takes a snapshot. The clearest repeatable test is a full demo reset. This command destroys all local demo volumes and data:

```powershell
.\start-e2e.ps1 -Reset
```

The init scripts seed at least one row/document in every captured source table. After all connectors are running, inspect snapshot events:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'             | Select-String -Pattern '"op"\s*:\s*"r"'
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory'           | Select-String -Pattern '"op"\s*:\s*"r"'
.\scripts\read-raw-topic.ps1 'raw.mongo.mydb_mongo.products'     | Select-String -Pattern '"op"\s*:\s*"r"'
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS'  | Select-String -Pattern '"op"\s*:\s*"r"'
```

Expected: every command prints at least one raw Debezium envelope containing `op=r`. Kafka retains `before`, `after`, `source`, and the original lowercase operation.

## 3. Create test identifiers

Use a new identifier when repeating the test. The values below are deliberately outside the demo seed range.

```powershell
$TestId    = 910001
$MongoId   = 'cdc-test-910001'
$OracleRef = 'CDC-TEST-910001'
```

## 4. Test `c` — insert/create

```powershell
# MySQL
docker exec mysql mysql -uroot -proot mydb -e `
  "INSERT INTO orders (id,customer_name,product,amount,status) VALUES ($TestId,'CDC Test','Keyboard',111.25,'pending');"

# PostgreSQL
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "INSERT INTO public.inventory (id,item_name,quantity,price,status) VALUES ($TestId,'CDC Test Item',10,111.25,'in_stock');"

# MongoDB
$mongoInsert = "db.products.insertOne({_id:'$MongoId',sku:'CDC-910001',name:'CDC Test Product',category:'Test',price:111.25,stock:10,status:'active',created_at:new Date()})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoInsert

# Oracle
@"
INSERT INTO debezium.transactions (txn_ref,account_id,amount,txn_type,status)
VALUES ('$OracleRef',99001,111.25,'CREDIT','PENDING');
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify the inserted state in Iceberg:

```powershell
docker exec trino trino --execute "SELECT * FROM iceberg.mysql_mydb.orders_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT * FROM iceberg.postgres_mydb_pg_public.inventory_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT * FROM iceberg.mongo_mydb_mongo.products_current WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT * FROM iceberg.oracle_xepdb1_debezium.transactions_current WHERE txn_ref='$OracleRef'"
```

Expected values: amount/price `111.25`, quantity/stock `10`, and one row in every query.

## 5. Test `u` — update

```powershell
# MySQL
docker exec mysql mysql -uroot -proot mydb -e `
  "UPDATE orders SET amount=222.50,status='shipped' WHERE id=$TestId;"

# PostgreSQL
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "UPDATE public.inventory SET quantity=20,price=222.50,status='reserved',updated_at=CURRENT_TIMESTAMP WHERE id=$TestId;"

# MongoDB
$mongoUpdate = "db.products.updateOne({_id:'$MongoId'},{`$set:{price:222.50,stock:20,status:'reserved'}})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoUpdate

# Oracle
@"
UPDATE debezium.transactions
SET amount=222.50,status='COMPLETED',updated_at=SYSTIMESTAMP
WHERE txn_ref='$OracleRef';
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify the updated state:

```powershell
docker exec trino trino --execute "SELECT id,amount,status FROM iceberg.mysql_mydb.orders_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT id,quantity,price,status FROM iceberg.postgres_mydb_pg_public.inventory_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT _id,price,stock,status FROM iceberg.mongo_mydb_mongo.products_current WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT id,txn_ref,amount,status FROM iceberg.oracle_xepdb1_debezium.transactions_current WHERE txn_ref='$OracleRef'"
```

Expected values: amount/price `222.50`, updated status, and still exactly one logical row per identifier.

## 6. Test `d` — delete

```powershell
# MySQL
docker exec mysql mysql -uroot -proot mydb -e `
  "DELETE FROM orders WHERE id=$TestId;"

# PostgreSQL
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "DELETE FROM public.inventory WHERE id=$TestId;"

# MongoDB
$mongoDelete = "db.products.deleteOne({_id:'$MongoId'})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoDelete

# Oracle
@"
DELETE FROM debezium.transactions WHERE txn_ref='$OracleRef';
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify all four records are absent:

```powershell
docker exec trino trino --execute "SELECT count(*) FROM iceberg.mysql_mydb.orders_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.postgres_mydb_pg_public.inventory_current WHERE id=$TestId"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.mongo_mydb_mongo.products_current WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.oracle_xepdb1_debezium.transactions_current WHERE txn_ref='$OracleRef'"
```

Expected: all four counts are `0`.

## 7. Inspect raw Kafka messages for `c/r/u/d`

List relevant topics:

```powershell
docker exec kafka-cdc kafka-topics --bootstrap-server kafka-cdc:9092 --list |
  Select-String -Pattern 'raw\.|schema-history\.raw'
```

Display every canonical operation found in each raw topic:
```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'            | Select-String -Pattern '"op"\s*:\s*"[crud]"'
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory'          | Select-String -Pattern '"op"\s*:\s*"[crud]"'
.\scripts\read-raw-topic.ps1 'raw.mongo.mydb_mongo.products'    | Select-String -Pattern '"op"\s*:\s*"[crud]"'
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS' | Select-String -Pattern '"op"\s*:\s*"[crud]"'
```

Display only events for the test identifiers:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'            | Select-String -SimpleMatch "$TestId"
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory'          | Select-String -SimpleMatch "$TestId"
.\scripts\read-raw-topic.ps1 'raw.mongo.mydb_mongo.products'    | Select-String -SimpleMatch "$MongoId"
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS' | Select-String -SimpleMatch "$OracleRef"
```

Check each topic's current end offset:

```powershell
@(
  'raw.mysql.mydb.orders',
  'raw.pg.public.inventory',
  'raw.mongo.mydb_mongo.products',
  'raw.oracle.DEBEZIUM.TRANSACTIONS'
) | ForEach-Object {
  docker exec kafka-cdc kafka-run-class kafka.tools.GetOffsetShell `
    --broker-list kafka-cdc:9092 --topic $_ --time -1
}
```

A console consumer without `--from-beginning` waits only for future records. `--timeout-ms 10000` is included so an idle topic returns after ten seconds instead of appearing stuck.

## 8. Select all Iceberg tables and inspect schemas

```powershell
docker exec trino trino --execute "SELECT * FROM iceberg.mysql_mydb.orders_current ORDER BY id"
docker exec trino trino --execute "SELECT * FROM iceberg.postgres_mydb_pg_public.inventory_current ORDER BY id"
docker exec trino trino --execute "SELECT * FROM iceberg.mongo_mydb_mongo.products_current ORDER BY _id"
docker exec trino trino --execute "SELECT * FROM iceberg.oracle_xepdb1_debezium.transactions_current ORDER BY id"

docker exec trino trino --execute "DESCRIBE iceberg.mysql_mydb.orders_current"
docker exec trino trino --execute "DESCRIBE iceberg.postgres_mydb_pg_public.inventory_current"
docker exec trino trino --execute "DESCRIBE iceberg.mongo_mydb_mongo.products_current"
docker exec trino trino --execute "DESCRIBE iceberg.oracle_xepdb1_debezium.transactions_current"
```

The final schemas must not contain `__op`, `before`, `after`, `source`, or `__deleted`.


## 9. Verify destination timestamp format

The sink normalizes configured timestamp fields to UTC ISO strings in every Iceberg table. Raw Kafka topics still keep the original Debezium logical timestamp representation.

```powershell
docker exec trino trino --execute "SELECT created_at,updated_at FROM iceberg.mysql_mydb.orders_current LIMIT 5"
docker exec trino trino --execute "SELECT created_at,updated_at FROM iceberg.postgres_mydb_pg_public.inventory_current LIMIT 5"
docker exec trino trino --execute "SELECT created_at FROM iceberg.mongo_mydb_mongo.products_current LIMIT 5"
docker exec trino trino --execute "SELECT created_at,updated_at FROM iceberg.oracle_xepdb1_debezium.transactions_current LIMIT 5"
```

Expected destination format:

```text
2026-07-29T09:51:08Z
```

If existing Iceberg tables were created before `IsoTimestampNormalizer` was enabled, reset/recreate the demo tables before expecting the physical column types to change.
## 10. Inspect Iceberg snapshot metadata

```powershell
function Show-IcebergSnapshots([string]$Schema, [string]$Table) {
  $sql = "SELECT snapshot_id,committed_at,operation,`n" +
    "element_at(summary,'task.engine') AS task_engine,`n" +
    "element_at(summary,'consumer.typeingest') AS type_ingest,`n" +
    "element_at(summary,'consumer.connectorname') AS connector_name,`n" +
    "element_at(summary,'consumer.ingest.time') AS ingest_time,`n" +
    "element_at(summary,'consumer.vtts.time') AS vtts_time,`n" +
    "element_at(summary,'cdcID') AS cdc_id,`n" +
    "element_at(summary,'pipeline.source-type') AS old_source_type`n" +
    "FROM iceberg.$Schema.\`"$Table\`$snapshots\`" ORDER BY committed_at DESC LIMIT 10"
  docker exec trino trino --output-format ALIGNED --execute $sql
}

Show-IcebergSnapshots 'mysql_mydb' 'orders_current'
Show-IcebergSnapshots 'mysql_mydb' 'customers_current'
Show-IcebergSnapshots 'postgres_mydb_pg_public' 'inventory_current'
Show-IcebergSnapshots 'mongo_mydb_mongo' 'products_current'
Show-IcebergSnapshots 'oracle_xepdb1_debezium' 'transactions_current'
```

Expected custom metadata:

- `task_engine = consumer`
- `type_ingest = CDC`
- connector name starts with `iceberg-sink-raw-`
- `ingest_time` and `vtts_time` are populated on normal commits
- `cdc_id` and `old_source_type` are `NULL`/blank because they must not be written into snapshot summaries

## 11. Recovery test

```powershell
docker compose restart connect

# Kafka Connect scans plugins during startup; wait until the REST API returns.
do {
  Start-Sleep -Seconds 5
  try { $ready = (Invoke-WebRequest 'http://127.0.0.1:8083/connectors' -UseBasicParsing).StatusCode -eq 200 }
  catch { $ready = $false }
} until ($ready)

$status = Invoke-RestMethod 'http://127.0.0.1:8083/connectors?expand=status'
$status | ConvertTo-Json -Depth 8
```

Run the four final `SELECT count(*)` queries again. Counts and logical rows must remain unchanged; no snapshot replay should create duplicate logical records.

## 12. Verify the multi-table CDC fan-out rule

The MySQL example deliberately captures two tables with one Debezium source connector:

| Source connector | Source table | Kafka topic | Sink connector | Iceberg table |
|---|---|---|---|---|
| `debezium-mysql-raw-source` | `mydb.orders` | `raw.mysql.mydb.orders` | `iceberg-sink-raw-mysql-orders` | `iceberg.mysql_mydb.orders_current` |
| `debezium-mysql-raw-source` | `mydb.customers` | `raw.mysql.mydb.customers` | `iceberg-sink-raw-mysql-customers` | `iceberg.mysql_mydb.customers_current` |

Confirm that the one source connector captures both tables:

```powershell
(Invoke-RestMethod `
  'http://127.0.0.1:8083/connectors/debezium-mysql-raw-source/config').'table.include.list'
```

Expected:

```text
mydb.orders,mydb.customers
```

Confirm the two topics, two sinks, and two destination tables:

```powershell
docker exec kafka-cdc kafka-topics --bootstrap-server kafka-cdc:9092 --list |
  Select-String -Pattern '^raw\.mysql\.mydb\.(orders|customers)$'

(Invoke-RestMethod 'http://127.0.0.1:8083/connectors') |
  Where-Object { $_ -match '^iceberg-sink-raw-mysql-(orders|customers)$' } |
  Sort-Object

docker exec trino trino --execute "SHOW TABLES FROM iceberg.mysql_mydb" |
  Select-String -Pattern 'orders_current|customers_current'
```

Run a complete `c/u/d` test for the second table. The first commit after auto-creating an Iceberg table can take longer than the normal commit interval, so the test uses a 35-second initial wait.

```powershell
$RuleTestId = Get-Random -Minimum 930000000 -Maximum 939999999
$RuleEmail = "cdc-rule-$RuleTestId@example.com"

docker exec mysql mysql -uroot -proot mydb -e `
  "INSERT INTO customers (id,full_name,email,status) VALUES ($RuleTestId,'CDC Rule Test','$RuleEmail','active');"
Start-Sleep -Seconds 35
docker exec trino trino --execute `
  "SELECT id,full_name,email,status FROM iceberg.mysql_mydb.customers_current WHERE id=$RuleTestId"

docker exec mysql mysql -uroot -proot mydb -e `
  "UPDATE customers SET full_name='CDC Rule Updated',status='verified' WHERE id=$RuleTestId;"
Start-Sleep -Seconds 25
docker exec trino trino --execute `
  "SELECT id,full_name,status FROM iceberg.mysql_mydb.customers_current WHERE id=$RuleTestId"

docker exec mysql mysql -uroot -proot mydb -e `
  "DELETE FROM customers WHERE id=$RuleTestId;"
Start-Sleep -Seconds 25
docker exec trino trino --execute `
  "SELECT count(*) FROM iceberg.mysql_mydb.customers_current WHERE id=$RuleTestId"

.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.customers' -TimeoutMs 5000 |
  Select-String -SimpleMatch "$RuleTestId"
```

Expected evidence:

- after INSERT, Trino returns one row and the raw event has `"op":"c"`;
- after UPDATE, Trino returns `CDC Rule Updated` / `verified` and the raw event has `"op":"u"`;
- after DELETE, Trino returns count `0` and the raw event has `"op":"d"`;
- `orders` and `customers` remain handled by the same source connector but isolated into separate topics, sink connectors, and Iceberg tables.

Inspect the destination snapshot lineage. `--%` is required here so Windows PowerShell does not consume `$snapshots` or the quoted Iceberg system-table name:

```powershell
docker exec --% trino trino --output-format ALIGNED --execute "SELECT operation, element_at(summary,'task.engine') AS task_engine, element_at(summary,'consumer.typeingest') AS type_ingest, element_at(summary,'consumer.connectorname') AS connector_name FROM iceberg.mysql_mydb.\"customers_current$snapshots\" ORDER BY committed_at DESC LIMIT 5"
```

Expected operations include `append`, `overwrite`, and `delete`; every row must show `consumer`, `CDC`, and `iceberg-sink-raw-mysql-customers`.
## 13. Verify independent current-state and raw append modes

Every raw topic has two independent consumer groups:

```text
raw topic -> iceberg-sink-*   -> *_current (materialized current state)
          -> iceberg-append-* -> *_cdc     (append-only raw history)
```

The `*_cdc` tables must contain exactly three columns:

```powershell
$rawTables = @(
  'mysql_mydb.orders_cdc',
  'mysql_mydb.customers_cdc',
  'postgres_mydb_pg_public.inventory_cdc',
  'mongo_mydb_mongo.products_cdc',
  'oracle_xepdb1_debezium.transactions_cdc'
)
foreach ($table in $rawTables) {
  docker exec trino trino --execute "DESCRIBE iceberg.$table"
}
```

Expected for every table:

```text
id              varchar
record          varchar
ngay_cap_nhat   varchar
```


Run a dual-mode MySQL test:

```powershell
$DualId = Get-Random -Minimum 940000000 -Maximum 949999999

docker exec mysql mysql -uroot -proot mydb -e `
  "INSERT INTO orders (id,customer_name,product,amount,status) VALUES ($DualId,'Dual Mode Test','Raw + Current',111.25,'pending');"
Start-Sleep -Seconds 30

docker exec trino trino --execute `
  "SELECT id,customer_name,amount,status FROM iceberg.mysql_mydb.orders_current WHERE id=$DualId"

docker exec trino trino --execute `
  "SELECT id,json_extract_scalar(record,'$.payload.op') op,ngay_cap_nhat FROM iceberg.mysql_mydb.orders_cdc WHERE coalesce(json_extract_scalar(record,'$.payload.after.id'),json_extract_scalar(record,'$.payload.before.id'))='$DualId'"

docker exec mysql mysql -uroot -proot mydb -e `
  "UPDATE orders SET amount=222.50,status='verified' WHERE id=$DualId; DELETE FROM orders WHERE id=$DualId;"
Start-Sleep -Seconds 30

docker exec trino trino --execute `
  "SELECT count(*) FROM iceberg.mysql_mydb.orders_current WHERE id=$DualId"

docker exec trino trino --execute `
  "SELECT json_extract_scalar(record,'$.payload.op') op,count(*) FROM iceberg.mysql_mydb.orders_cdc WHERE coalesce(json_extract_scalar(record,'$.payload.after.id'),json_extract_scalar(record,'$.payload.before.id'))='$DualId' GROUP BY 1 ORDER BY 1"
```

Expected final result: `orders_current` has zero rows for the deleted ID, while `orders_cdc` retains one `c`, one `u`, and one `d` event. Debezium tombstones have a null value and are intentionally skipped.

`record` is produced through `StringConverter`, so it is not parsed or reformatted. Its identity can be verified by comparing SHA-256 of a Kafka value with `to_hex(sha256(to_utf8(record)))` for the matching `topic-partition-offset` ID.
