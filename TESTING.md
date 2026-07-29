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

Expected: all eight connectors and all eight tasks are `RUNNING`.

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
"INSERT INTO debezium.transactions (txn_ref,account_id,amount,txn_type,status) VALUES ('$OracleRef',99001,111.25,'CREDIT','PENDING'); COMMIT; EXIT;" |
  docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify the inserted state in Iceberg:

```powershell
docker exec trino trino --execute "SELECT * FROM iceberg.default.orders_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT * FROM iceberg.default.inventory_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT * FROM iceberg.default.products_cdc WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT * FROM iceberg.default.transactions_cdc WHERE txn_ref='$OracleRef'"
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
"UPDATE debezium.transactions SET amount=222.50,status='COMPLETED',updated_at=SYSTIMESTAMP WHERE txn_ref='$OracleRef'; COMMIT; EXIT;" |
  docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify the updated state:

```powershell
docker exec trino trino --execute "SELECT id,amount,status FROM iceberg.default.orders_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT id,quantity,price,status FROM iceberg.default.inventory_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT _id,price,stock,status FROM iceberg.default.products_cdc WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT id,txn_ref,amount,status FROM iceberg.default.transactions_cdc WHERE txn_ref='$OracleRef'"
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
"DELETE FROM debezium.transactions WHERE txn_ref='$OracleRef'; COMMIT; EXIT;" |
  docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1

Start-Sleep -Seconds 15
```

Verify all four records are absent:

```powershell
docker exec trino trino --execute "SELECT count(*) FROM iceberg.default.orders_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.default.inventory_cdc WHERE id=$TestId"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.default.products_cdc WHERE _id='$MongoId'"
docker exec trino trino --execute "SELECT count(*) FROM iceberg.default.transactions_cdc WHERE txn_ref='$OracleRef'"
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
docker exec trino trino --execute "SELECT * FROM iceberg.default.orders_cdc ORDER BY id"
docker exec trino trino --execute "SELECT * FROM iceberg.default.inventory_cdc ORDER BY id"
docker exec trino trino --execute "SELECT * FROM iceberg.default.products_cdc ORDER BY _id"
docker exec trino trino --execute "SELECT * FROM iceberg.default.transactions_cdc ORDER BY id"

docker exec trino trino --execute "DESCRIBE iceberg.default.orders_cdc"
docker exec trino trino --execute "DESCRIBE iceberg.default.inventory_cdc"
docker exec trino trino --execute "DESCRIBE iceberg.default.products_cdc"
docker exec trino trino --execute "DESCRIBE iceberg.default.transactions_cdc"
```

The final schemas must not contain `__op`, `before`, `after`, `source`, or `__deleted`.

## 9. Inspect Iceberg snapshot metadata

```powershell
function Show-IcebergSnapshots([string]$Table) {
  $sql = "SELECT snapshot_id,committed_at,operation,`n" +
    "element_at(summary,'task.engine') AS task_engine,`n" +
    "element_at(summary,'consumer.typeingest') AS type_ingest,`n" +
    "element_at(summary,'consumer.connectorname') AS connector_name,`n" +
    "element_at(summary,'consumer.ingest.time') AS ingest_time,`n" +
    "element_at(summary,'consumer.vtts.time') AS vtts_time,`n" +
    "element_at(summary,'cdcID') AS cdc_id,`n" +
    "element_at(summary,'pipeline.source-type') AS old_source_type`n" +
    "FROM iceberg.default.\`"$Table`$snapshots\`" ORDER BY committed_at DESC LIMIT 10"
  docker exec trino trino --output-format ALIGNED --execute $sql
}

Show-IcebergSnapshots 'orders_cdc'
Show-IcebergSnapshots 'inventory_cdc'
Show-IcebergSnapshots 'products_cdc'
Show-IcebergSnapshots 'transactions_cdc'
```

Expected custom metadata:

- `task_engine = consumer`
- `type_ingest = CDC`
- connector name starts with `iceberg-sink-raw-`
- `ingest_time` and `vtts_time` are populated on normal commits
- `cdc_id` and `old_source_type` are `NULL`/blank because they must not be written into snapshot summaries

## 10. Recovery test

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
