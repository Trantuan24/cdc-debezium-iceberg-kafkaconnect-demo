# Raw CDC Debezium Messages

This file records the raw Kafka messages observed during the CDC test flow.
The raw topics keep the official Debezium operation codes:

- `c`: create / insert
- `r`: snapshot read
- `u`: update
- `d`: delete

Sink-side mapping to `I/U/D` is temporary and must not appear in these raw Kafka topics.

## Common Read Commands

Read MySQL raw topic:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'
```

Read PostgreSQL raw topic:

```powershell
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory'
```

Filter by test id:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'   | Select-String -SimpleMatch '910001'
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory' | Select-String -SimpleMatch '910001'
```

Filter by Debezium operation:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mysql.mydb.orders'   | Select-String -Pattern '"op"\s*:\s*"[crud]"'
.\scripts\read-raw-topic.ps1 'raw.pg.public.inventory' | Select-String -Pattern '"op"\s*:\s*"[crud]"'
```

## MySQL

Source table:

```text
mydb.orders
```

Raw Kafka topic:

```text
raw.mysql.mydb.orders
```

Test id:

```text
910001
```

### MySQL Insert: `op=c`

Command executed:

```powershell
docker exec mysql mysql -uroot -proot mydb -e `
  "INSERT INTO orders (id,customer_name,product,amount,status) VALUES ($TestId,'CDC Test','Keyboard',111.25,'pending');"
```

Kafka metadata:

```text
topic     = raw.mysql.mydb.orders
partition = 0
offset    = 7
op        = c
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": null,
  "after": {
    "id": 910001,
    "customer_name": "CDC Test",
    "product": "Keyboard",
    "amount": 111.25,
    "status": "pending",
    "created_at": "2026-07-29T08:56:00Z",
    "updated_at": "2026-07-29T08:56:00Z"
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "mysql",
    "name": "raw.mysql",
    "snapshot": "false",
    "db": "mydb",
    "table": "orders",
    "file": "mysql-bin.000001",
    "pos": 5931,
    "row": 0,
    "thread": 2175
  },
  "op": "c",
  "ts_ms": 1785315360789,
  "transaction": null
}
```

Notes:

- `before = null` because the row did not exist before the insert.
- `after` contains the inserted row.
- `snapshot = false` means this is realtime CDC from MySQL binlog.

### MySQL Update: `op=u`

Command executed:

```powershell
docker exec mysql mysql -uroot -proot mydb -e `
  "UPDATE orders SET amount=222.50,status='shipped' WHERE id=$TestId;"
```

Kafka metadata:

```text
topic     = raw.mysql.mydb.orders
partition = 0
offset    = 8
op        = u
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": {
    "id": 910001,
    "customer_name": "CDC Test",
    "product": "Keyboard",
    "amount": 111.25,
    "status": "pending",
    "created_at": "2026-07-29T08:56:00Z",
    "updated_at": "2026-07-29T08:56:00Z"
  },
  "after": {
    "id": 910001,
    "customer_name": "CDC Test",
    "product": "Keyboard",
    "amount": 222.50,
    "status": "shipped",
    "created_at": "2026-07-29T08:56:00Z",
    "updated_at": "2026-07-29T09:22:41Z"
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "mysql",
    "name": "raw.mysql",
    "snapshot": "false",
    "db": "mydb",
    "table": "orders",
    "file": "mysql-bin.000001",
    "pos": 6279,
    "row": 0,
    "thread": 2332
  },
  "op": "u",
  "ts_ms": 1785316961499,
  "transaction": null
}
```

Notes:

- `before` is the old row: `amount=111.25`, `status=pending`.
- `after` is the new row: `amount=222.50`, `status=shipped`.
- Offset increased from `7` to `8`, preserving CDC order.

### MySQL Delete: `op=d`

Command executed:

```powershell
docker exec mysql mysql -uroot -proot mydb -e `
  "DELETE FROM orders WHERE id=$TestId;"
```

Kafka metadata:

```text
topic     = raw.mysql.mydb.orders
partition = 0
offset    = 9
op        = d
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": {
    "id": 910001,
    "customer_name": "CDC Test",
    "product": "Keyboard",
    "amount": 222.50,
    "status": "shipped",
    "created_at": "2026-07-29T08:56:00Z",
    "updated_at": "2026-07-29T09:22:41Z"
  },
  "after": null,
  "source": {
    "version": "2.5.4.Final",
    "connector": "mysql",
    "name": "raw.mysql",
    "snapshot": "false",
    "db": "mydb",
    "table": "orders",
    "file": "mysql-bin.000001",
    "pos": 6665,
    "row": 0,
    "thread": 2399
  },
  "op": "d",
  "ts_ms": 1785317633999,
  "transaction": null
}
```

Tombstone record after delete:

```text
offset = 10
key    = {"payload":{"id":910001}}
value  = null
```

Notes:

- `before` is the last known row before delete.
- `after = null` because the row is deleted.
- The tombstone `value=null` is normal Debezium behavior for Kafka log compaction.

## PostgreSQL

Source table:

```text
mydb_pg.public.inventory
```

Raw Kafka topic:

```text
raw.pg.public.inventory
```

Test id:

```text
910001
```

### PostgreSQL Insert: `op=c`

Command executed:

```powershell
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "INSERT INTO public.inventory (id,item_name,quantity,price,status) VALUES ($TestId,'CDC Test Item',10,111.25,'in_stock');"
```

Kafka metadata:

```text
topic     = raw.pg.public.inventory
partition = 0
offset    = 7
op        = c
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": null,
  "after": {
    "id": 910001,
    "item_name": "CDC Test Item",
    "quantity": 10,
    "price": 111.25,
    "status": "in_stock",
    "created_at": 1785318004671095,
    "updated_at": 1785318004671095
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "postgresql",
    "name": "raw.pg",
    "snapshot": "false",
    "db": "mydb_pg",
    "schema": "public",
    "table": "inventory",
    "txId": 758,
    "lsn": 24340808,
    "sequence": "[\"24340520\",\"24340808\"]"
  },
  "op": "c",
  "ts_ms": 1785318005170,
  "transaction": null
}
```

Notes:

- `before = null` because the row did not exist before the insert.
- `after` contains the inserted row.
- PostgreSQL timestamp columns appear as Debezium `MicroTimestamp` integer values.
- `snapshot = false` means realtime CDC from PostgreSQL WAL.

### PostgreSQL Update: `op=u`

Command executed:

```powershell
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "UPDATE public.inventory SET quantity=20,price=222.50,status='reserved',updated_at=CURRENT_TIMESTAMP WHERE id=$TestId;"
```

Kafka metadata:

```text
topic     = raw.pg.public.inventory
partition = 0
offset    = 8
op        = u
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": {
    "id": 910001,
    "item_name": "CDC Test Item",
    "quantity": 10,
    "price": 111.25,
    "status": "in_stock",
    "created_at": 1785318004671095,
    "updated_at": 1785318004671095
  },
  "after": {
    "id": 910001,
    "item_name": "CDC Test Item",
    "quantity": 20,
    "price": 222.50,
    "status": "reserved",
    "created_at": 1785318004671095,
    "updated_at": 1785318198671515
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "postgresql",
    "name": "raw.pg",
    "snapshot": "false",
    "db": "mydb_pg",
    "schema": "public",
    "table": "inventory",
    "txId": 759,
    "lsn": 24342392,
    "sequence": "[\"24342104\",\"24342392\"]"
  },
  "op": "u",
  "ts_ms": 1785318199051,
  "transaction": null
}
```

Notes:

- `before` is the old row: `quantity=10`, `price=111.25`, `status=in_stock`.
- `after` is the new row: `quantity=20`, `price=222.50`, `status=reserved`.
- Offset increased from `7` to `8`, preserving CDC order.

### PostgreSQL Delete: `op=d`

Command executed:

```powershell
docker exec postgres-source psql -U postgres -d mydb_pg -c `
  "DELETE FROM public.inventory WHERE id=$TestId;"
```

Kafka metadata:

```text
topic     = raw.pg.public.inventory
partition = 0
offset    = 9
op        = d
```

Key:

```json
{
  "payload": {
    "id": 910001
  }
}
```

Value payload:

```json
{
  "before": {
    "id": 910001,
    "item_name": "CDC Test Item",
    "quantity": 20,
    "price": 222.50,
    "status": "reserved",
    "created_at": 1785318004671095,
    "updated_at": 1785318198671515
  },
  "after": null,
  "source": {
    "version": "2.5.4.Final",
    "connector": "postgresql",
    "name": "raw.pg",
    "snapshot": "false",
    "db": "mydb_pg",
    "schema": "public",
    "table": "inventory",
    "txId": 760,
    "lsn": 24343896,
    "sequence": "[\"24343608\",\"24343896\"]"
  },
  "op": "d",
  "ts_ms": 1785318427796,
  "transaction": null
}
```

Tombstone record after delete:

```text
offset = 10
key    = {"payload":{"id":910001}}
value  = null
```

Notes:

- `before` is the last known row before delete.
- `after = null` because the row is deleted.
- The tombstone `value=null` is normal Debezium behavior for Kafka log compaction.

## MongoDB

Source collection:

```text
mydb_mongo.products
```

Raw Kafka topic:

```text
raw.mongo.mydb_mongo.products
```

Test id:

```text
cdc-test-910001
```

Read MongoDB raw topic:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mongo.mydb_mongo.products'
```

Filter by test id:

```powershell
.\scripts\read-raw-topic.ps1 'raw.mongo.mydb_mongo.products' | Select-String -SimpleMatch 'cdc-test-910001'
```

### MongoDB Insert: `op=c`

Command executed:

```powershell
$mongoInsert = "db.products.insertOne({_id:'$MongoId',sku:'CDC-910001',name:'CDC Test Product',category:'Test',price:111.25,stock:10,status:'active',created_at:new Date()})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoInsert
```

Kafka metadata:

```text
topic     = raw.mongo.mydb_mongo.products
partition = 0
offset    = 7
op        = c
_id       = cdc-test-910001
```

Key:

```json
{
  "payload": {
    "id": "\"cdc-test-910001\""
  }
}
```

Value payload:

```json
{
  "before": null,
  "after": {
    "_id": "cdc-test-910001",
    "sku": "CDC-910001",
    "name": "CDC Test Product",
    "category": "Test",
    "price": 111.25,
    "stock": 10,
    "status": "active",
    "created_at": {
      "$date": 1785319269364
    }
  },
  "updateDescription": null,
  "source": {
    "version": "2.5.4.Final",
    "connector": "mongodb",
    "name": "raw.mongo",
    "snapshot": "false",
    "db": "mydb_mongo",
    "rs": "rs0",
    "collection": "products",
    "ord": 1,
    "wallTime": 1785319269386
  },
  "op": "c",
  "ts_ms": 1785319269918,
  "transaction": null
}
```

Notes:

- `before = null` because the document did not exist before insert.
- `after` contains the inserted document. In the raw MongoDB envelope, `after` is emitted as a JSON string; it is formatted above as JSON for readability.
- `updateDescription = null` because this is an insert.
- `snapshot = false` means realtime CDC from MongoDB change streams.

### MongoDB Update: `op=u`

Command executed:

```powershell
$mongoUpdate = "db.products.updateOne({_id:'$MongoId'},{`$set:{price:222.50,stock:20,status:'reserved'}})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoUpdate
```

Kafka metadata:

```text
topic     = raw.mongo.mydb_mongo.products
partition = 0
offset    = 8
op        = u
_id       = cdc-test-910001
```

Key:

```json
{
  "payload": {
    "id": "\"cdc-test-910001\""
  }
}
```

Value payload:

```json
{
  "before": {
    "_id": "cdc-test-910001",
    "sku": "CDC-910001",
    "name": "CDC Test Product",
    "category": "Test",
    "price": 111.25,
    "stock": 10,
    "status": "active",
    "created_at": {
      "$date": 1785319269364
    }
  },
  "after": {
    "_id": "cdc-test-910001",
    "sku": "CDC-910001",
    "name": "CDC Test Product",
    "category": "Test",
    "price": 222.5,
    "stock": 20,
    "status": "reserved",
    "created_at": {
      "$date": 1785319269364
    }
  },
  "updateDescription": {
    "removedFields": null,
    "updatedFields": {
      "price": 222.5,
      "status": "reserved",
      "stock": 20
    },
    "truncatedArrays": null
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "mongodb",
    "name": "raw.mongo",
    "snapshot": "false",
    "db": "mydb_mongo",
    "rs": "rs0",
    "collection": "products",
    "ord": 1,
    "wallTime": 1785319405229
  },
  "op": "u",
  "ts_ms": 1785319405263,
  "transaction": null
}
```

Notes:

- `before` is the old document: `price=111.25`, `stock=10`, `status=active`.
- `after` is the new document: `price=222.5`, `stock=20`, `status=reserved`.
- `updateDescription.updatedFields` lists the fields changed by MongoDB.
- Offset increased from `7` to `8`, preserving CDC order.

### MongoDB Delete: `op=d`

Command executed:

```powershell
$mongoDelete = "db.products.deleteOne({_id:'$MongoId'})"
docker exec mongodb mongosh mongodb://mongodb:27017/mydb_mongo --quiet --eval $mongoDelete
```

Kafka metadata:

```text
topic     = raw.mongo.mydb_mongo.products
partition = 0
offset    = 9
op        = d
_id       = cdc-test-910001
```

Key:

```json
{
  "payload": {
    "id": "\"cdc-test-910001\""
  }
}
```

Value payload:

```json
{
  "before": {
    "_id": "cdc-test-910001",
    "sku": "CDC-910001",
    "name": "CDC Test Product",
    "category": "Test",
    "price": 222.5,
    "stock": 20,
    "status": "reserved",
    "created_at": {
      "$date": 1785319269364
    }
  },
  "after": null,
  "updateDescription": null,
  "source": {
    "version": "2.5.4.Final",
    "connector": "mongodb",
    "name": "raw.mongo",
    "snapshot": "false",
    "db": "mydb_mongo",
    "rs": "rs0",
    "collection": "products",
    "ord": 1,
    "wallTime": 1785319531144
  },
  "op": "d",
  "ts_ms": 1785319531158,
  "transaction": null
}
```

Tombstone record after delete:

```text
offset = 10
key    = {"payload":{"id":"\"cdc-test-910001\""}}
value  = null
```

Notes:

- `before` is the last known document before delete.
- `after = null` because the document is deleted.
- `updateDescription = null` because delete is not a field update.
- The tombstone `value=null` is normal Debezium behavior for Kafka log compaction.
## Oracle

Source table:

```text
XEPDB1.DEBEZIUM.TRANSACTIONS
```

Raw Kafka topic:

```text
raw.oracle.DEBEZIUM.TRANSACTIONS
```

Test reference:

```text
CDC-TEST-910001
```

Primary key used by Kafka key:

```text
ID
```

Read Oracle raw topic:

```powershell
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS'
```

Filter by test reference:

```powershell
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS' | Select-String -SimpleMatch 'CDC-TEST-910001'
```

Filter by primary key:

```powershell
.\scripts\read-raw-topic.ps1 'raw.oracle.DEBEZIUM.TRANSACTIONS' | Select-String -SimpleMatch '1000024'
```

Note: during this run, `CDC-TEST-910001` existed twice because one insert was executed earlier to verify the SQL*Plus fix. The update and delete commands therefore affected two rows. The examples below use the second row, `ID=1000024`, as the main record.

### Oracle Insert: `op=c`

Command executed:

```powershell
@"
INSERT INTO debezium.transactions (txn_ref,account_id,amount,txn_type,status)
VALUES ('$OracleRef',99001,111.25,'CREDIT','PENDING');
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1
```

Kafka metadata:

```text
topic     = raw.oracle.DEBEZIUM.TRANSACTIONS
partition = 0
offset    = 8
op        = c
ID        = 1000024
TXN_REF   = CDC-TEST-910001
```

Key:

```json
{
  "payload": {
    "ID": 1000024
  }
}
```

Value payload:

```json
{
  "before": null,
  "after": {
    "ID": 1000024,
    "TXN_REF": "CDC-TEST-910001",
    "ACCOUNT_ID": 99001,
    "AMOUNT": 111.25,
    "TXN_TYPE": "CREDIT",
    "STATUS": "PENDING",
    "CREATED_AT": 1785319850737445,
    "UPDATED_AT": 1785319850737445
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "oracle",
    "name": "raw.oracle",
    "snapshot": "false",
    "db": "XEPDB1",
    "schema": "DEBEZIUM",
    "table": "TRANSACTIONS",
    "txId": "05000c00b9020000",
    "scn": "3107236",
    "commit_scn": "3107237",
    "rs_id": "0x000024.00004a1e.0010",
    "redo_thread": 1,
    "user_name": "DEBEZIUM"
  },
  "op": "c",
  "ts_ms": 1785319851343,
  "transaction": null
}
```

Notes:

- `before = null` because the row did not exist before insert.
- `after` contains the inserted Oracle row.
- `snapshot = false` means realtime CDC from Oracle redo/log mining.
- Kafka key is `ID`, not `TXN_REF`.

### Oracle Update: `op=u`

Command executed:

```powershell
@"
UPDATE debezium.transactions
SET amount=222.50,status='COMPLETED',updated_at=SYSTIMESTAMP
WHERE txn_ref='$OracleRef';
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1
```

Command result:

```text
2 rows updated.
Commit complete.
```

Kafka metadata for the main record:

```text
topic     = raw.oracle.DEBEZIUM.TRANSACTIONS
partition = 0
offset    = 10
op        = u
ID        = 1000024
TXN_REF   = CDC-TEST-910001
```

Key:

```json
{
  "payload": {
    "ID": 1000024
  }
}
```

Value payload:

```json
{
  "before": {
    "ID": 1000024,
    "TXN_REF": "CDC-TEST-910001",
    "ACCOUNT_ID": 99001,
    "AMOUNT": 111.25,
    "TXN_TYPE": "CREDIT",
    "STATUS": "PENDING",
    "CREATED_AT": 1785319850737445,
    "UPDATED_AT": 1785319850737445
  },
  "after": {
    "ID": 1000024,
    "TXN_REF": "CDC-TEST-910001",
    "ACCOUNT_ID": 99001,
    "AMOUNT": 222.50,
    "TXN_TYPE": "CREDIT",
    "STATUS": "COMPLETED",
    "CREATED_AT": 1785319850737445,
    "UPDATED_AT": 1785319973029135
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "oracle",
    "name": "raw.oracle",
    "snapshot": "false",
    "db": "XEPDB1",
    "schema": "DEBEZIUM",
    "table": "TRANSACTIONS",
    "txId": "04001800c5020000",
    "scn": "3107689",
    "commit_scn": "3107690",
    "rs_id": "0x000024.00004aed.00f0",
    "redo_thread": 1,
    "user_name": "DEBEZIUM"
  },
  "op": "u",
  "ts_ms": 1785319974137,
  "transaction": null
}
```

Notes:

- `before` is the old row: `AMOUNT=111.25`, `STATUS=PENDING`.
- `after` is the new row: `AMOUNT=222.50`, `STATUS=COMPLETED`.
- The same statement also produced `offset=9` for `ID=1000023`, because the `WHERE txn_ref='$OracleRef'` condition matched two rows.
- Both update events share the same `scn` and `commit_scn` because they belong to the same Oracle transaction.

### Oracle Delete: `op=d`

Command executed:

```powershell
@"
DELETE FROM debezium.transactions WHERE txn_ref='$OracleRef';
COMMIT;
EXIT;
"@ | docker exec -i oracle sqlplus -s debezium/dbz@//localhost:1521/XEPDB1
```

Command result:

```text
2 rows deleted.
Commit complete.
```

Kafka metadata for the main record:

```text
topic     = raw.oracle.DEBEZIUM.TRANSACTIONS
partition = 0
offset    = 13
op        = d
ID        = 1000024
TXN_REF   = CDC-TEST-910001
```

Key:

```json
{
  "payload": {
    "ID": 1000024
  }
}
```

Value payload:

```json
{
  "before": {
    "ID": 1000024,
    "TXN_REF": "CDC-TEST-910001",
    "ACCOUNT_ID": 99001,
    "AMOUNT": 222.50,
    "TXN_TYPE": "CREDIT",
    "STATUS": "COMPLETED",
    "CREATED_AT": 1785319850737445,
    "UPDATED_AT": 1785319973029135
  },
  "after": null,
  "source": {
    "version": "2.5.4.Final",
    "connector": "oracle",
    "name": "raw.oracle",
    "snapshot": "false",
    "db": "XEPDB1",
    "schema": "DEBEZIUM",
    "table": "TRANSACTIONS",
    "txId": "06000700b3020000",
    "scn": "3107944",
    "commit_scn": "3107945",
    "rs_id": "0x000024.00004b71.0010",
    "redo_thread": 1,
    "user_name": "DEBEZIUM"
  },
  "op": "d",
  "ts_ms": 1785320042017,
  "transaction": null
}
```

Tombstone record after delete:

```text
offset = 14
key    = {"payload":{"ID":1000024}}
value  = null
```

Notes:

- `before` is the last known row before delete.
- `after = null` because the row is deleted.
- The same statement also produced a delete event for `ID=1000023`.
- The tombstone `value=null` is normal Debezium behavior for Kafka log compaction.