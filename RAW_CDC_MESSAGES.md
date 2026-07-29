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

TODO: append insert, update, and delete raw messages after running MongoDB CDC commands.

Expected raw topic:

```text
raw.mongo.mydb_mongo.products
```

## Oracle

TODO: append insert, update, and delete raw messages after running Oracle CDC commands.

Expected raw topic:

```text
raw.oracle.DEBEZIUM.TRANSACTIONS
```