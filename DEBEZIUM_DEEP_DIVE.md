# Debezium – Tổng hợp kiến thức chi tiết

## 1. Debezium là gì?

**Debezium** là một nền tảng CDC (Change Data Capture) mã nguồn mở, chạy trên nền **Kafka Connect**.  
Nhiệm vụ cốt lõi: **theo dõi và phát ra mọi thay đổi dữ liệu (INSERT/UPDATE/DELETE) từ database nguồn thành các event trên Kafka**, gần như theo thời gian thực.

> 💡 Debezium KHÔNG query vào table. Nó đọc **transaction log nội bộ** của từng DB – đây là log DB tự ghi để đảm bảo ACID.

---

## 2. Debezium đọc gì từ mỗi loại DB?

### 2.1 MySQL → Binary Log (binlog)

**Cơ chế:** Debezium đăng ký như một **MySQL Replica (Slave)**, nhận binlog stream từ MySQL master.

**Yêu cầu bắt buộc trên MySQL:**
```ini
[mysqld]
server-id           = 1         # ID duy nhất của MySQL server
log_bin             = mysql-bin  # Bật binlog
binlog_format       = ROW        # Ghi theo từng row (không phải STATEMENT hay MIXED)
binlog_row_image    = FULL       # Ghi đầy đủ before + after của mỗi row
gtid_mode           = ON         # Global Transaction ID (để track vị trí đọc)
enforce_gtid_consistency = ON
```

**Quyền cần cấp cho user Debezium:**
```sql
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
```

**Config connector (project này):**
```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql",
  "database.port": "3306",
  "database.server.id": "184056"   // ID giả làm MySQL Slave
}
```

**Luồng:**
```
MySQL binlog (mysql-bin.000001, mysql-bin.000002, ...)
    │
    ▼ (Debezium đọc như MySQL Slave)
Change Event JSON (op: c/u/d/r)
    │
    ▼
Kafka Topic: mysql.mydb.orders
```

---

### 2.2 PostgreSQL → WAL (Write-Ahead Log) + Logical Replication

**Cơ chế:** PostgreSQL ghi WAL trước khi commit để đảm bảo không mất data. Debezium tạo **Logical Replication Slot** để nhận stream WAL được decode thành JSON.

**Yêu cầu bắt buộc trên PostgreSQL:**
```bash
# Bật wal_level=logical (trong project này set qua docker command)
postgres -c wal_level=logical
```

**Plugin decode WAL:**
| Plugin | Mô tả |
|--------|-------|
| `pgoutput` | Built-in từ PG 10+, không cần cài thêm ✅ (dùng trong project này) |
| `wal2json` | Cần cài extension |
| `decoderbufs` | Cần cài extension |

**Config connector (project này):**
```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres-source",
  "database.dbname": "mydb_pg",
  "plugin.name": "pgoutput"        // Plugin decode WAL
}
```

**Luồng:**
```
PostgreSQL WAL (pg_wal/)
    │
    ▼ (Logical Replication Slot + pgoutput plugin)
Change Event JSON (op: c/u/d/r)
    │
    ▼
Kafka Topic: pg.public.inventory
```

> ⚠️ **Lưu ý:** Replication Slot sẽ tích lũy WAL nếu Debezium bị dừng → có thể làm đầy disk. Cần monitor `pg_replication_slots`.

---

### 2.3 Oracle → Redo Log + LogMiner

**Cơ chế:** Oracle dùng **Redo Log** (tương tự binlog MySQL). Debezium dùng **Oracle LogMiner** – một công cụ built-in của Oracle – để parse Redo Log thành SQL statements, extract ra change events.

**Yêu cầu:**
- Oracle phải bật **Supplemental Logging**
- User cần quyền `EXECUTE_CATALOG_ROLE`, `SELECT ANY TRANSACTION`, v.v.
- Oracle Connector là bản **Debezium Community** (miễn phí nhưng cần Oracle license)

**Luồng:**
```
Oracle Redo Log (redo log files: redo01.log, redo02.log, ...)
    │
    ▼ (LogMiner API phân tích)
SQL statements → Debezium parse
    │
    ▼
Kafka Topic
```

> ⚠️ LogMiner cần được sizing và monitor redo/archive log cẩn thận. Debezium còn hỗ trợ OpenLogReplicator; adapter XStream yêu cầu Oracle GoldenGate license.

---

### 2.4 SQL Server → Transaction Log + Built-in CDC Feature

**Cơ chế:** SQL Server có **CDC built-in** – khi bật, SQL Server Agent tự động copy thay đổi từ Transaction Log vào các bảng staging đặc biệt (`cdc.<schema>_<table>_CT`). Debezium đọc từ các bảng này, không cần đọc raw log.

**Bật CDC trên SQL Server:**
```sql
-- Bật CDC cho database
EXEC sys.sp_cdc_enable_db;

-- Bật CDC cho từng bảng
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'orders',
    @role_name     = NULL;
```

**Luồng:**
```
Transaction Log
    │
    ▼ (SQL Server CDC Agent – chạy tự động)
cdc.dbo_orders_CT (staging table)
    │
    ▼ (Debezium đọc staging table)
Change Event JSON
    │
    ▼
Kafka Topic
```

---

### 2.5 MongoDB → Oplog (Operations Log)

**Cơ chế:** MongoDB Replica Set duy trì **Oplog**, nhưng Debezium không đọc trực tiếp `local.oplog.rs`. Connector mở **MongoDB Change Stream**; MongoDB chịu trách nhiệm đọc/giải mã oplog và trả về luồng thay đổi cho Debezium.

**Yêu cầu:**
```
MongoDB phải chạy ở chế độ Replica Set (dù chỉ 1 node).
Standalone KHÔNG có Oplog.
```

**Khởi tạo Replica Set (1 node):**
```javascript
rs.initiate()
```

**Luồng:**
```
MongoDB write operation
    │
    ▼ (ghi vào)
local.oplog.rs  { op: "i"/"u"/"d", ns: "mydb.orders", o: {...} }
    │
    ▼ (MongoDB Change Streams decode, Debezium consume)
Change Event JSON
    │
    ▼
Kafka Topic
```

---

## 3. Bảng so sánh tổng hợp

| Database | Log nguồn | Debezium đọc từ đâu | Yêu cầu đặc biệt |
|----------|-----------|---------------------|------------------|
| **MySQL** | Binary Log (binlog) | Giả làm MySQL Slave, đọc binlog stream | `binlog_format=ROW`, quyền REPLICATION |
| **PostgreSQL** | WAL (Write-Ahead Log) | Logical Replication Slot + pgoutput plugin | `wal_level=logical` |
| **Oracle** | Redo Log | LogMiner API | Supplemental Logging, Oracle license |
| **SQL Server** | Transaction Log | CDC staging tables (`cdc.*_CT`) | Bật SQL Server CDC Agent |
| **MongoDB** | Oplog (`local.oplog.rs`) | MongoDB Change Streams | Replica set hoặc sharded cluster |

---

## 4. Các loại event op của Debezium

| `op` | Tên đầy đủ | Khi nào xảy ra |
|------|-----------|----------------|
| `c` | **create** | Có INSERT mới vào DB (streaming phase) |
| `u` | **update** | Có UPDATE trong DB (streaming phase) |
| `d` | **delete** | Có DELETE trong DB (streaming phase) |
| `r` | **read** | Snapshot phase: Debezium đọc dữ liệu cũ đang có trong DB để đồng bộ lần đầu |

### `c` vs `r` – tại sao đều map thành `I` (Insert) trong Iceberg?

```
Timeline:

[Debezium khởi động]
        │
        ▼
[SNAPSHOT PHASE] ─── Iceberg đang RỖNG
        │   Đọc 1000 rows cũ trong MySQL
        │   → emit 1000 events op="r"
        │   → Iceberg nhận 1000 "I" → ghi vào
        │
        ▼ (snapshot xong)
[STREAMING PHASE] ─── Iceberg đã có 1000 rows
        │   User INSERT row 1001 → op="c" → Iceberg nhận "I" → thêm vào
        │   User UPDATE row 500  → op="u" → Iceberg nhận "U" → sửa
        │   User DELETE row 3    → op="d" → Iceberg nhận "D" → xoá
```

**Lý do `c` và `r` đều → `I`:**
- `c`: Row mới → chưa có trong Iceberg → phải **Insert**
- `r`: Row cũ đọc từ snapshot → Iceberg đang rỗng → cũng phải **Insert**  
- Cả hai đều là "thêm row vào Iceberg" dù nguồn gốc khác nhau

---

## 5. Cấu trúc Debezium Envelope (message thô)

Khi Debezium capture được 1 sự kiện, nó đóng gói thành **Envelope JSON**:

```json
{
  "before": {
    "id": 1,
    "customer_name": "Alice",
    "amount": 999.99
  },
  "after": {
    "id": 1,
    "customer_name": "Alice",
    "amount": 1199.99
  },
  "op": "u",
  "ts_ms": 1716000000000,
  "source": {
    "version": "2.5.4.Final",
    "connector": "mysql",
    "db": "mydb",
    "table": "orders",
    "server_id": 1,
    "file": "mysql-bin.000001",
    "pos": 12345
  }
}
```

| Field | Ý nghĩa |
|-------|---------|
| `before` | Dữ liệu row TRƯỚC khi thay đổi |
| `after` | Dữ liệu row SAU khi thay đổi |
| `op` | Loại thao tác: `c/u/d/r` |
| `ts_ms` | Timestamp của event |
| `source.file` | File binlog đang đọc (MySQL) |
| `source.pos` | Vị trí trong binlog |

---

## 6. SMT Chain trong project này (xử lý sau khi Debezium capture)

Envelope thô → không thể đẩy thẳng vào Iceberg → phải qua 3 SMT (Single Message Transform):

```
Debezium Envelope
    │
    ▼ [SMT 1] ExtractNewRecordState (unwrap)
    │  Lột vỏ Envelope, chỉ giữ "after" + thêm field __op="u"
    │
    ▼ [SMT 2] DebeziumOpMapper (custom Java)
    │  Map __op: "c"/"r"→"I", "u"→"U", "d"→"D"
    │
    ▼ [SMT 3] ReplaceField (dropFields)
    │  Xóa field __deleted thừa
    │
    ▼
Kafka Topic: mysql.mydb.orders
{
  "id": 1,
  "customer_name": "Alice",
  "amount": 1199.99,
  "__op": "U"          ← sạch, flat, Iceberg hiểu được
}
```

---

## 7. Tại sao Debezium đọc log thay vì query trực tiếp vào table?

| Cách | Vấn đề |
|------|--------|
| `SELECT * FROM table` định kỳ | Không biết row nào bị DELETE. Không có thông tin `before`. Chậm, tốn tài nguyên. |
| **Đọc transaction log** ✅ | Có đầy đủ `before/after/op`. Không tốn query. Gần realtime. Biết được DELETE. |

---

## 8. Tài liệu tham khảo

- [Debezium Official Docs](https://debezium.io/documentation/)
- [MySQL Connector Docs](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [PostgreSQL Connector Docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Tabular Iceberg Sink (now Databricks)](https://github.com/databricks/iceberg-kafka-connect)
- [ExtractNewRecordState SMT](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)
