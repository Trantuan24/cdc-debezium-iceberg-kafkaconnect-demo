# CDC Pipeline – MySQL → Iceberg (CHEATSHEET)

## Flow
```
MySQL (binlog) → Debezium → Kafka topic: mysql.mydb.orders
  → SMT1: ExtractNewRecordState  (unwrap envelope)
  → SMT2: DebeziumOpMapper       (c/u/d/r → I/U/D)
  → Tabular Iceberg Sink v0.6.19
  → Iceberg table: default.orders_cdc  (MinIO + HiveCatalog)
  → Trino query
```

## Khác gì project cũ?
| | Old project | **Project này** |
|---|---|---|
| Source | Custom Python producer | **MySQL binlog** |
| SMT | CustomCDCTransform | **DebeziumOpMapper** (map op values) |
| Sink | Tabular v0.6.19 | Tabular v0.6.19 ✓ same |
| Hive/MinIO/Trino | ✓ | ✓ reuse y chang |

## Chạy lần đầu (3 bước)

```bash
# 1. Build SMT + copy Iceberg JARs từ old project
bash build-smt.sh

# 2. Spin up toàn bộ stack
docker-compose up --build -d

# 3. Đợi ~30s rồi register connectors
bash scripts/register-connectors.sh
```

## Verify

```bash
# Xem trạng thái connectors
curl http://localhost:8083/connectors/debezium-mysql-source/status | python3 -m json.tool
curl http://localhost:8083/connectors/iceberg-sink-mysql-orders/status | python3 -m json.tool

# Test INSERT/UPDATE/DELETE
python3 scripts/test_pipeline.py

# Query thẳng trên Trino
docker exec -it trino trino --execute "SELECT * FROM iceberg.default.orders_cdc"
```

## Ports
| Service | Port |
|---|---|
| MySQL | 3306 |
| Kafka | 29092 (external) / 9092 (internal) |
| Kafka Connect | **8083** |
| MinIO Console | **9001** |
| Trino | **8080** |
| Hive Metastore | 9083 |

## Khi cần reset

```bash
# Xóa connector để re-register
curl -X DELETE http://localhost:8083/connectors/debezium-mysql-source
curl -X DELETE http://localhost:8083/connectors/iceberg-sink-mysql-orders
bash scripts/register-connectors.sh

# Teardown hoàn toàn (xóa cả data)
docker-compose down -v
```

## File quan trọng
```
connectors/debezium-source.json   ← Debezium config (topic prefix, DB, table)
connectors/iceberg-sink.json      ← Iceberg sink config (catalog, bucket, cdc-field)
smt/src/.../DebeziumOpMapper.java ← SMT map c/u/d → I/U/D
mysql/init.sql                    ← Schema bảng orders
```

## Thêm bảng mới
1. Thêm `mydb.ten_bang` vào `table.include.list` trong `debezium-source.json`
2. Tạo sink config mới copy từ `iceberg-sink.json`, đổi `topics` và `iceberg.tables`
3. Re-register connector
