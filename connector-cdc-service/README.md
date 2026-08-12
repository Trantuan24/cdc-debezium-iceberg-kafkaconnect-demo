# connector-cdc-service

## Overview

`connector-cdc-service` là Kafka Connect image độc lập cho pipeline CDC từ bốn
loại cơ sở dữ liệu vào Iceberg:

```text
MySQL / PostgreSQL / MongoDB / Oracle
  -> Debezium source
  -> Kafka raw topic
  -> custom SMT
  -> Iceberg sink (current-state hoặc append)
```

Image đã đóng gói sẵn:

- Kafka Connect `7.7.1`.
- Debezium `2.5.4.Final` cho MySQL, PostgreSQL, MongoDB và Oracle.
- Iceberg Sink Connector custom fork.
- Ba SMT: `DebeziumOpMapper`, `IsoTimestampNormalizer` và `RawAppendEnvelope`.

Đây là **một Docker image duy nhất**: `duytuan24/connector-cdc-service:1.1.0`.
Các mục dưới đây là những loại connector/plugin được đóng gói bên trong image,
không phải các image tách rời.

### MySQL CDC

- Connector: `io.debezium.connector.mysql.MySqlConnector`.
- Đọc MySQL binlog và phát các sự kiện insert, update, delete vào Kafka.
- Dùng cho các bảng nghiệp vụ đặt trong cấu hình MySQL source connector.

### PostgreSQL CDC

- Connector: `io.debezium.connector.postgresql.PostgresConnector`.
- Đọc PostgreSQL WAL thông qua logical replication rồi phát sự kiện vào Kafka.
- PostgreSQL phải được cấu hình replication slot và publication phù hợp.

### MongoDB CDC

- Connector: `io.debezium.connector.mongodb.MongoDbConnector`.
- Đọc MongoDB change stream/oplog và phát thay đổi document vào Kafka.
- Dữ liệu `before`/`after` có cấu trúc khác nhóm database quan hệ nhưng vẫn giữ
  nguyên raw Debezium value ở luồng append.

### Oracle CDC

- Connector: `io.debezium.connector.oracle.OracleConnector`.
- Đọc thay đổi Oracle bằng LogMiner và phát sự kiện vào Kafka.
- Image đã kèm `ojdbc11.jar`; database vẫn cần bật supplemental logging và cấp
  quyền CDC cho tài khoản connector.

### Iceberg Sink

- Connector: `io.tabular.iceberg.connect.IcebergSinkConnector`.
- Đọc record từ Kafka và ghi vào bảng Iceberg.
- Hỗ trợ hai cách sử dụng hiện tại: bảng current-state và bảng append lưu lịch sử
  sự kiện.

### Custom SMT

- `DebeziumOpMapper`: ánh xạ operation của Debezium cho luồng current-state.
- `IsoTimestampNormalizer`: chuẩn hóa timestamp trước khi ghi Iceberg.
- `RawAppendEnvelope`: tạo record append 9 trường và giữ nguyên raw Kafka value
  trong trường `data`.

`RawAppendEnvelope` tạo bản ghi append với contract hiện tại:

```text
loainguon      = "cdc"
manguondulieu  = topic-partition-offset
sukien         = insert / update / delete
phienban       = 1
body           = Kafka key (chuỗi từ StringConverter; không có key thì "")
header         = Kafka headers dạng JSON array; không có header thì ""
data           = raw Kafka value
ingest_date    = ngày ingest UTC
ingest_time    = thời điểm ingest
```

Thư mục này là build context hoàn chỉnh. Có thể copy riêng sang máy hoặc
repository khác để build; quá trình build không đọc file ở thư mục cha và không
phụ thuộc phần demo local. Database, Kafka broker, Hive Metastore và S3/MinIO
được cung cấp từ môi trường chạy, không nằm trong image.

## Cấu trúc

```text
connector-cdc-service/
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env.example
├── README.md
└── plugins/
    ├── debezium-connector-mysql/
    ├── debezium-connector-postgres/
    ├── debezium-connector-mongodb/
    ├── debezium-connector-oracle/
    ├── iceberg-kafka-connect/
    └── custom-smt/
```

## Plugin trong image

| Plugin | Chức năng |
|---|---|
| `debezium-connector-mysql` | CDC MySQL |
| `debezium-connector-postgres` | CDC PostgreSQL |
| `debezium-connector-mongodb` | CDC MongoDB |
| `debezium-connector-oracle` + `ojdbc11.jar` | CDC Oracle |
| `iceberg-kafka-connect` | Ghi current-state và raw append vào Iceberg |
| `custom-smt` | Map operation, chuẩn hóa timestamp và tạo raw envelope |

Đây là sáu nhóm plugin cần thiết cho luồng hiện tại. Không chứa database, Kafka broker, Hive Metastore, MinIO/S3, Trino hoặc connector config theo môi trường.

## Đánh giá connector-service tham khảo

Nguồn tham khảo:
[Trantuan24/kafka-to-iceberg-connector/connector-service](https://github.com/Trantuan24/kafka-to-iceberg-connector/tree/main/connector-service).

| Thành phần tham khảo | Quyết định |
|---|---|
| Một Kafka Connect worker độc lập | Giữ mô hình |
| Plugin nằm trong chính service | Giữ mô hình |
| Kafka Connect `8.0.3` | Không lấy; bản này dùng `7.7.1` đã test |
| Iceberg bundle của service tham khảo | Không trộn với bundle local đã test |
| `CustomCDCTransform` cho envelope API | Không dùng cho raw Debezium |
| Không có Debezium source plugin | Bổ sung đủ bốn loại DB |
| Credential cố định trong compose | Thay bằng `.env` hoặc secret runtime |

## Build và push image

Ví dụ dưới đây dùng Git Bash và phát hành version `1.1.0`:

```bash
cd /d/nifi-test/cdc-debezium-iceberg-kafkaconnect-demo/connector-cdc-service

IMAGE="duytuan24/connector-cdc-service"
VERSION="1.1.0"

docker build \
  --pull \
  --no-cache \
  -t "$IMAGE:$VERSION" \
  -t "$IMAGE:latest" \
  .

docker login
docker push "$IMAGE:$VERSION"
docker push "$IMAGE:latest"
```

Không dùng repository root làm build context.

## Chạy bằng Docker Compose

```powershell
cd connector-cdc-service
Copy-Item .env.example .env
# Sửa KAFKA_BOOTSTRAP_SERVERS và credential trong .env
docker compose --env-file .env build
docker compose --env-file .env up -d
```

Kafka, source DB, Hive Metastore và S3/MinIO là endpoint bên ngoài service. Nếu dùng hostname nội bộ Docker thì container này phải tham gia network có thể resolve các hostname đó.

## Xác minh plugin

Sau khi worker chạy:

```powershell
Invoke-RestMethod http://localhost:8083/connector-plugins |
  Select-Object class,type,version
```

Phải có tối thiểu:

```text
io.debezium.connector.mysql.MySqlConnector
io.debezium.connector.postgresql.PostgresConnector
io.debezium.connector.mongodb.MongoDbConnector
io.debezium.connector.oracle.OracleConnector
io.tabular.iceberg.connect.IcebergSinkConnector
```

Kết quả build đã kiểm tra:

```text
MySqlConnector       2.5.4.Final  source  loadable=true
PostgresConnector    2.5.4.Final  source  loadable=true
MongoDbConnector     2.5.4.Final  source  loadable=true
OracleConnector      2.5.4.Final  source  loadable=true
IcebergSinkConnector custom fork  sink    loadable=true
DebeziumOpMapper                         loadable=true
IsoTimestampNormalizer                  loadable=true
RawAppendEnvelope                       loadable=true
```

## Connector config

Connector JSON không thuộc image vì hostname, database, topic, Hive URI, S3 endpoint và credential thay đổi theo môi trường. Hệ thống triển khai cung cấp JSON rồi đăng ký qua Kafka Connect REST API:

```powershell
$doc = Get-Content C:\deploy\connectors\debezium-mysql-source.json -Raw |
  ConvertFrom-Json
$body = $doc.config | ConvertTo-Json -Depth 20
Invoke-RestMethod `
  -Method Put `
  -ContentType 'application/json' `
  -Uri "http://localhost:8083/connectors/$($doc.name)/config" `
  -Body $body
```

## Điều kiện production

- Đặt `CONNECT_INTERNAL_REPLICATION_FACTOR` theo số Kafka broker; thường là `3` ở production.
- Mỗi Connect cluster dùng `CONNECT_GROUP_ID` và ba internal topic riêng.
- Không đưa password DB, Kafka credential hoặc S3 secret vào image.
- Inject secret bằng cơ chế của môi trường triển khai.
- Backup internal config/offset/status topic trước khi thay cluster hoặc đổi group.
