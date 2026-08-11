# connector-cdc-service

Thư mục độc lập dùng để build Kafka Connect image cho luồng:

```text
MySQL / PostgreSQL / MongoDB / Oracle
  -> Debezium source
  -> Kafka raw topic
  -> Iceberg sink
```

Có thể copy riêng toàn bộ thư mục `connector-cdc-service` sang máy hoặc repository khác và build. Quá trình build không đọc file nào ở thư mục cha và không phụ thuộc phần demo local.

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

## Build độc lập

Chỉ cần đứng trong thư mục này:

```powershell
cd connector-cdc-service
docker build -t connector-cdc-service:1.0.0 .

docker tag connector-cdc-service:1.0.0 \
  duytuan24/connector-cdc-service:1.0.0

docker push duytuan24/connector-cdc-service:1.0.0

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