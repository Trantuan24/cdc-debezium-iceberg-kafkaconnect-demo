# Cơ chế hoạt động của CDC Pipeline (MySQL -> Iceberg)

Kiến trúc này là một hệ thống **CDC (Change Data Capture)**, với mục tiêu: *Phản ánh gần như realtime mọi thay đổi (INSERT/UPDATE/DELETE) từ MySQL sang Data Lakehouse (Iceberg) để phục vụ phân tích.*

Dữ liệu đi qua 4 trạm chính trong pipeline:

---

## Trạm 1: MySQL -> Debezium (Capture)
* **MySQL:** Có một file nhật ký gọi là `binlog` (binary log). Bất kỳ thao tác INSERT/UPDATE/DELETE nào trên database cũng được ghi lại vào đây.
* **Debezium Source Connector:** Đóng vai trò như một "camera giám sát" liên tục đọc `binlog`. 
* Khi có thay đổi (ví dụ UPDATE), Debezium chộp lấy sự kiện và đóng gói thành một message JSON cực kỳ chi tiết (gọi là Envelope). Message này chứa:
  * `before`: Dữ liệu trước khi đổi.
  * `after`: Dữ liệu sau khi đổi.
  * `op`: Loại thao tác (`c` = create/insert, `u` = update, `d` = delete).

## Trạm 2: Xử lý giữa đường (Kafka Connect SMTs)
Iceberg không hiểu được cấu trúc Envelope phức tạp của Debezium. Do đó, ngay trong bộ nhớ của Kafka Connect, trước khi đẩy vào Kafka topic, message phải đi qua 2 bộ lọc (SMT - Single Message Transform):

1. **`unwrap` (ExtractNewRecordState):** 
   * Lột bỏ cái vỏ Envelope của Debezium. 
   * Nó chỉ lấy phần data `after` (dữ liệu mới nhất) làm payload chính.
   * Kèm thêm 1 cột metadata tên là `__op` để lưu lại loại thao tác (vẫn giữ nguyên giá trị `c`, `u`, `d` của Debezium).

2. **`opMap` (DebeziumOpMapper - Custom Java SMT):**
   * *Tabular Iceberg Sink* bắt buộc cột đánh dấu CDC phải mang các giá trị chuẩn: `I` (Insert), `U` (Update), `D` (Delete).
   * SMT do chúng ta tự viết này làm nhiệm vụ "phiên dịch" cột `__op`: 
     * `c` -> `I`
     * `u` -> `U`
     * `d` -> `D`

👉 **Kết quả:** Một message phẳng (flat JSON), gọn gàng được đẩy vào Kafka topic `mysql.mydb.orders`.

## Trạm 3: Kafka -> Iceberg Sink -> MinIO (Storage)
* **Tabular Iceberg Sink:** Đọc data từ Kafka topic theo từng batch (vd: 10 giây/lần).
* **CDC Mode:** Nhờ cấu hình `"iceberg.tables.cdc-field": "__op"`, connector hiểu đây là luồng dữ liệu thay đổi (CDC).
  * Nếu `__op = "I"`: Ghi thành 1 file data (Parquet) bình thường.
  * Nếu `__op = "U"` hoặc `"D"`: Iceberg Sink sẽ sinh ra một file đặc biệt gọi là **Equality Delete File** dựa trên khóa chính (`id`). File này đánh dấu rằng bản ghi có `id` tương ứng ở các file cũ đã bị xóa hoặc thay thế.
* Toàn bộ các file Parquet này được lưu vật lý dưới **MinIO** (đóng vai trò như S3 object storage).

## Trạm 4: Hive Metastore & Trino (Query)
* **Hive Metastore:** Đóng vai trò như "danh bạ". Mỗi khi Iceberg Sink ghi xong file mới, nó báo cho Hive Metastore biết đường dẫn và cấu trúc mới nhất của bảng.
* **Trino (Query Engine):** Khi bạn thực hiện câu lệnh `SELECT * FROM iceberg.default.orders_cdc`, Trino sẽ:
  1. Hỏi Hive Metastore để biết danh sách các file của bảng.
  2. Đọc các file Parquet từ MinIO.
  3. **Bước gộp dữ liệu (Merge-on-Read):** Trino tự động đối chiếu các file data gốc với các **Delete File** đã tạo ở Trạm 3. Nó lọc bỏ các dòng cũ, trộn lại và trả về cho bạn tập dữ liệu chính xác, cập nhật nhất giống hệt với trạng thái bên MySQL.
