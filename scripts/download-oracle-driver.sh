#!/usr/bin/env bash
# download-oracle-driver.sh
# Oracle không cho phép redistribute ojdbc.jar tự do.
# Script này hướng dẫn cách lấy driver và đặt vào đúng chỗ.
set -e

PLUGIN_DIR="kafka-connect/plugins/debezium-connector-oracle"
DRIVER_FILE="$PLUGIN_DIR/ojdbc11.jar"

echo "══════════════════════════════════════════════════════"
echo "  Oracle JDBC Driver Setup"
echo "══════════════════════════════════════════════════════"

if [ -f "$DRIVER_FILE" ]; then
  echo "✅ ojdbc11.jar đã có tại: $DRIVER_FILE"
  exit 0
fi

echo ""
echo "❌ Chưa có ojdbc11.jar. Cần download thủ công:"
echo ""
echo "  Option 1 – Maven (nếu đã có Maven + Oracle Maven repo access):"
echo "    mvn dependency:get -Dartifact=com.oracle.database.jdbc:ojdbc11:23.3.0.23.09"
echo ""
echo "  Option 2 – Download thủ công (khuyến nghị):"
echo "    1. Truy cập: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html"
echo "    2. Tải: ojdbc11.jar (Oracle Database 21c hoặc 23c)"
echo "    3. Copy vào: $PLUGIN_DIR/"
echo ""
echo "  Option 3 – Nếu đã có Oracle client cài trên máy:"
echo "    Tìm file ojdbc11.jar trong \$ORACLE_HOME/jdbc/lib/"
echo "    cp \$ORACLE_HOME/jdbc/lib/ojdbc11.jar $DRIVER_FILE"
echo ""
mkdir -p "$PLUGIN_DIR"
echo "📁 Thư mục $PLUGIN_DIR đã được tạo. Đặt ojdbc11.jar vào đó rồi chạy lại build-smt.sh"
exit 1
