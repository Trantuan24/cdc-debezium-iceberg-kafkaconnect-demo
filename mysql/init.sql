-- Debezium reads row changes from the binary log and takes a consistent
-- snapshot on first start. SELECT and LOCK TABLES are required for the
-- snapshot; the remaining grants are required for binlog streaming.
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- Create test table
USE mydb;

CREATE TABLE IF NOT EXISTS orders (
    id            INT          NOT NULL AUTO_INCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    product       VARCHAR(100) NOT NULL,
    amount        DECIMAL(10,2) NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
