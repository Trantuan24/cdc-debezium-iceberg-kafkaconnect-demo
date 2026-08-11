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
-- Seed one row so a fresh Debezium snapshot always exposes op=r for MySQL,
-- matching the seeded PostgreSQL, MongoDB, and Oracle demo sources.
INSERT INTO orders (id, customer_name, product, amount, status)
VALUES (1, 'Snapshot Seed', 'Demo Product', 10.00, 'seeded');
-- A second table captured by the same Debezium source connector. It proves the
-- table-level fan-out rule:
-- customers -> raw.mysql.mydb.customers -> its own sink -> customers_cdc.
CREATE TABLE IF NOT EXISTS customers (
    id            INT          NOT NULL AUTO_INCREMENT,
    full_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'active',
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_customers_email (email)
);

INSERT INTO customers (id, full_name, email, status)
VALUES (1, 'Snapshot Customer', 'snapshot.customer@example.com', 'seeded');
