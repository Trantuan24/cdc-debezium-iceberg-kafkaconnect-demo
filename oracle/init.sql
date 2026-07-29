-- Oracle XE 21c source initialization for Debezium LogMiner.
-- The container runs this script as SYSDBA on first database creation.

-- LogMiner requires ARCHIVELOG. The gvenzl/oracle-xe:21-slim image used by
-- this lab does not reliably honor ORACLE_ENABLE_ARCHIVELOG, so configure it
-- explicitly during first initialization.
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;

-- Database-level minimal supplemental logging is required. Do not enable ALL
-- columns globally; enable it only on captured tables to limit redo volume.
ALTER SESSION SET CONTAINER = CDB$ROOT;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Debezium must use a common user in a CDB/PDB deployment so it can mine redo
-- in CDB$ROOT and snapshot captured tables in XEPDB1.
CREATE USER c##dbzuser IDENTIFIED BY dbz CONTAINER=ALL;
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL;

-- Application data is owned by a separate local PDB schema. The connector
-- authenticates as C##DBZUSER, never as SYSTEM or as the application owner.
ALTER SESSION SET CONTAINER = XEPDB1;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=CURRENT;
ALTER USER c##dbzuser DEFAULT TABLESPACE USERS QUOTA 10M ON USERS CONTAINER=CURRENT;
CREATE USER debezium IDENTIFIED BY dbz
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO debezium;

CREATE TABLE debezium.transactions (
    id          NUMBER(18, 0) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txn_ref     VARCHAR2(50)   NOT NULL,
    account_id  NUMBER(18, 0)  NOT NULL,
    amount      NUMBER(15, 2)  NOT NULL,
    txn_type    VARCHAR2(20)   NOT NULL,
    status      VARCHAR2(20)   DEFAULT 'PENDING',
    created_at  TIMESTAMP      DEFAULT SYSTIMESTAMP,
    updated_at  TIMESTAMP      DEFAULT SYSTIMESTAMP
);

-- ALL-column table logging lets Debezium construct reliable before/after
-- states for updates and equality-key deletes.
ALTER TABLE debezium.transactions ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO debezium.transactions (txn_ref, account_id, amount, txn_type, status)
VALUES ('TXN-001', 1001, 500.00, 'CREDIT', 'COMPLETED');
INSERT INTO debezium.transactions (txn_ref, account_id, amount, txn_type, status)
VALUES ('TXN-002', 1002, 150.75, 'DEBIT', 'COMPLETED');
INSERT INTO debezium.transactions (txn_ref, account_id, amount, txn_type, status)
VALUES ('TXN-003', 1001, 200.00, 'DEBIT', 'PENDING');
COMMIT;

PROMPT 'Oracle initialized: LogMiner user C##DBZUSER and DEBEZIUM.TRANSACTIONS';
EXIT;