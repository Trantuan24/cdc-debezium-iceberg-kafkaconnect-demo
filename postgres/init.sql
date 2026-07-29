-- Dedicated least-privilege Debezium account. POSTGRES_USER remains the
-- administrative account; the connector must not run as a superuser.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz';
GRANT CONNECT ON DATABASE mydb_pg TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    item_name VARCHAR(100),
    quantity INT,
    price NUMERIC(10, 2),
    status VARCHAR(50) DEFAULT 'in_stock',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE inventory REPLICA IDENTITY FULL;

GRANT SELECT ON TABLE public.inventory TO debezium;
GRANT USAGE, SELECT ON SEQUENCE public.inventory_id_seq TO debezium;

-- Pre-create the publication so Debezium does not need table ownership or
-- CREATE privileges. The connector configuration uses this exact name and
-- disables automatic publication creation.
CREATE PUBLICATION dbz_inventory_publication FOR TABLE public.inventory;

INSERT INTO inventory (item_name, quantity, price) VALUES
    ('MacBook Pro', 10, 1999.99),
    ('iPhone 15', 50, 999.50),
    ('AirPods', 100, 199.00);
