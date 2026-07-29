
SELECT 'MySQL (orders_cdc)' as src, summary FROM iceberg.default."orders_cdc$snapshots" ORDER BY committed_at DESC LIMIT 1;
SELECT 'PostgreSQL (inventory_cdc)' as src, summary FROM iceberg.default."inventory_cdc$snapshots" ORDER BY committed_at DESC LIMIT 1;

