SELECT summary FROM iceberg.default."orders_cdc$snapshots" ORDER BY committed_at DESC LIMIT 1;
