SELECT snapshot_id, operation, summary FROM iceberg.default."inventory_cdc$snapshots" ORDER BY committed_at DESC LIMIT 1;
