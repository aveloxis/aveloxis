ALTER SYSTEM SET max_locks_per_transaction = 256;
ALTER SYSTEM SET max_pred_locks_per_transaction = 256;
ALTER SYSTEM SET lock_timeout = '180s';

-- Skipped for now because I think Augur holds connections longer. 
-- ALTER SYSTEM SET idle_in_transaction_session_timeout = '15min';
ALTER SYSTEM SET wal_buffers = '64MB';
ALTER SYSTEM SET wal_writer_delay = '200ms';
ALTER SYSTEM SET commit_delay = 100;
ALTER SYSTEM SET max_parallel_maintenance_workers = 4;
-- Make autovacuum more aggressive
ALTER SYSTEM SET autovacuum_max_workers = 6;
ALTER SYSTEM SET autovacuum_naptime = '30s';
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.1;
ALTER SYSTEM SET autovacuum_analyze_scale_factor = 0.05;
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = 2;
ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 1000;


------------------------------
--round TWO 
------------------------------

-- Ensure constraint exclusion is enabled
--ALTER SYSTEM SET constraint_exclusion = 'partition';

-- Enable partition-wise joins and aggregates (PG17 enhanced)
--ALTER SYSTEM SET enable_partitionwise_join = on;
ALTER SYSTEM SET enable_partitionwise_aggregate = on;

-- Increase parallel workers for partitioned tables
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 12;

-- Enable new PG17 incremental backup for better I/O
--ALTER SYSTEM SET wal_summarize = on;
ALTER SYSTEM SET summarize_wal = on;
-- Optimize checkpoint settings
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET min_wal_size = '1GB';

-- Use new vacuum acceleration
ALTER SYSTEM SET vacuum_buffer_usage_limit = '2GB';

-- Enable improved parallel query execution
ALTER SYSTEM SET parallel_leader_participation = on;
------------------------------
--round TWO 
------------------------------


