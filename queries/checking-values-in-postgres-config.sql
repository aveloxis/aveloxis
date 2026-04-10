
-- 1. LOCK MANAGEMENT PARAMETERS
-- ============================================
SELECT 
    name,
    setting,
    unit,
    category,
    short_desc
FROM pg_settings 
WHERE name IN (
    'max_locks_per_transaction',
    'max_pred_locks_per_transaction',
    'max_pred_locks_per_relation',
    'max_pred_locks_per_page',
    'lock_timeout',
    'idle_in_transaction_session_timeout'
)
ORDER BY name;

-- 2. WRITE PERFORMANCE PARAMETERS
-- ============================================
SELECT 
    name,
    setting,
    unit,
    CASE 
        WHEN unit = '8kB' THEN pg_size_pretty(setting::bigint * 8192)
        WHEN unit = 'kB' THEN pg_size_pretty(setting::bigint * 1024)
        WHEN unit = 'MB' THEN pg_size_pretty(setting::bigint * 1024 * 1024)
        WHEN unit = 'ms' THEN setting || 'ms'
        WHEN unit = 'min' THEN setting || ' minutes'
        WHEN unit = 's' THEN setting || ' seconds'
        ELSE setting || COALESCE(' ' || unit, '')
    END AS human_readable_value,
    category
FROM pg_settings 
WHERE name IN (
    'shared_buffers',
    'checkpoint_completion_target',
    'checkpoint_timeout',
    'max_wal_size',
    'min_wal_size',
    'wal_buffers',
    'wal_writer_delay',
    'commit_delay',
    'commit_siblings',
    'max_parallel_maintenance_workers'
)
ORDER BY name;

-- 3. COMBINED VIEW - ALL PARAMETERS WITH CURRENT VS RECOMMENDED
-- ============================================
WITH current_settings AS (
    SELECT 
        name,
        setting AS current_value,
        unit,
        CASE 
            WHEN unit = '8kB' THEN pg_size_pretty(setting::bigint * 8192)
            WHEN unit = 'kB' THEN pg_size_pretty(setting::bigint * 1024)
            WHEN unit = 'MB' THEN pg_size_pretty(setting::bigint * 1024 * 1024)
            WHEN unit = 'ms' THEN setting || 'ms'
            WHEN unit = 'min' THEN setting || ' minutes'
            WHEN unit = 's' THEN setting || ' seconds'
            ELSE setting || COALESCE(' ' || unit, '')
        END AS current_human_readable
    FROM pg_settings
),
recommended_settings AS (
    SELECT * FROM (VALUES
        -- Lock Management
        ('max_locks_per_transaction', '256'),
        ('max_pred_locks_per_transaction', '256'),
        ('max_pred_locks_per_relation', '-2'),
        ('max_pred_locks_per_page', '2'),
        ('lock_timeout', '10000'),  -- 10s in milliseconds
        ('idle_in_transaction_session_timeout', '300000'),  -- 5min in milliseconds
        -- Write Performance
        ('shared_buffers', '1048576'),  -- 8GB in 8kB blocks
        ('checkpoint_completion_target', '0.9'),
        ('checkpoint_timeout', '15'),  -- 15 minutes
        ('max_wal_size', '4096'),  -- 4GB in MB
        ('min_wal_size', '1024'),  -- 1GB in MB
        ('wal_buffers', '8192'),  -- 64MB in 8kB blocks
        ('wal_writer_delay', '200'),  -- 200ms
        ('commit_delay', '100'),  -- microseconds
        ('commit_siblings', '5'),
        ('max_parallel_maintenance_workers', '4')
    ) AS t(name, recommended_value)
)
SELECT 
    cs.name AS parameter,
    cs.current_value,
    cs.current_human_readable,
    rs.recommended_value,
    CASE 
        WHEN cs.current_value = rs.recommended_value THEN '✓ Already Set'
        ELSE '⚠ Different'
    END AS status,
    cs.unit
FROM current_settings cs
JOIN recommended_settings rs ON cs.name = rs.name
ORDER BY 
    CASE 
        WHEN cs.current_value != rs.recommended_value THEN 0 
        ELSE 1 
    END,
    cs.name;
		

-- Check for table bloat
SELECT 
  schemaname,
  relname,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS size,
  n_dead_tup,
  n_live_tup,
  round(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_percentage
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY n_dead_tup DESC;

select * from  pg_stat_user_tables; 