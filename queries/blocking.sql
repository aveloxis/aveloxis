SELECT bl.pid     AS blocked_pid,
     a.usename  AS blocked_user,
     a.query    AS blocked_statement
FROM  pg_catalog.pg_locks         bl
 JOIN pg_catalog.pg_stat_activity a  ON a.pid = bl.pid
WHERE NOT bl.granted
order by blocked_pid; 

--SELECT pg_cancel_backend(3520);

SELECT bl.pid     AS blocked_pid,
     a.usename  AS blocked_user,
     kl.pid     AS blocking_pid,
     ka.usename AS blocking_user,
     a.query    AS blocked_statement
FROM  pg_catalog.pg_locks         bl
 JOIN pg_catalog.pg_stat_activity a  ON a.pid = bl.pid	
 JOIN pg_catalog.pg_locks         kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid
 JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
WHERE NOT bl.granted
order by blocking_pid; 

select * from pg_stat_activity 
where pid=21580;
select pid, usename, pg_blocking_pids(pid) as blocked_by,
query as blocked_query
from pg_stat_activity 
where cardinality(pg_blocking_pids(pid)) > 0;

-- Recursive CTE to show chained lock dependencies
WITH RECURSIVE lock_chain AS (
    -- Base case: Find all direct blocking relationships
    SELECT 
        bl.pid AS blocked_pid,
        a.usename AS blocked_user,
        a.application_name AS blocked_app,
        kl.pid AS blocking_pid,
        ka.usename AS blocking_user,
        ka.application_name AS blocking_app,
        bl.relation::regclass AS locked_object,
        bl.mode AS requested_lock_mode,
        kl.mode AS blocking_lock_mode,
        a.query AS blocked_query,
        ka.query AS blocking_query,
        ka.state AS blocking_state,
        1 AS chain_level,
        ARRAY[kl.pid] AS chain_path,
        kl.pid AS root_blocker_pid,
        ka.usename AS root_blocker_user,
        NOW() - a.query_start AS blocked_duration,
        NOW() - ka.query_start AS blocking_duration
    FROM pg_catalog.pg_locks bl
    JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
    JOIN pg_catalog.pg_locks kl ON (
        -- Handle both transaction-level and object-level locks
        (kl.locktype = bl.locktype AND 
         kl.database IS NOT DISTINCT FROM bl.database AND
         kl.relation IS NOT DISTINCT FROM bl.relation AND
         kl.page IS NOT DISTINCT FROM bl.page AND
         kl.tuple IS NOT DISTINCT FROM bl.tuple AND
         kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid AND
         kl.transactionid IS NOT DISTINCT FROM bl.transactionid AND
         kl.classid IS NOT DISTINCT FROM bl.classid AND
         kl.objid IS NOT DISTINCT FROM bl.objid AND
         kl.objsubid IS NOT DISTINCT FROM bl.objsubid AND
         kl.pid != bl.pid)
    )
    JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
    WHERE NOT bl.granted AND kl.granted
    
    UNION ALL
    
    -- Recursive case: Find processes blocked by already blocked processes
    SELECT 
        bl.pid AS blocked_pid,
        a.usename AS blocked_user,
        a.application_name AS blocked_app,
        lc.blocked_pid AS blocking_pid,
        lc.blocked_user AS blocking_user,
        lc.blocked_app AS blocking_app,
        bl.relation::regclass AS locked_object,
        bl.mode AS requested_lock_mode,
        bl2.mode AS blocking_lock_mode,
        a.query AS blocked_query,
        lc.blocked_query AS blocking_query,
        a2.state AS blocking_state,
        lc.chain_level + 1 AS chain_level,
        lc.chain_path || bl.pid AS chain_path,
        lc.root_blocker_pid,
        lc.root_blocker_user,
        NOW() - a.query_start AS blocked_duration,
        lc.blocked_duration AS blocking_duration
    FROM lock_chain lc
    JOIN pg_catalog.pg_locks bl ON bl.pid != ALL(lc.chain_path)  -- Prevent cycles
    JOIN pg_catalog.pg_locks bl2 ON bl2.pid = lc.blocked_pid
    JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
    JOIN pg_catalog.pg_stat_activity a2 ON a2.pid = lc.blocked_pid
    WHERE NOT bl.granted 
    AND bl2.granted
    AND (
        -- Check if bl is blocked by bl2 (which is lc.blocked_pid)
        (bl2.locktype = bl.locktype AND 
         bl2.database IS NOT DISTINCT FROM bl.database AND
         bl2.relation IS NOT DISTINCT FROM bl.relation AND
         bl2.page IS NOT DISTINCT FROM bl.page AND
         bl2.tuple IS NOT DISTINCT FROM bl.tuple AND
         bl2.virtualxid IS NOT DISTINCT FROM bl.virtualxid AND
         bl2.transactionid IS NOT DISTINCT FROM bl.transactionid AND
         bl2.classid IS NOT DISTINCT FROM bl.classid AND
         bl2.objid IS NOT DISTINCT FROM bl.objid AND
         bl2.objsubid IS NOT DISTINCT FROM bl.objsubid)
    )
)

-- Main query with formatted output
SELECT 
    LPAD('', (chain_level - 1) * 2, ' ') || '→ ' || blocked_pid::text AS lock_chain_visualization,
    chain_level,
    blocked_pid,
    blocked_user,
    blocking_pid,
    blocking_user,
    root_blocker_pid,
    root_blocker_user,
    locked_object,
    requested_lock_mode,
    blocking_lock_mode,
    ARRAY_TO_STRING(chain_path || blocked_pid, ' → ') AS full_chain_path,
    TO_CHAR(blocked_duration, 'HH24:MI:SS') AS blocked_for,
    TO_CHAR(blocking_duration, 'HH24:MI:SS') AS blocker_active_for,
    LEFT(blocked_query, 50) || CASE WHEN LENGTH(blocked_query) > 50 THEN '...' ELSE '' END AS blocked_query_snippet,
    LEFT(blocking_query, 50) || CASE WHEN LENGTH(blocking_query) > 50 THEN '...' ELSE '' END AS blocking_query_snippet
FROM lock_chain
ORDER BY 
    root_blocker_pid,
    chain_level,
    chain_path,
    blocked_pid;

-- Complete query with both outputs in one execution
WITH RECURSIVE lock_chain AS (
    -- Base case: Find all direct blocking relationships
    SELECT 
        bl.pid AS blocked_pid,
        a.usename AS blocked_user,
        a.application_name AS blocked_app,
        kl.pid AS blocking_pid,
        ka.usename AS blocking_user,
        ka.application_name AS blocking_app,
        bl.relation::regclass AS locked_object,
        bl.mode AS requested_lock_mode,
        kl.mode AS blocking_lock_mode,
        a.query AS blocked_query,
        ka.query AS blocking_query,
        ka.state AS blocking_state,
        1 AS chain_level,
        ARRAY[kl.pid] AS chain_path,
        kl.pid AS root_blocker_pid,
        ka.usename AS root_blocker_user,
        NOW() - a.query_start AS blocked_duration,
        NOW() - ka.query_start AS blocking_duration
    FROM pg_catalog.pg_locks bl
    JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
    JOIN pg_catalog.pg_locks kl ON (
        (kl.locktype = bl.locktype AND 
         kl.database IS NOT DISTINCT FROM bl.database AND
         kl.relation IS NOT DISTINCT FROM bl.relation AND
         kl.page IS NOT DISTINCT FROM bl.page AND
         kl.tuple IS NOT DISTINCT FROM bl.tuple AND
         kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid AND
         kl.transactionid IS NOT DISTINCT FROM bl.transactionid AND
         kl.classid IS NOT DISTINCT FROM bl.classid AND
         kl.objid IS NOT DISTINCT FROM bl.objid AND
         kl.objsubid IS NOT DISTINCT FROM bl.objsubid AND
         kl.pid != bl.pid)
    )
    JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
    WHERE NOT bl.granted AND kl.granted
    
    UNION ALL
    
    -- Recursive case: Find processes blocked by already blocked processes
    SELECT 
        bl.pid AS blocked_pid,
        a.usename AS blocked_user,
        a.application_name AS blocked_app,
        lc.blocked_pid AS blocking_pid,
        lc.blocked_user AS blocking_user,
        lc.blocked_app AS blocking_app,
        bl.relation::regclass AS locked_object,
        bl.mode AS requested_lock_mode,
        bl2.mode AS blocking_lock_mode,
        a.query AS blocked_query,
        lc.blocked_query AS blocking_query,
        a2.state AS blocking_state,
        lc.chain_level + 1 AS chain_level,
        lc.chain_path || bl.pid AS chain_path,
        lc.root_blocker_pid,
        lc.root_blocker_user,
        NOW() - a.query_start AS blocked_duration,
        lc.blocked_duration AS blocking_duration
    FROM lock_chain lc
    JOIN pg_catalog.pg_locks bl ON bl.pid != ALL(lc.chain_path)
    JOIN pg_catalog.pg_locks bl2 ON bl2.pid = lc.blocked_pid
    JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
    JOIN pg_catalog.pg_stat_activity a2 ON a2.pid = lc.blocked_pid
    WHERE NOT bl.granted 
    AND bl2.granted
    AND (
        (bl2.locktype = bl.locktype AND 
         bl2.database IS NOT DISTINCT FROM bl.database AND
         bl2.relation IS NOT DISTINCT FROM bl.relation AND
         bl2.page IS NOT DISTINCT FROM bl.page AND
         bl2.tuple IS NOT DISTINCT FROM bl.tuple AND
         bl2.virtualxid IS NOT DISTINCT FROM bl.virtualxid AND
         bl2.transactionid IS NOT DISTINCT FROM bl.transactionid AND
         bl2.classid IS NOT DISTINCT FROM bl.classid AND
         bl2.objid IS NOT DISTINCT FROM bl.objid AND
         bl2.objsubid IS NOT DISTINCT FROM bl.objsubid)
    )
)
-- Tree visualization output
SELECT 
    STRING_AGG(
        CASE 
            WHEN chain_level = 1 THEN 'PID ' || blocking_pid || ' (' || blocking_user || ')'
            ELSE REPEAT('  ', chain_level - 1) || '└→ PID ' || blocked_pid || ' (' || blocked_user || ')'
        END,
        E'\n' ORDER BY chain_level, blocked_pid
    ) AS lock_chain_tree
FROM lock_chain
GROUP BY root_blocker_pid, root_blocker_user
ORDER BY root_blocker_pid;


-- Create alert for locks over 30 seconds
SELECT * FROM pg_stat_activity 
WHERE wait_event_type = 'Lock' 
  AND NOW() - query_start > interval '30 seconds';