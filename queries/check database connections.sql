SELECT pid, datname, usename, client_addr, application_name, backend_start, state, query_start, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = 'augur'
ORDER BY backend_start;

select query, wait_event, count(*) as total from 
(
SELECT pid, datname, usename, client_addr, application_name, backend_start, state, query_start, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = 'augur'
ORDER BY backend_start) a 
group by query, wait_event ; 

SELECT
    pid,
    datname,
    usename,
    client_addr,
    application_name,
    backend_start,
    state,
    query_start,
    wait_event_type,
    wait_event,
    query
FROM
    pg_stat_activity
WHERE
    datname = 'augur'
    --AND application_name LIKE '%augur%' -- Filter for your application
    AND state IN ('idle', 'idle in transaction', 'idle in transaction (aborted)')
    AND wait_event_type = 'Client'
    AND wait_event = 'ClientRead'
    -- Removed AND query = 'ROLLBACK' as it's unreliable for a persistent stuck state
    AND backend_start < NOW() - INTERVAL '5 minutes' -- Adjust this threshold carefully
    AND pid <> pg_backend_pid(); -- DO NOT TERMINATE YOUR OWN SESSION!; 
		
		
		SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE
    datname = 'augur'
    AND state = 'idle in transaction (aborted)'
    AND backend_start < NOW() - INTERVAL '5 minutes'
    AND pid <> pg_backend_pid();