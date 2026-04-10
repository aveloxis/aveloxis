SELECT 
  blocked.pid AS blocked_pid,
  blocked.usename AS blocked_user,
  blocking.pid AS blocking_pid,
  blocking.usename AS blocking_user,
  blocked.wait_event_type,
  blocked.wait_event,
  blocked.query AS blocked_query,
  blocking.query AS blocking_query
FROM pg_stat_activity AS blocked
JOIN pg_stat_activity AS blocking 
  ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE blocked.wait_event_type IS NOT NULL;

select * from pg_stat_activity; 
