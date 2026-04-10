  SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid

    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.granted
	 order by blocking_pid;
	 
	 
	 SELECT a.datname,
         l.relation::regclass,
         l.transactionid,
         l.mode,
         l.GRANTED,
         a.usename,
         a.query,
         a.query_start,
         age(now(), a.query_start) AS "age",
         a.pid
FROM pg_stat_activity a 
JOIN pg_locks l ON l.pid = a.pid
where a.pid=1491488
ORDER BY a.pid, a.query_start;


SELECT aveloxis_data.repos.repo_id AS aveloxis_data_repo_repo_id, 
aveloxis_data.repos.repo_group_id AS aveloxis_data_repo_repo_group_id, 
aveloxis_data.repos.repo_git AS aveloxis_data_repo_repo_git, aveloxis_data.repos.repo_path AS aveloxis_data_repo_repo_path, 
aveloxis_data.repos.repo_name AS aveloxis_data_repo_repo_name, aveloxis_data.repos.repo_added AS aveloxis_data_repo_repo_added, aveloxis_data.repos.repo_type AS 
aveloxis_data_repo_repo_type, aveloxis_data.repos.url AS aveloxis_data_repo_url, aveloxis_data.repos.owner_id AS aveloxis_data_repo_owner_id, aveloxis_data.repos.description AS 
aveloxis_data_repo_description, aveloxis_data.repos.primary_language AS aveloxis_data_repo_primary_language, aveloxis_data.repos.created_at AS aveloxis_data_repo_created_at, 
aveloxis_data.repos.forked_from AS aveloxis_data_repo_forked_from, aveloxis_data.repos.updated_at AS aveloxis_data_repo_updated_at, aveloxis_data.repos.repo_archived_date_collected AS 
aveloxis_data_repo_repo_archived_date_collected, aveloxis_data.repos.repo_archived AS aveloxis_data_repo_repo_archived, aveloxis_data.repos.tool_source AS aveloxis_data_repo_tool_source, 
aveloxis_data.repos.tool_version AS aveloxis_data_repo_tool_version, aveloxis_data.repos.data_source AS aveloxis_data_repo_data_source, aveloxis_data.repos.data_collection_date AS aveloxis_data_repo_data_collection_date 
from repos; 


