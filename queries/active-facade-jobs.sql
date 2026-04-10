select * from aveloxis_ops.collection_status a, repos b 
where a.repo_id=b.repo_id 
and 
(a.facade_status='Collecting' or facade_data_last_collected is NULL 
) 
order by facade_status, commit_sum; 


SELECT
  a.repo_id,
  a.commit_sum, b.repo_git, 
  COALESCE(
    (
      SELECT COUNT(*)
      FROM (
        SELECT cmt_commit_hash
        FROM commits c
        WHERE c.repo_id = a.repo_id	
        GROUP BY cmt_commit_hash
      ) q
    ), 0
  ) AS commits_captured 
FROM aveloxis_ops.collection_status a
JOIN repos b ON b.repo_id = a.repo_id
WHERE a.facade_status = 'Collecting'
ORDER BY a.commit_sum;
