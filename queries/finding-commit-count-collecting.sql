
Select repo_id, facade_status, commit_sum, sum(counter) as collected_commits 
from 
(
select a.repo_id, a.facade_status, b.cmt_commit_hash, 
a.commit_sum, count(*) as counter from 
 aveloxis_ops.collection_status a, 
 aveloxis_data.commits b 
where a.repo_id=b.repo_id 
and a.facade_status='Collecting' 
group by 
a.repo_id, a.facade_status,  b.cmt_commit_hash,  a.commit_sum
) ff
group by repo_id, facade_status, commit_sum, cmt_commit_hash 
order by commit_sum; 
