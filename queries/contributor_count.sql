-- NOTE: This references an Augur materialized view not yet available in Aveloxis
select repo_id, count(*) as counter from 
(
select DISTINCT(cntrb_id) as cntrb_id, repo_id from explorer_contributor_actions
) a 
group by repo_id 
order by counter desc; 