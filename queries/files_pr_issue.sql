select * from repos where repo_git like '%bitnami/containers';

select * from pull_request_commits where repo_id = 138243
order by data_collection_date desc; 

select count(*) from pull_request_files where repo_id = 138243; 

select repo_id, counter from 
(
select repo_id, count(*) as counter from pull_request_files 
group by repo_id 
order by counter desc
) 
union all 
select repo_id, counter from (
select repo_id, count(*) as counter from pull_request_commits
group by repo_id 
order by counter desc)
order by counter
