select * from (
select platform_issue_id,  count(*) as counter from issues group by platform_issue_id order by counter desc) a, issues b 
where b.platform_issue_id = a.platform_issue_id order by a.counter desc; 

select platform_issue_id, count from (
(select platform_issue_id, repo_id, count(*) as counter from issues group by platform_issue_id, repo_id order by counter desc)) a repo b 
where 

select * from issues where platform_issue_id=434790456; 

select * from repos;