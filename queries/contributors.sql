select * from contributors, contributor_repo
where contributors.cntrb_id=contributor_repo.cntrb_id 
and contributors.gh_login='sgoggins'
order by created_at; 


select repo_name, count(*) as counter from 
(
select * from contributors, contributor_repo
where contributors.cntrb_id=contributor_repo.cntrb_id 
and contributors.gh_login='sgoggins'
order by created_at) a 
group by a.repo_name; 
