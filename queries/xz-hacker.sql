select * from contributors where lower(gh_login) = 'jiat75'; 

select * from pull_requests where author_id = '0104a6d6-a200-0000-0000-000000000000'; 

select repo_git, cntrb_category, count(*) as counter from 
(
select * from contributor_repo where cntrb_id = '0104a6d6-a200-0000-0000-000000000000'
) a group by a.repo_git, cntrb_category order by counter desc; 