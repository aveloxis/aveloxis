select count(*) from commits where cmt_ght_author_id is NULL; 
-- A commit author is the person who wrote the code 
-- the cmt_ght_author_id is a foreign key to the contributors table "cntrb_id" column 
-- repo_id = 1 is Augur on the public instance 

	select cmt_author_email, cmt_commit_hash, count(*) as counter 
	from commits where repo_id = 1 
	group by cmt_author_email, cmt_commit_hash
	order by counter desc; 
-- "commits" is a misnomer for the table name ... it actually contains commit_files
-- This shows me that s@goggins.com has the two most giant commits ... 

-- When did the giant commits happen? 
select cmt_author_email, cmt_commit_hash, cmt_author_date, count(*) as counter 
from commits where repo_id = 1 
group by cmt_author_email, cmt_commit_hash, cmt_author_date
order by counter desc; 

--Contributors: Table: contributors and contributors_aliases is a listing of all the secondary emails used to make commits
select a.cntrb_id, b.cntrb_id, b.alias_email 
from contributors a, contributors_aliases b 
where a.cntrb_id = b.cntrb_id; 

-- does not work yet. 
select contributors.cntrb_id from contributors 
left outer join contributors_aliases on contributors.cntrb_id; 


select repos.repo_id, repos.repo_name, releases.release_name, releases.release_created_at 
from repos, releases where repos.repo_id = releases.repo_id
and repos.repo_id = 1
order by release_created_at desc; 


select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
aveloxis_ops.user_groups.user_id = 2 and
aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
and 
user_groups.group_id=166; -- sandbox 

select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
aveloxis_ops.user_groups.user_id = 2 and
aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
and 
user_groups.group_id=167; -- incubating 


select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
aveloxis_ops.user_groups.user_id = 2 and
aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
and 
user_groups.group_id=168; -- supported  


select * from releases where repo_id in 
(select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
aveloxis_ops.user_groups.user_id = 2 and
aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
and 
user_groups.group_id=168 -- supported cncf projects); 
