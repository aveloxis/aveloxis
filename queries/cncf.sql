SELECT distinct 
    substring(repo_git from 'github.com/([^/]+)/') as org_name
FROM
    repos
where repo_id in (  select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=166); -- sandbox 

SELECT distinct 
    substring(repo_git from 'github.com/([^/]+)/') as org_name
FROM
    repos
where repo_id in (select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=167); -- incubating  
				
				
SELECT distinct 
    substring(repo_git from 'github.com/([^/]+)/') as org_name
FROM
    repos
where repo_id in (select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=168); -- supported  

--195	2	cncf-graduated-orgs	f
--194	2	cncf-incubating-orgs	f
--193	2	cncf-sandbox-orgs	f
				
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=193;
				
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=194;
				
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=195;
				
				



        
        select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=166; -- sandbox 
        
        select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=167; -- incubating 
        
        select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=168; -- supported  
        

