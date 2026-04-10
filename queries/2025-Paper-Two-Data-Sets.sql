
-- Google 
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=25;
				

-- Microsoft 
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=17;
				
-- AWS 
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=250;
			

-- WHO 
select repo_id from aveloxis_ops.user_groups, aveloxis_ops.user_repos where 
        aveloxis_ops.user_groups.user_id = 2 and
        aveloxis_ops.user_repos.group_id=aveloxis_ops.user_groups.group_id 
        and 
        aveloxis_ops.user_groups.group_id=78;
				
