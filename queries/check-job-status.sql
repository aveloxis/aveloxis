        SELECT  
        user_id
        FROM aveloxis_ops.user_groups 
        JOIN aveloxis_ops.user_repos ON aveloxis_ops.user_groups.group_id = aveloxis_ops.user_repos.group_id
        JOIN aveloxis_data.repos ON aveloxis_ops.user_repos.repo_id = aveloxis_data.repos.repo_id
        JOIN aveloxis_ops.collection_status ON aveloxis_ops.user_repos.repo_id = aveloxis_ops.collection_status.repo_id
        WHERE facade_status='Update'
        GROUP BY user_id;buffers_backend_fsync
				
				select max(data_collection_date) from commits where repo_id=1; 
				
				select date(data_collection_date) as dater, count(*) from commits group by dater order by dater desc;