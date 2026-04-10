 select repo_git 
        from aveloxis_ops.collection_status x, repos y 
        where core_status='Success'
				and x.repo_id=y.repo_id
        and core_data_last_collected <= NOW() - INTERVAL '7 DAYS'
        order by core_data_last_collected 
        limit 1000000;
				
				
 select repo_git 
        from aveloxis_ops.collection_status x, repos y 
        where core_status='Success'
				and x.repo_id=y.repo_id
        and core_data_last_collected >= NOW() - INTERVAL '7 DAYS'
        order by core_data_last_collected 
        limit 1000000;
				
				
				select * from pull_requests where pr_url='https://api.github.com/repos/alphagov/ckanext-spatial/pulls/2';