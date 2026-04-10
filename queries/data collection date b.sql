select  data_collection_date desc from commits where repo_id=1;


select  data_collection_date, count(*) as counter  from commits where repo_id=1 group by data_collection_date order by counter desc; 