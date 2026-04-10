select repo_id, max(data_collection_date) as data_collection_date, count(*) 
from repo_dependencies group by repo_id order by data_collection_date desc; 
--60,817

select repo_id, max(data_collection_date) as data_collection_date, count(*) 
from repo_deps_libyear group by repo_id order by data_collection_date desc; 
--17,676

