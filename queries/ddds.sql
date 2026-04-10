-- NOTE: This references an Augur materialized view not yet available in Aveloxis
select repo_id, month, year, count(*) as newbies from explorer_new_contributors 
group by repo_id, month, year 
order by repo_id, year desc, month desc; 