select cntrb_id, count(*) as counter from contributors_aliases 
group by cntrb_id 
order by counter desc; 

Select * from contributors_aliases where 
cntrb_id = '01014c21-fe00-0000-0000-000000000000' 
order by data_collection_date desc; 