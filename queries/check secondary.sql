
select * from 
(
select repo_id, now()-secondary_data_last_collected as oldness, secondary_status from aveloxis_ops.collection_status
) 
where oldness > INTERVAL '60 days' 
order by oldness desc;  --38,199 38,195 38,145 38,007 37,841 37,839 37,895 37,899 37,900