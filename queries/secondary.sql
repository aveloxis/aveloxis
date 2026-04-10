update aveloxis_ops.collection_status
set secondary_weight = -60523872456
where repo_id in (
select repo_id from 
(
select repo_id, now()-secondary_data_last_collected as oldness, secondary_status from aveloxis_ops.collection_status
) 
where oldness > INTERVAL '70 days' and oldness < INTERVAL '150 days'
order by oldness desc); 



select now()-secondary_data_last_collected as oldness, secondary_status from aveloxis_ops.collection_status
where oldness > INTERVAL '60 days' 
order by oldness desc;