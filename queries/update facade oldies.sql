update aveloxis_ops.collection_status set facade_weight = -61187968749 where repo_id in 
(
select repo_id from 
(
select now()-facade_data_last_collected as oldness, facade_status from aveloxis_ops.collection_status
) 
where oldness > INTERVAL '60 days' and facade_status != 'Error'
order by oldness desc);  --599 594 541 407 241 239 240 