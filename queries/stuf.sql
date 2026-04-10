select repo_id, count(*)
 from commits 
 group by repo_id; ---103204 103246

select repo_id, count(*) 
from pull_requests 
group by repo_id; --76196 76197 76271 76281 76317 76318 76320 76323 77346 77542 77719 77720

select repo_id, count(*) 
from issues 
group by repo_id; --48116 48141 48156 48200 48203 48206 48217 48775 48891 48973

select repo_id, count(*) 
from pull_request_reviews 
group by repo_id;-- 46403 46440 46470 46546 48564 46590 46592 46613 48660 46958 46970 46975 47309 47379 47672 47685

select sum(count) from 
(
select repo_id, count(*) from pull_request_reviews group by repo_id
); --25,319,769 25,320,102 25,320,330 25,320,360. 25,320,456

select cntrb_id, count(*) as contributor_repo_count 
from contributor_repo
group by cntrb_id; -- 1,080,201 1,083,293 1,083,595 1,084,555

select cntrb_id, count(*) as contributor_repo_count 
from contributor_repo 
where data_collection_date >= now() - interval '1 days'
group by cntrb_id; -- 4893 16:45 5/25; 14,687 10:06 5/29 14,874 13,206

select a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight, now() - max(a.data_collection_date) as recency 
from pull_request_reviews a, repos b, aveloxis_ops.collection_status c  
where a.repo_id=b.repo_id and a.repo_id=c.repo_id and b.repo_id=c.repo_id 
group by a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight 
order by secondary_data_last_collected, recency desc;  --46403  46440 46470 46546 45564 45583 46590 46592 46613 48660 46958 46970 46975 47309 47379 47885

select a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight, now() - max(a.data_collection_date) as recency 
from pull_request_files a, repos b, aveloxis_ops.collection_status c  
where a.repo_id=b.repo_id and a.repo_id=c.repo_id and b.repo_id=c.repo_id 
group by a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight 
order by secondary_data_last_collected, recency desc;  --75644 75647 75662 75667 75670 75694 75785 76148 76161 76162 76166 76715 76820 77198 77205 77206 

select a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight, now() - max(a.data_collection_date) as recency 
from pull_request_commits a, repos b, aveloxis_ops.collection_status c  
where a.repo_id=b.repo_id and a.repo_id=c.repo_id and b.repo_id=c.repo_id 
group by a.repo_id, b.repo_git, c.secondary_data_last_collected, c.secondary_status, c.secondary_weight 
order by secondary_data_last_collected, recency desc;  --75802 75803 75820 75827 75830 75855 75900 76237 76252 76253 76256 76806 76919  77288

select a.repo_id, b.repo_git, c.core_data_last_collected, c.core_status, c.core_weight, now() - max(a.data_collection_date) as recency 
from pull_requests a, repos b, aveloxis_ops.collection_status c  
where a.repo_id=b.repo_id and a.repo_id=c.repo_id and b.repo_id=c.repo_id 
group by a.repo_id, b.repo_git, c.core_data_last_collected, c.core_status, c.core_weight 
order by core_data_last_collected, recency desc; --76318  76318 76320 76323 77346 77542 77719 77720

select b.repo_git, a.* from aveloxis_ops.collection_status a, repos b 
where a.repo_id=b.repo_id and (core_status='Collecting' or secondary_status='Collecting' or facade_status='Collecting');  --105813 105908

select * from aveloxis_ops.collection_status order by ml_data_last_collected; 

/*
update aveloxis_ops.collection_status set core_status='Success' where core_status='Ignore' and core_data_last_collected is not null ;
update aveloxis_ops.collection_status set secondary_status='Success' where secondary_status='Ignore' and secondary_data_last_collected is not null ;
update aveloxis_ops.collection_status set facade_status='Success' where facade_status='Ignore' and facade_data_last_collected is not null ;
*/
select count(*) from pull_request_files; --189,095,572 189,882,171 191,551,306 191,582,676 191,853,511 191,966,120 192,665,383 192,665,383 
select count(*) from pull_request_commits; ---51,134,471 51,134,503 51,134,591 51,134,704 51,171,757 51,232,226 51,233,677 51,233,900 51,234,055

select * from 
(
select repo_git, now()-core_data_last_collected as oldness, core_status  from aveloxis_ops.collection_status, repos
where repos.repo_id=aveloxis_ops.collection_status.repo_id
) 
where oldness > INTERVAL '60 days' 
order by oldness desc; --405 401 352 213 42 40 38 42

select * from 
(
select now()-secondary_data_last_collected as oldness, secondary_status from aveloxis_ops.collection_status
) 
where oldness > INTERVAL '60 days' and secondary_status != 'Error'
order by oldness desc;  --38,199 38,195 38,145 38,007 37,841 37,839 37,895 37,899 37,900 37,895 37,894 37,946 37,908 36,776 38,076 38,038 38,077
-- 65 in error 
-- 37,829 not in error 36,686

select * from 
(
select now()-facade_data_last_collected as oldness, facade_status from aveloxis_ops.collection_status
) 
where oldness > INTERVAL '60 days' and facade_status != 'Error'
order by oldness desc; --599 594 541 407 241 239 240 



select count(*) from messages; --56,591,463. 57,622,193 58,228,911 58,524,990 61,302,462 67,810,388 67,893,742 67,944,223 67,964,113 68,078,362 68,123,369 68,341,758 68,341,852 68,342,116

select count(*) from repos; --103866 103909 105813 105,908


--update aveloxis_ops.collection_status set secondary_weight = -52204938259 where secondary_data_last_collected is NULL; 

--update aveloxis_ops.collection_status set core_weight = -52204938259 where core_data_last_collected is NULL; 

-- select * from repos where repo_id = 224378