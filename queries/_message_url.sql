select data_collection_date from pull_requests where pr_comments_url is NULL
order by data_collection_date desc
; 