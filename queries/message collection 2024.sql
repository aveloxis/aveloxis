select repo_id, count(*) as counter 
from messages 
where repo_id = 123948
group by repo_id ;

select * from repos where repo_git like '%kubernetes/kubernetes'; 

update aveloxis_ops.collection_status set core_weight = -52204938288 where repo_id = 123948; 

select repo_id, message_url from (
select repo_id, pr_comments_url as message_url, 'pr' as type, created_at as creation_date, pr_number as gh_number
from pull_requests where repo_id=1 
and pr_state != 'open' --order by created_at desc, platform_pr_id desc
union 
select repo_id, comments_url as message_url, 'issue' as type, created_at as creation_date, issue_number as gh_number 
from issues where repo_id=1 
and issue_state != 'open' --order by created_at desc, issue_number desc; 
order by creation_date desc, gh_number desc); 

select repo_id, message_url from (
select repo_id, pr_comments_url as message_url, 'pr' as type, created_at as creation_date, pr_number as gh_number
from pull_requests where repo_id=1 
and pr_state != 'open' --order by created_at desc, platform_pr_id desc
union 
select repo_id, comments_url as message_url, 'issue' as type, created_at as creation_date, issue_number as gh_number 
from issues where repo_id=1 
and issue_state != 'open' --order by created_at desc, issue_number desc; 
order by creation_date desc, gh_number desc); 


/*

select pull_requests.repo_id, pr_comments_url as message_url, 'pr' as type, created_at as creation_date, pr_number as gh_number, count(*) as existing_messages 
from pull_requests, pull_request_message_ref 
where pull_requests.repo_id=1 
and pull_requests.repo_id=pull_request_message_ref.repo_id 
and pr_state != 'open' 
and pull_requests.pull_request_id=pull_request_message_ref.pull_request_id
group by pull_requests.repo_id, message_url, type, creation_date, gh_number, platform_pr_id order by created_at desc, platform_pr_id desc 



select pull_requests.repo_id, pr_comments_url as message_url, 'pr' as type, created_at as creation_date, pr_number as gh_number, count(*) as existing_messages 
from pull_requests
left outer join pull_request_message_ref  on pull_requests.repo_id=pull_request_message_ref.repo_id and pull_requests.pull_request_id=pull_request_message_ref.pull_request_id
where pull_requests.repo_id=1 
and pull_requests.pr_state != 'open' 
group by pull_requests.repo_id, message_url, type, creation_date, gh_number, platform_pr_id order by created_at desc, platform_pr_id desc 

*/
select pull_requests.repo_id, pr_comments_url as message_url, 'pr' as type, created_at as creation_date, pr_number as gh_number, coalesce(COUNT(pull_request_message_ref.msg_id))  as existing_messages 
from pull_requests
left outer join pull_request_message_ref  on pull_requests.repo_id=pull_request_message_ref.repo_id and pull_requests.pull_request_id=pull_request_message_ref.pull_request_id
where pull_requests.repo_id=1 
and pull_requests.pr_state != 'open' 
group by pull_requests.repo_id, message_url, type, creation_date, gh_number, platform_pr_id --order by created_at desc, platform_pr_id desc 
union 
select issues.repo_id, comments_url as message_url, 'issue' as type, created_at as creation_date, issue_number as gh_number, coalesce(COUNT(issue_message_ref.msg_id))  as existing_messages 
from issues
left outer join issue_message_ref  on issues.repo_id=issue_message_ref.repo_id and issues.issue_id=issue_message_ref.issue_id
where issues.repo_id=1 
and issues.issue_state != 'open' 
group by issues.repo_id, message_url, type, creation_date, gh_number, issue_number --order by created_at desc, issue_number desc 
order by creation_date desc, gh_number desc 