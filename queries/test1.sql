select * from 
pull_requests a,
pull_request_message_ref b ,
messages c 
where 
a.repo_id=c.repo_id 
and 
a.repo_id=b.repo_id 
and 
b.msg_id=c.msg_id
and
a.pull_request_id=b.pull_request_id 
and
a.repo_id=1 
limit 100; 