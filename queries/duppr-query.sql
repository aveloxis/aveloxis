select * from pull_requests where pr_url='https://api.github.com/repos/tutors-sdk/tutors-time/pulls/5'; 

select * from repos where repo_git like '%tutors%'; 


select * from repos where repo_src_id is null order by repo_added desc; 