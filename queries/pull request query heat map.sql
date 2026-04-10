SELECT
	rl.repo_id AS ID,
	r.repo_name,
	r.repo_path,
	rl.rl_analysis_date,
	rl.file_path,
	rl.file_name 
FROM
	repo_labor rl,
	repos r 
WHERE
	rl.repo_id = r.repo_id 
	and rl.repo_id = 1
ORDER BY
	rl_analysis_date;
	
Select * from pull_request_files
where repo_id = 1; 

Select * from pull_request_files
where repo_id = 1; 

Select * from pull_request_files
where repo_id = 1
and pr_file_path like '%workers/%'
