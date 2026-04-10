select a.repo_id, a.file_path, a.file_name, count(*) from (
select repo_id, file_path, file_name from repo_labor where lower(file_name) = 'funding.yml' or lower(file_name) = 'funding.yaml' 
order by repo_id ) a group by a.repo_id, a.file_path, a.file_name