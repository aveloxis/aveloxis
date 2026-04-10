select r.repo_id, r.repo_name, r.repo_git, re.release_published_at
from repos r, releases re 
where r.repo_id = re.repo_id 
and r.repo_id=1 
and release_published_at is not NULL 
order by release_published_at