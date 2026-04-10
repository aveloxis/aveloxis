
SELECT C.repo_git,
		a.repo_id,
    split_part(regexp_replace(repo_git, '^https://github.com/', ''), '/', 1) AS organization,
    split_part(regexp_replace(repo_git, '^https://github.com/', ''), '/', 2) AS repository,
	b.pull_request_id,
	A.pr_number,
	COALESCE ( COUNT ( b.repo_id ), 0 ) AS comments 
FROM
	pull_requests A
	LEFT OUTER JOIN pull_request_message_ref b ON A.repo_id = b.repo_id 
	AND A.pull_request_id = b.pull_request_id
	LEFT OUTER JOIN repos C ON A.repo_id = C.repo_id 
WHERE
	A.repo_id = 52445 
	AND A.pull_request_id = 7507054 
GROUP BY
	C.repo_git,
	a.repo_id,
	b.pull_request_id,
	A.pr_number, 
	organization, 
	repository; 
	
	
	WITH url AS (
    SELECT 'https://github.com/vectordotdev/docker-client' AS github_url
)
SELECT 
    split_part(regexp_replace(github_url, '^https://github.com/', ''), '/', 1) AS organization,
    split_part(regexp_replace(github_url, '^https://github.com/', ''), '/', 2) AS repository
FROM url;
