
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
	
	select * from issues where repo_id = 52445;


SELECT 
	  x.repo_id, 
    comment_url,
    split_part(split_part(comment_url, '/repos/', 2), '/', 1) AS organization,
    split_part(split_part(comment_url, '/repos/', 2), '/', 2) AS repository,
    split_part(split_part(comment_url, '/issues/', 2), '/', 1) AS issue_number,
    theid, 
    thenumber, 
    COALESCE(COUNT(y.pull_request_id) + COUNT(z.issue_id), 0) AS comments 
FROM 
(
    SELECT repo_id, pr_comments_url AS comment_url ,pull_request_id AS theid, pr_number AS thenumber 
    FROM aveloxis_data.pull_requests 
    WHERE repo_id=52445 and pr_comments_url = 'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments'
    
    UNION
    
    SELECT repo_id, comments_url as comment_url, issue_id AS theid, issue_number AS thenumber 
    FROM aveloxis_data.issues 
    WHERE repo_id = 52445 and comments_url = 'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments'
) x 
LEFT JOIN aveloxis_data.pull_request_message_ref y ON x.theid = y.pull_request_id
LEFT JOIN aveloxis_data.issue_message_ref z ON x.theid = z.issue_id 
GROUP BY x.repo_id, comment_url, organization, repository, issue_number, theid, thenumber;


SELECT 
	  repo_id, 
    comment_url,
    split_part(split_part(comment_url, '/repos/', 2), '/', 1) AS organization,
    split_part(split_part(comment_url, '/repos/', 2), '/', 2) AS repository,
    split_part(split_part(comment_url, '/issues/', 2), '/', 1) AS issue_number, 
		COALESCE ( COUNT ( * ), 0 ) AS comments 
FROM 
	(
	(select repo_id, pr_comments_url AS comment_url from pull_requests WHERE repo_id=52445 order by created_at desc)
	UNION
	(select repo_id, comments_url as comment_url from issues WHERE repo_id=52445 order by created_at desc) 
	) a 
WHERE 
    comment_url = 'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments'
group by repo_id, comment_url, organization, repository, issue_number;


SELECT 
    substring(
        'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments' 
        FROM '/issues/([0-9]+)/comments'
    ) AS issue_number;
		
SELECT 
    substring(
        'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments' 
        FROM '/issues/([0-9]+)/comments'
    ) AS issue_number,
    position('microsoft' IN 'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments') AS microsoft_position,
    position('DeepGNN' IN 'https://api.github.com/repos/microsoft/DeepGNN/issues/105/comments') AS deepgnn_position;