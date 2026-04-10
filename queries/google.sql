SELECT A.login_name,
	C.repo_id, d.repo_git, 
	b.group_id, b."name"
FROM
	aveloxis_ops.users A,
	aveloxis_ops.user_groups b,
	aveloxis_ops.user_repos C,
	aveloxis_data.repos d
WHERE
	A.user_id = b.user_id 
	AND b.group_id = C.group_id 
	AND d.repo_id=c.repo_id 
	AND b.name='Google'
	AND login_name = 'sean'
ORDER BY
	A.login_name,
	b.group_id;


