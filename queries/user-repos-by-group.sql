SELECT A.login_name,
	C.repo_id,
	b.group_id 
FROM
	aveloxis_ops.users A,
	aveloxis_ops.user_groups b,
	aveloxis_ops.user_repos C 
WHERE
	A.user_id = b.user_id 
	AND b.group_id = C.group_id 
	AND lower(A.login_name)='numfocus'
ORDER BY
	A.login_name,
	b.group_id;