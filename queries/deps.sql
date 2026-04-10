SELECT
	repo_id,
	dep_name,
	counter 
FROM
	(
	SELECT
		aveloxis_data.repo_dependencies.dep_name,
		aveloxis_data.repo_dependencies.repo_id,
		COUNT ( * ) AS counter 
	FROM
		aveloxis_data.repo_dependencies 
	GROUP BY
		aveloxis_data.repo_dependencies.dep_name,
		aveloxis_data.repo_dependencies.repo_id 
	ORDER BY
		counter DESC 
	) A 
WHERE
	dep_name IN ( 'flask', 'requests', 'logging' ) 
ORDER BY
	repo_id;