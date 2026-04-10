    SELECT
        pr.repo_id,
        il.label_description,
        COUNT(*) AS count
    FROM
        pull_requests pr
    JOIN
        pull_request_labels il ON pr.pull_request_id = il.pull_request_id
    WHERE
        pr.pull_request_id IS NOT NULL
    GROUP BY
        pr.repo_id,
        il.label_description
    ORDER BY
        pr.repo_id,
        count DESC;
				
	select distinct label_description from 
	(    SELECT
        pr.repo_id,
        il.label_description,
        COUNT(*) AS count
    FROM
        pull_requests pr
    JOIN
        pull_request_labels il ON pr.pull_request_id = il.pull_request_id
    WHERE
        pr.pull_request_id IS NOT NULL
    GROUP BY
        pr.repo_id,
        il.label_description
    ORDER BY
        pr.repo_id,
        count DESC); 