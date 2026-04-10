

SELECT
    * 
FROM
    (
    SELECT
        repo_info.repo_id,
        repos.repo_name,
        MAX ( pr_count ) AS max_pr_count,	
        COUNT ( * ) AS meta_count 
    FROM
        repo_info,
        repos -- WHERE issues_enabled = 'true' 
    WHERE
        pr_count >= 1
        AND repos.repo_id = repo_info.repo_id 
				and repo_info.repo_id in (40434,297005,131021,52647,132984)
    GROUP BY
        repo_info.repo_id,
        repos.repo_name 
    ORDER BY
        repo_info.repo_id,
        repos.repo_name 
    ) yy
    LEFT OUTER JOIN (
    SELECT A
        .repo_id,
        A.repo_name,	
                a.repo_git, 
        b.pr_count,
        d.repo_id AS pull_request_repo_id,
        e.last_collected,
        f.last_pr_collected,
        COUNT ( * ) AS pull_requests_collected,
        ( b.pr_count - COUNT ( * ) ) AS pull_requests_missing,
        ABS ( CAST ( ( COUNT ( * ) ) + 1 AS DOUBLE PRECISION ) / CAST ( b.pr_count + 1 AS DOUBLE PRECISION ) ) AS ratio_abs,
        ( CAST ( ( COUNT ( * ) ) + 1 AS DOUBLE PRECISION ) / CAST ( b.pr_count + 1 AS DOUBLE PRECISION ) ) AS ratio_issues 
    FROM
        aveloxis_data.repos A,
        aveloxis_data.pull_requests d,
        aveloxis_data.repo_info b,
        ( SELECT repo_id, MAX ( data_collection_date ) AS last_collected FROM aveloxis_data.repo_info GROUP BY repo_id ORDER BY repo_id ) e,
        ( SELECT repo_id, MAX ( data_collection_date ) AS last_pr_collected FROM aveloxis_data.pull_requests GROUP BY repo_id ORDER BY repo_id ) f 
    WHERE
        A.repo_id = b.repo_id 
        AND LOWER ( A.repo_git ) LIKE'%github.com%' 
        AND A.repo_id = d.repo_id 
        AND b.repo_id = d.repo_id 
        AND e.repo_id = A.repo_id 
        AND b.data_collection_date = e.last_collected 
        AND f.repo_id = A.repo_id -- AND d.pull_request_id IS NULL
				and a.repo_id in (40434,297005,131021,52647,132984)
    GROUP BY
        A.repo_id,
        d.repo_id,
        b.pr_count,
        e.last_collected,
        f.last_pr_collected 
    ORDER BY
        A.repo_id DESC 
    ) zz ON yy.repo_id = zz.repo_id 
ORDER BY
		ratio_abs; 