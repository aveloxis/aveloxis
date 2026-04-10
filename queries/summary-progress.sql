select distinct action from issue_events
order by action; 

SELECT
    * 
FROM
    (
        ( SELECT repo_id, issues_enabled, COUNT ( * ) AS meta_count 
        FROM repo_info 
        WHERE issues_count != 0 
        GROUP BY repo_id, issues_enabled 
        ORDER BY repo_id ) zz
        LEFT OUTER JOIN (
        SELECT A.repo_id,
            A.repo_name,
            b.issues_count,
            d.repo_id AS issue_repo_id,
            e.last_collected,
                        f.most_recently_collected_issue, 
            COUNT ( * ) AS issue_count,
            (
            b.issues_count - COUNT ( * )) AS issues_missing,
            ABS (
            CAST (( COUNT ( * )) +1 AS DOUBLE PRECISION )  / CAST ( b.issues_count + 1 AS DOUBLE PRECISION )) AS ratio_abs,
            (
            CAST (( COUNT ( * )) +1 AS DOUBLE PRECISION )  / CAST ( b.issues_count + 1 AS DOUBLE PRECISION )) AS ratio_issues 
        FROM
            aveloxis_data.repos A,
            aveloxis_data.issues d,
            aveloxis_data.repo_info b,
            ( SELECT repo_id, MAX ( data_collection_date ) AS last_collected FROM aveloxis_data.repo_info GROUP BY repo_id ORDER BY repo_id ) e, 
            ( SELECT repo_id, MAX ( data_collection_date ) AS most_recently_collected_issue FROM issues GROUP BY repo_id ORDER BY repo_id ) f 
        WHERE
            A.repo_id = b.repo_id 
                        AND lower(A.repo_git) like '%github.com%'
            AND A.repo_id = d.repo_id 
            AND b.repo_id = d.repo_id 
            AND e.repo_id = A.repo_id 
            AND b.data_collection_date = e.last_collected 
            -- AND d.issue_id IS NULL 
            AND f.repo_id = A.repo_id
                        and d.pull_request is NULL 
                        and b.issues_count is not NULL 
        GROUP BY
            A.repo_id,
            d.repo_id,
            b.issues_count,
            e.last_collected,
            f.most_recently_collected_issue 
        ORDER BY ratio_abs
        ) yy ON zz.repo_id = yy.repo_id 
    ) D 
        where d.issues_enabled = 'true';
-- order by most_recently_collected_issue desc 

-- CTE to limit repos to GitHub only
WITH github_repos AS (
    SELECT repo_id, repo_name, repo_git
    FROM aveloxis_data.repos
    WHERE LOWER(repo_git) LIKE '%github.com%'
),

-- Latest repo_info per repos
latest_repo_info AS (
    SELECT DISTINCT ON (repo_id)
        repo_id, data_collection_date AS last_collected, pr_count
    FROM aveloxis_data.repo_info
    ORDER BY repo_id, data_collection_date DESC
),

-- Latest pull request date per repos
latest_pr_info AS (
    SELECT DISTINCT ON (repo_id)
        repo_id, data_collection_date AS last_pr_collected
    FROM aveloxis_data.pull_requests
    ORDER BY repo_id, data_collection_date DESC
),

-- Aggregated pull request counts
pull_request_agg AS (
    SELECT
        pr.repo_id,
        COUNT(*) AS pull_requests_collected
    FROM aveloxis_data.pull_requests pr
    GROUP BY pr.repo_id
),

-- Combine metadata
metadata_summary AS (
    SELECT
        r.repo_id,
        r.repo_name,
        ri.pr_count,
        pr_agg.pull_requests_collected,
        ri.last_collected,
        pri.last_pr_collected,
        (ri.pr_count - COALESCE(pr_agg.pull_requests_collected, 0)) AS pull_requests_missing,
        ABS((COALESCE(pr_agg.pull_requests_collected, 0) + 1)::double precision / (ri.pr_count + 1)::double precision) AS ratio_abs,
        ((COALESCE(pr_agg.pull_requests_collected, 0) + 1)::double precision / (ri.pr_count + 1)::double precision) AS ratio_issues
    FROM github_repos r
    JOIN latest_repo_info ri ON r.repo_id = ri.repo_id
    LEFT JOIN latest_pr_info pri ON r.repo_id = pri.repo_id
    LEFT JOIN pull_request_agg pr_agg ON r.repo_id = pr_agg.repo_id
),

-- Summary of repos with nonzero PR counts
repo_summary AS (
    SELECT
        ri.repo_id,
        r.repo_name,
        MAX(ri.pr_count) AS max_pr_count,
        COUNT(*) AS meta_count
    FROM aveloxis_data.repo_info ri
    JOIN aveloxis_data.repos r ON r.repo_id = ri.repo_id
    WHERE ri.pr_count >= 1
    GROUP BY ri.repo_id, r.repo_name
)

-- Final joined result
SELECT *
FROM repo_summary rs
LEFT JOIN metadata_summary ms ON rs.repo_id = ms.repo_id
ORDER BY ratio_abs;
		

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
--		last_pr_collected desc;
		--pull_requests_missing desc; 		
        
select 'pull requets collected' as data_point, count(*) as count from pull_requests
union 
select data_point, sum(COUNT) 
from 
(
SELECT
    data_point, 
    SUM ( COUNT ) AS COUNT 
FROM
    (
    SELECT distinct on (repo_id) repo_id, 'pull request metadata count' as data_point, max(latest_collection_date) as maxdate,
        SUM ( pr_count ) AS COUNT 
    FROM
        ( SELECT distinct on (repo_id) repo_id, MAX( data_collection_date) as latest_collection_date, pr_count FROM aveloxis_data.repo_info GROUP BY repo_id, pr_count ORDER BY repo_id ) A 
    GROUP BY
        data_point, repo_id
		order by repo_id
    ) b group by data_point, repo_id, count, maxdate ) d group by data_point ;
		
	
		
select repo_id, pr_count, max(data_collection_date) from repo_info where pr_count=0
group by repo_id, pr_count
order by repo_id, pr_count asc; 

select count(*) from messages; 

select tool_source, count(*) as counter from messages
group by tool_source; 

select sum(refcount) as refcount from 
(
select count(*) as refcount, 'data_point' from pull_request_message_ref
union 
select count(*) as refcount, 'data_point' from pull_request_review_message_ref
union
select count(*) as refcount, 'data_point' from issue_message_ref
) a;

select count(*) as messagecount, 'data_point' from messages;
