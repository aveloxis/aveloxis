select repo_id, issue_comments/issue_count::float as comments_per_issue from 
(
        SELECT
            issues.repo_id, a.issue_count, count(*) as issue_comments
        FROM
            issues,
            issue_message_ref, 
						(select repo_id, count(*) as issue_count from issues group by repo_id) a 
        WHERE
            issues.repo_id = issue_message_ref.repo_id and 
						issues.issue_id = issue_message_ref.issue_id  and 
						a.repo_id = issues.repo_id
				group by issues.repo_id, a.issue_count
        order by repo_id); 