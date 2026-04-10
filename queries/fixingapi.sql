-- Logic should count merging or clsoing as a response to avoid NULLS 
               SELECT
                    repos.repo_id AS repo_id,
                    pull_requests.platform_pr_id AS platform_pr_id,
                    repos.repo_name AS repo_name,
                    author_association,
                    repo_groups.rg_name AS repo_group,
                    pull_requests.pr_state,
                    pull_requests.merged_at,
                    pull_requests.created_at AS created_at,
                    pull_requests.closed_at AS closed_at,
                    date_part( 'year', created_at :: DATE ) AS CREATED_YEAR,
                    date_part( 'month', created_at :: DATE ) AS CREATED_MONTH,
                    date_part( 'year', closed_at :: DATE ) AS CLOSED_YEAR,
                    date_part( 'month', closed_at :: DATE ) AS CLOSED_MONTH,
                    meta_label,
                    head_or_base,
                    ( EXTRACT ( EPOCH FROM pull_requests.closed_at ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 3600 AS hours_to_close,
                    ( EXTRACT ( EPOCH FROM pull_requests.closed_at ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 86400 AS days_to_close, 
                    ( EXTRACT ( EPOCH FROM first_response_time ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 3600 AS hours_to_first_response,
                    ( EXTRACT ( EPOCH FROM first_response_time ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 86400 AS days_to_first_response, 
                    ( EXTRACT ( EPOCH FROM last_response_time ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 3600 AS hours_to_last_response,
                    ( EXTRACT ( EPOCH FROM last_response_time ) - EXTRACT ( EPOCH FROM pull_requests.created_at ) ) / 86400 AS days_to_last_response, 
                    first_response_time,
                    last_response_time,
                    EXTRACT ( EPOCH FROM average_time_between_responses),
                    assigned_count,
                    review_requested_count,
                    labeled_count,
                    subscribed_count,
                    mentioned_count,
                    referenced_count,
                    closed_count,
                    head_ref_force_pushed_count,
                    merged_count::INT,
                    milestoned_count,
                    unlabeled_count,
                    head_ref_deleted_count,
                    comment_count,
                    COALESCE(lines_added, 0), 
                    COALESCE(lines_removed, 0),
                    commit_count, 
                    COALESCE(file_count, 0)
                FROM
                    repos,
                    repo_groups,
                    pull_requests LEFT OUTER JOIN ( 
                        SELECT pull_requests.pull_request_id,
                        count(*) FILTER (WHERE action = 'assigned') AS assigned_count,
                        count(*) FILTER (WHERE action = 'review_requested') AS review_requested_count,
                        count(*) FILTER (WHERE action = 'labeled') AS labeled_count,
                        count(*) FILTER (WHERE action = 'unlabeled') AS unlabeled_count,
                        count(*) FILTER (WHERE action = 'subscribed') AS subscribed_count,
                        count(*) FILTER (WHERE action = 'mentioned') AS mentioned_count,
                        count(*) FILTER (WHERE action = 'referenced') AS referenced_count,
                        count(*) FILTER (WHERE action = 'closed') AS closed_count,
                        count(*) FILTER (WHERE action = 'head_ref_force_pushed') AS head_ref_force_pushed_count,
                        count(*) FILTER (WHERE action = 'head_ref_deleted') AS head_ref_deleted_count,
                        count(*) FILTER (WHERE action = 'milestoned') AS milestoned_count,
                        COALESCE(count(*) FILTER (WHERE action = 'merged'), 0) AS merged_count,
                        COALESCE(MIN(messages.msg_timestamp), pull_requests.merged_at, pull_requests.closed_at) AS first_response_time,
                        COALESCE(COUNT(DISTINCT messages.msg_timestamp), 0) AS comment_count,
                        COALESCE(MAX(messages.msg_timestamp), pull_requests.closed_at) AS last_response_time,
                        COALESCE((MAX(messages.msg_timestamp) - MIN(messages.msg_timestamp)) / COUNT(DISTINCT messages.msg_timestamp), pull_requests.created_at - pull_requests.closed_at) AS average_time_between_responses
                        FROM  pull_requests 
												LEFT OUTER JOIN pull_request_events on pull_requests.pull_request_id = pull_request_events.pull_request_id 
												JOIN repos on repos.repo_id = pull_requests.repo_id 
												LEFT OUTER JOIN pull_request_message_ref on pull_requests.pull_request_id = pull_request_message_ref.pull_request_id
												LEFT OUTER JOIN messages on pull_request_message_ref.msg_id = messages.msg_id
                        WHERE repos.repo_id = 1
                        GROUP BY pull_requests.pull_request_id
                    ) response_times
                    ON pull_requests.pull_request_id = response_times.pull_request_id
                    LEFT JOIN (
                        SELECT pull_request_commits.pull_request_id, count(DISTINCT pr_cmt_sha) AS commit_count                                
												FROM pull_request_commits, pull_requests, pull_request_meta
                        WHERE pull_requests.pull_request_id = pull_request_commits.pull_request_id
                        AND pull_requests.pull_request_id = pull_request_meta.pull_request_id
                        AND pull_requests.repo_id = 1
                        AND pr_cmt_sha <> pull_requests.merge_commit_sha
                        AND pr_cmt_sha <> pull_request_meta.meta_sha
                        GROUP BY pull_request_commits.pull_request_id
                    ) all_commit_counts
                    ON pull_requests.pull_request_id = all_commit_counts.pull_request_id
                    LEFT JOIN (
                        SELECT MAX(pr_meta_id), pull_request_meta.pull_request_id, head_or_base, meta_label
                        FROM pull_requests, pull_request_meta
                        WHERE pull_requests.pull_request_id = pull_request_meta.pull_request_id
                        AND pull_requests.repo_id = 1
                        AND head_or_base = 'base'
                        GROUP BY pull_request_meta.pull_request_id, head_or_base, meta_label
                    ) base_labels
                    ON base_labels.pull_request_id = all_commit_counts.pull_request_id
                    LEFT JOIN (
                        SELECT sum(cmt_added) AS lines_added, sum(cmt_removed) AS lines_removed, pull_request_commits.pull_request_id, count(DISTINCT cmt_filename) AS file_count
                        FROM pull_request_commits, commits, pull_requests, pull_request_meta
                        WHERE cmt_commit_hash = pr_cmt_sha
                        AND pull_requests.pull_request_id = pull_request_commits.pull_request_id
                        AND pull_requests.pull_request_id = pull_request_meta.pull_request_id
                        AND pull_requests.repo_id = 1
                        AND commits.repo_id = pull_requests.repo_id
                        AND commits.cmt_commit_hash <> pull_requests.merge_commit_sha
                        AND commits.cmt_commit_hash <> pull_request_meta.meta_sha
                        GROUP BY pull_request_commits.pull_request_id
                    ) master_merged_counts 
                    ON base_labels.pull_request_id = master_merged_counts.pull_request_id                    
                WHERE 
                    repos.repo_group_id = repo_groups.repo_group_id 
                    AND repos.repo_id = pull_requests.repo_id 
                    AND repos.repo_id = 1 
                ORDER BY
                    merged_count DESC; 
										
										
	select * from messages where repo_id =1 order by msg_timestamp desc; 