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
	average_time_between_responses,
	assigned_count,
	review_requested_count,
	labeled_count,
	subscribed_count,
	mentioned_count,
	referenced_count,
	closed_count,
	head_ref_force_pushed_count,
	merged_count,
	milestoned_count,
	unlabeled_count,
	head_ref_deleted_count,
	comment_count,
	lines_added,
	lines_removed,
	commit_count,
	file_count 
FROM
	repos,
	repo_groups,
	pull_requests
	LEFT OUTER JOIN (
	SELECT
		pull_requests.pull_request_id,
		COUNT ( * ) FILTER ( WHERE ACTION = 'assigned' ) AS assigned_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'review_requested' ) AS review_requested_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'labeled' ) AS labeled_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'unlabeled' ) AS unlabeled_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'subscribed' ) AS subscribed_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'mentioned' ) AS mentioned_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'referenced' ) AS referenced_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'closed' ) AS closed_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'head_ref_force_pushed' ) AS head_ref_force_pushed_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'head_ref_deleted' ) AS head_ref_deleted_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'milestoned' ) AS milestoned_count,
		COUNT ( * ) FILTER ( WHERE ACTION = 'merged' ) AS merged_count,
		MIN ( messages.msg_timestamp ) AS first_response_time,
		COUNT ( DISTINCT messages.msg_timestamp ) AS comment_count,
		MAX ( messages.msg_timestamp ) AS last_response_time,
		( MAX ( messages.msg_timestamp ) - MIN ( messages.msg_timestamp ) ) / COUNT ( DISTINCT messages.msg_timestamp ) AS average_time_between_responses 
	FROM
		pull_request_events,
		pull_requests,
		repos,
		pull_request_message_ref,
		messages
	WHERE
		--repos.repo_id = { repo_id } 
		--AND 
		repos.repo_id = pull_requests.repo_id 
		AND pull_requests.pull_request_id = pull_request_events.pull_request_id 
		AND pull_requests.pull_request_id = pull_request_message_ref.pull_request_id 
		AND pull_request_message_ref.msg_id = messages.msg_id 
	GROUP BY
		pull_requests.pull_request_id 
	) response_times ON pull_requests.pull_request_id = response_times.pull_request_id
	LEFT OUTER JOIN (
	SELECT
		pull_request_commits.pull_request_id,
		COUNT ( DISTINCT pr_cmt_sha ) AS commit_count 
	FROM
		pull_request_commits,
		pull_requests,
		pull_request_meta 
	WHERE
		pull_requests.pull_request_id = pull_request_commits.pull_request_id 
		AND pull_requests.pull_request_id = pull_request_meta.pull_request_id 
		--AND pull_requests.repo_id = { repo_id } 
		AND pr_cmt_sha <> pull_requests.merge_commit_sha 
		AND pr_cmt_sha <> pull_request_meta.meta_sha 
	GROUP BY
		pull_request_commits.pull_request_id 
	) all_commit_counts ON pull_requests.pull_request_id = all_commit_counts.pull_request_id
	LEFT OUTER JOIN (
	SELECT MAX
		( pr_meta_id ),
		pull_request_meta.pull_request_id,
		head_or_base,
		meta_label 
	FROM
		pull_requests,
		pull_request_meta 
	WHERE
		pull_requests.pull_request_id = pull_request_meta.pull_request_id 
		--AND pull_requests.repo_id = { repo_id } 
		AND head_or_base = 'base' 
	GROUP BY
		pull_request_meta.pull_request_id,
		head_or_base,
		meta_label 
	) base_labels ON base_labels.pull_request_id = all_commit_counts.pull_request_id
	LEFT OUTER JOIN (
	SELECT SUM
		( cmt_added ) AS lines_added,
		SUM ( cmt_removed ) AS lines_removed,
		pull_request_commits.pull_request_id,
		COUNT ( DISTINCT cmt_filename ) AS file_count 
	FROM
		pull_request_commits,
		commits,
		pull_requests,
		pull_request_meta 
	WHERE
		cmt_commit_hash = pr_cmt_sha 
		AND pull_requests.pull_request_id = pull_request_commits.pull_request_id 
		AND pull_requests.pull_request_id = pull_request_meta.pull_request_id 
		--AND pull_requests.repo_id = { repo_id } 
		AND commits.repo_id = pull_requests.repo_id 
		AND commits.cmt_commit_hash <> pull_requests.merge_commit_sha 
		AND commits.cmt_commit_hash <> pull_request_meta.meta_sha 
	GROUP BY
		pull_request_commits.pull_request_id 
	) master_merged_counts ON base_labels.pull_request_id = master_merged_counts.pull_request_id 
WHERE
	repos.repo_group_id = repo_groups.repo_group_id 
	AND repos.repo_id = pull_requests.repo_id 
	--AND repos.repo_id=1
	--AND repos.repo_id = { repo_id } 
ORDER BY
	merged_count DESC