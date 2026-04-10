SELECT
	x.cntrb_id,
	x.created_at,
	x.repo_id,
	x.repo_name,
	x.LOGIN,
	x.ACTION,
	x.RANK 
FROM
	(
	SELECT
		b.cntrb_id,
		b.created_at,
		b.MONTH,
		b.YEAR,
		b.repo_id,
		b.repo_name,
		b.full_name,
		b.LOGIN,
		b.ACTION,
		b.RANK 
	FROM
		(
		SELECT A
			.ID AS cntrb_id,
			A.created_at,
			date_part( 'month' :: TEXT, ( A.created_at ) :: DATE ) AS MONTH,
			date_part( 'year' :: TEXT, ( A.created_at ) :: DATE ) AS YEAR,
			A.repo_id,
			repos.repo_name,
			A.full_name,
			A.LOGIN,
			A.ACTION,
			RANK ( ) OVER ( PARTITION BY A.ID, A.repo_id ORDER BY A.created_at ) AS RANK 
		FROM
			(
			SELECT
				canonical_full_names.canonical_id AS ID,
				issues.created_at,
				issues.repo_id,
				'issue_opened' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				(
					( aveloxis_data.issues LEFT JOIN aveloxis_data.contributors ON ( ( contributors.cntrb_id = issues.reporter_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			WHERE
				( issues.pull_request IS NULL ) 
			GROUP BY
				canonical_full_names.canonical_id,
				issues.repo_id,
				issues.created_at,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				canonical_full_names.canonical_id AS ID,
				to_timestamp( ( commits.cmt_author_date ) :: TEXT, 'YYYY-MM-DD' :: TEXT ) AS created_at,
				commits.repo_id,
				'commit' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				(
					( aveloxis_data.commits LEFT JOIN aveloxis_data.contributors ON ( ( ( contributors.cntrb_canonical ) :: TEXT = ( commits.cmt_author_email ) :: TEXT ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_canonical ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			GROUP BY
				commits.repo_id,
				canonical_full_names.canonical_email,
				canonical_full_names.canonical_id,
				commits.cmt_author_date,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				messages.cntrb_id AS ID,
				commit_comment_ref.created_at,
				commits.repo_id,
				'commit_comment' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				aveloxis_data.commit_comment_ref,
				aveloxis_data.commits,
				(
					( aveloxis_data.messages LEFT JOIN aveloxis_data.contributors ON ( ( contributors.cntrb_id = messages.cntrb_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			WHERE
				( ( commits.cmt_id = commit_comment_ref.cmt_id ) AND ( commit_comment_ref.msg_id = messages.msg_id ) ) 
			GROUP BY
				messages.cntrb_id,
				commits.repo_id,
				commit_comment_ref.created_at,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				issue_events.cntrb_id AS ID,
				issue_events.created_at,
				issues.repo_id,
				'issue_closed' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				aveloxis_data.issues,
				(
					( aveloxis_data.issue_events LEFT JOIN aveloxis_data.contributors ON ( ( contributors.cntrb_id = issue_events.cntrb_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			WHERE
				(
					( issues.issue_id = issue_events.issue_id ) 
					AND ( issues.pull_request IS NULL ) 
					AND ( issue_events.cntrb_id IS NOT NULL ) 
					AND ( ( issue_events.ACTION ) :: TEXT = 'closed' :: TEXT ) 
				) 
			GROUP BY
				issue_events.cntrb_id,
				issues.repo_id,
				issue_events.created_at,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				pull_requests.author_id AS ID,
				pull_requests.created_at AS created_at,
				pull_requests.repo_id,
				'open_pull_request' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				(
					( aveloxis_data.pull_requests LEFT JOIN aveloxis_data.contributors ON ( ( pull_requests.author_id = contributors.cntrb_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			GROUP BY
				pull_requests.author_id,
				pull_requests.repo_id,
				pull_requests.created_at,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				messages.cntrb_id AS ID,
				messages.msg_timestamp AS created_at,
				pull_requests.repo_id,
				'pull_request_comment' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				aveloxis_data.pull_requests,
				aveloxis_data.pull_request_message_ref,
				(
					( aveloxis_data.messages LEFT JOIN aveloxis_data.contributors ON ( ( contributors.cntrb_id = messages.cntrb_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			WHERE
				( ( pull_request_message_ref.pull_request_id = pull_requests.pull_request_id ) AND ( pull_request_message_ref.msg_id = messages.msg_id ) ) 
			GROUP BY
				messages.cntrb_id,
				pull_requests.repo_id,
				messages.msg_timestamp,
				contributors.cntrb_full_name,
				contributors.cntrb_login UNION ALL
			SELECT
				issues.reporter_id AS ID,
				messages.msg_timestamp AS created_at,
				issues.repo_id,
				'issue_comment' :: TEXT AS ACTION,
				contributors.cntrb_full_name AS full_name,
				contributors.cntrb_login AS LOGIN 
			FROM
				aveloxis_data.issues,
				aveloxis_data.issue_message_ref,
				(
					( aveloxis_data.messages LEFT JOIN aveloxis_data.contributors ON ( ( contributors.cntrb_id = messages.cntrb_id ) ) )
					LEFT JOIN (
					SELECT DISTINCT ON
						( contributors_1.cntrb_canonical ) contributors_1.cntrb_full_name,
						contributors_1.cntrb_canonical AS canonical_email,
						contributors_1.data_collection_date,
						contributors_1.cntrb_id AS canonical_id 
					FROM
						aveloxis_data.contributors contributors_1 
					WHERE
						( ( contributors_1.cntrb_canonical ) :: TEXT = ( contributors_1.cntrb_email ) :: TEXT ) 
					ORDER BY
						contributors_1.cntrb_canonical 
					) canonical_full_names ON ( ( ( canonical_full_names.canonical_email ) :: TEXT = ( contributors.cntrb_canonical ) :: TEXT ) ) 
				) 
			WHERE
				( ( issue_message_ref.msg_id = messages.msg_id ) AND ( issues.issue_id = issue_message_ref.issue_id ) AND ( issues.pull_request_id = NULL :: BIGINT ) ) 
			GROUP BY
				issues.reporter_id,
				issues.repo_id,
				messages.msg_timestamp,
				contributors.cntrb_full_name,
				contributors.cntrb_login 
			) A,
			aveloxis_data.repos 
		WHERE
			( ( A.ID IS NOT NULL ) AND ( A.repo_id = repos.repo_id ) ) 
		GROUP BY
			A.ID,
			A.repo_id,
			A.ACTION,
			A.created_at,
			repos.repo_name,
			A.full_name,
			A.LOGIN 
		ORDER BY
			A.created_at DESC 
		) b 
	) x 
ORDER BY
	x.created_at DESC