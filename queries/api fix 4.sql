-- NOTE: This references an Augur materialized view not yet available in Aveloxis
            SELECT repo_id, COUNT(*) AS repo_contributor_count FROM
            (
            SELECT explorer_contributor_actions.cntrb_id as cntrb_id, repo_group_id, repos.repo_id as repo_id, COUNT(*) FROM explorer_contributor_actions, repos
						where explorer_contributor_actions.repo_id=repos.repo_id 
						and repo_group_id = 1
						GROUP BY cntrb_id, repos.repo_id, repos.repo_group_id
            ) a
          --  WHERE repo_group_id= 1
            GROUP BY repo_id
            ORDER BY repo_id;