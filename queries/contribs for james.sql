       SELECT r.repo_id,
                    r.repo_git,
                    ca.cntrb_id,
                    c.cmt_id
                    FROM
                    repos r, commits c, contributors_aliases ca
                    WHERE
                    c.repo_id in (31136) AND
                    c.repo_id = r.repo_id and
                    c.cmt_committer_email = ca.alias_email and 
                    c.cmt_ght_author_id IS NOT NULL  -- during processing the cntrb_id id populated after commit, so there
                    -- will always be some small number of NULL values temporarily. 