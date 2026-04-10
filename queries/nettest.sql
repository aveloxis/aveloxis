-- NOTE: This references an Augur materialized view not yet available in Aveloxis
                        SELECT
                        	repo_git, ntile(4) over ( order by commits_all_time) 
                        FROM
                        	repos A,
                        	(
                        	SELECT C.repo_id, d.commits_all_time
                        	FROM
                        		aveloxis_ops.users A,
                        		aveloxis_ops.user_groups b,
                        		aveloxis_ops.user_repos C, 
                        		api_get_all_repos_commits d 
                        	WHERE
                        		A.user_id = b.user_id 
                        		AND b.group_id = C.group_id 
                        		AND d.repo_id= c.repo_id
                        		AND b.NAME = 'science' --AND lower(A.login_name)='numfocus'
                        	ORDER BY
                        		A.login_name,
                        		d.commits_all_time,
                        		b.group_id 
                        	) b 
                        WHERE
                        	A.repo_id = b.repo_id order by commits_all_time desc;
					                    SET SCHEMA 'aveloxis_data';
                                  SELECT 
                            b.repo_id,
                            b.repo_name
                        FROM
                            repo_groups a,
                            repos b
                        WHERE
                            a.repo_group_id = b.repo_group_id AND
                            b.repo_git = 'https://github.com/rails/rails';
														
														
									                   SET SCHEMA 'aveloxis_data';
                    SELECT r.repo_id,
                    r.repo_git,
                    i.reporter_id as cntrb_id,
                    i.issue_id
                    FROM
                    repos r, issues i
                     WHERE
                    i.repo_id in (29985) AND
                    i.repo_id = r.repo_id	;				
														

                                      SELECT r.repo_id,
                    r.repo_git,
                    prm.cntrb_id,
                    prm.pull_request_id
                    FROM
                    repos r, pull_request_meta prm
                    WHERE
                    prm.repo_id in (29985) AND
                    prm.repo_id = r.repo_id;
										
										
										                    SET SCHEMA 'aveloxis_data';
                    SELECT r.repo_id,
                    r.repo_git,
                    ca.cntrb_id,
                    c.cmt_id
                    FROM
                    repos r, commits c, contributors_aliases ca
                    WHERE
                    c.repo_id in (29985) AND
                    c.repo_id = r.repo_id and
                    c.cmt_committer_email = ca.alias_email; 
										
										
										                    SELECT r.repo_id,
                    r.repo_git,
                    prr.cntrb_id,
                    prr.pull_request_id
                    FROM
                    repos r, pull_request_reviews prr
                    WHERE
                    prr.repo_id in (29985) AND
                    prr.repo_id = r.repo_id;