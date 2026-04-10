select cntrb_id, count(*) as action_count from contributor_repo GROUP BY cntrb_id;  --- 1,023,384 1,024,734 1,025,700  1,027,086 1,059,797, 1,068,041 1,068,268

            SELECT DISTINCT
                gh_login,
                cntrb_id 
            FROM
                (
                SELECT DISTINCT
                    gh_login,
                    cntrb_id,
                    data_collection_date 
                FROM
                    (
                    SELECT DISTINCT
                        contributors.gh_login,
                        contributors.cntrb_id,
                        contributor_repo.data_collection_date :: DATE 
                    FROM
                        contributor_repo
                        RIGHT OUTER JOIN contributors ON contributors.cntrb_id = contributor_repo.cntrb_id 
                        AND contributors.gh_login IS NOT NULL 
                    ORDER BY
                        contributor_repo.data_collection_date :: DATE NULLS FIRST 
                    ) A 
                ORDER BY
                data_collection_date DESC NULLS FIRST 
                ) b; 
								
								
								
								SELECT DISTINCT
                    gh_login,
                    cntrb_id,
                    data_collection_date 
                FROM
                    (
                    SELECT DISTINCT
                        contributors.gh_login,
                        contributors.cntrb_id,
                        contributor_repo.data_collection_date :: DATE 
                    FROM
                        contributor_repo
                        RIGHT OUTER JOIN contributors ON contributors.cntrb_id = contributor_repo.cntrb_id 
                        AND contributors.gh_login IS NOT NULL 
                    ORDER BY
                        contributor_repo.data_collection_date :: DATE NULLS FIRST 
                    ) A 
                ORDER BY
                data_collection_date DESC NULLS FIRST ; 