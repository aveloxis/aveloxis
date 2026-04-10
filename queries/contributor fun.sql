explain analyze 
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
								
								
--explain analyze 
select gh_login, cntrb_id from contributors where cntrb_id not in (select cntrb_id from contributor_repo)
and gh_login is not null 
union 
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