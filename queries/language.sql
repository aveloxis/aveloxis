            SELECT
                e.repo_id,
                aveloxis_data.repos.repo_git,
                aveloxis_data.repos.repo_name,
                e.programming_language,
                e.code_lines,
                e.files
            FROM
                aveloxis_data.repos,
            (SELECT 
                d.repo_id,
                d.programming_language,
                SUM(d.code_lines) AS code_lines,
                COUNT(*)::int AS files
            FROM
                (SELECT
                        aveloxis_data.repo_labor.repo_id,
                        aveloxis_data.repo_labor.programming_language,
                        aveloxis_data.repo_labor.code_lines
                    FROM
                        aveloxis_data.repo_labor,
                        ( SELECT 
                                aveloxis_data.repo_labor.repo_id,
                                MAX ( data_collection_date ) AS last_collected
                            FROM 
                                aveloxis_data.repo_labor
                            GROUP BY aveloxis_data.repo_labor.repo_id) recent 
                    WHERE
                        aveloxis_data.repo_labor.repo_id = recent.repo_id
                        AND aveloxis_data.repo_labor.data_collection_date > recent.last_collected - (5 * interval '1 minute')) d
            GROUP BY d.repo_id, d.programming_language) e
            WHERE aveloxis_data.repos.repo_id = e.repo_id
            ORDER BY e.repo_id