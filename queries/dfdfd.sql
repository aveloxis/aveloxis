        SELECT a.repo_id,
            a.repo_name,
            b.name,
            b.requirement,
            b.current_verion,
            b.latest_version,
            b.current_release_date,
            b.libyear,
            max(b.data_collection_date) AS max
        FROM aveloxis_data.repos a,
            aveloxis_data.repo_deps_libyear b
        GROUP BY a.repo_id, a.repo_name, b.name, b.requirement, b.current_verion, b.latest_version, b.current_release_date, b.libyear
        ORDER BY a.repo_id, b.requirement;