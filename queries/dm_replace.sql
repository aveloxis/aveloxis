
        SELECT r.repo_group_id AS repo_group_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('week', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS week, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches,
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL  
        AND e.domain IS NULL  
        AND p.rg_recache = 1 
        GROUP BY week, 
        year, 
        affiliation, 
        fa.cmt_{report_attribution}_email, 
        r.repo_group_id, info.a, info.b, info.c)
        ).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)

    session.execute_sql(cache_projects_by_week)

    cache_projects_by_month = s.sql.text(
        (INSERT INTO dm_repo_group_monthly (repo_group_id, email, affiliation, month, year, added, removed, whitespace, files, patches, tool_source, tool_version, data_source) 
        SELECT r.repo_group_id AS repo_group_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('month', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS month, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches,
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL 
        AND e.domain IS NULL 
        AND p.rg_recache = 1 
        GROUP BY month, 
        year, 
        affiliation, 
        fa.cmt_{report_attribution}_email,
        r.repo_group_id, info.a, info.b, info.c
        )).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)

    session.execute_sql(cache_projects_by_month)

    cache_projects_by_year = s.sql.text((
        INSERT INTO dm_repo_group_annual (repo_group_id, email, affiliation, year, added, removed, whitespace, files, patches, tool_source, tool_version, data_source) 
        SELECT r.repo_group_id AS repo_group_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches,
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL 
        AND e.domain IS NULL 
        AND p.rg_recache = 1 
        GROUP BY year, 
        affiliation, 
        fa.cmt_{report_attribution}_email,
        r.repo_group_id, info.a, info.b, info.c

        
        
        )).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)

     
     

    session.execute_sql(cache_projects_by_year)
    # Start caching by repos
    session.log_activity('Verbose','Caching repos')

    cache_repos_by_week = s.sql.text(
        (
        INSERT INTO dm_repo_weekly (repo_id, email, affiliation, week, year, added, removed, whitespace, files, patches, tool_source, tool_version, data_source) 
        SELECT a.repo_id AS repo_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('week', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS week, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches,
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL 
        AND e.domain IS NULL 
        AND p.rg_recache = 1 
        GROUP BY week, 
        year, 
        affiliation, 
        fa.cmt_{report_attribution}_email,
        a.repo_id, info.a, info.b, info.c
        )).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)

    session.execute_sql(cache_repos_by_week)

    cache_repos_by_month = s.sql.text((
        INSERT INTO dm_repo_monthly (repo_id, email, affiliation, month, year, added, removed, whitespace, files, patches, tool_source, tool_version, data_source)
        SELECT a.repo_id AS repo_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('month', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS month, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches, 
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL 
        AND e.domain IS NULL 
        AND p.rg_recache = 1 
        GROUP BY month, 
        year, 
        affiliation, 
        fa.cmt_{report_attribution}_email,
        a.repo_id, info.a, info.b, info.c
        )).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)

    session.execute_sql(cache_repos_by_month)

    cache_repos_by_year = s.sql.text((
        INSERT INTO dm_repo_annual (repo_id, email, affiliation, year, added, removed, whitespace, files, patches, tool_source, tool_version, data_source)
        SELECT a.repo_id AS repo_id, 
        fa.cmt_{report_attribution}_email AS email, 
        fa.cmt_{report_attribution}_affiliation AS affiliation, 
        fdate_part('year', TO_TIMESTAMP(a.cmt_{report_date}_date, 'YYYY-MM-DD')) AS year, 
        SUM(a.cmt_added) AS added, 
        SUM(a.cmt_removed) AS removed, 
        SUM(a.cmt_whitespace) AS whitespace, 
        COUNT(DISTINCT a.cmt_filename) AS files, 
        COUNT(DISTINCT a.cmt_commit_hash) AS patches, 
        info.a AS tool_source, info.b AS tool_version, info.c AS data_source 
        FROM (VALUES(:tool_source,:tool_version,:data_source)) info(a,b,c), 
        commits a 
        JOIN repos r ON r.repo_id = a.repo_id 
        JOIN repo_groups p ON p.repo_group_id = r.repo_group_id 
        LEFT JOIN exclude e ON 
            (a.cmt_author_email = e.email 
                AND (e.projects_id = r.repo_group_id 
                    OR e.projects_id = 0)) 
            OR (a.cmt_author_email LIKE CONCAT('%%',e.domain) 
                AND (e.projects_id = r.repo_group_id 
                OR e.projects_id = 0)) 
        WHERE e.email IS NULL 
        AND e.domain IS NULL 
        AND p.rg_recache = 1 
        GROUP BY year, 
        affiliation, 
        fa.cmt_{report_attribution}_email,
        a.repo_id, info.a, info.b, info.c
        )).bindparams(tool_source=session.tool_source,tool_version=session.tool_version,data_source=session.data_source)
