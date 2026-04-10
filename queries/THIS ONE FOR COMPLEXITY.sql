WITH repo_filter(repo_id) AS (
    VALUES
      (300407),(191250),(36843),(132866),(299696),(287965),(298906),(300006),(36173),
      (299797),(298605),(300091),(300617),(210633),(298661),(298065),(298532),(202144),
      (202157),(202136),(202164),(202143),(292482),(290918),(210625),(36760),(300897),
      (298506),(203934),(299642),(301343),(300114),(300724),(36255),(36310),(56087),
      (203935),(203932),(299491),(293090),(300349),(288136),(223588),(188867),(56049),
      (56086),(56081),(56053),(203926),(56050),(298009),(244026),(267616),(299862),
      (299207),(143803),(297634),(256414),(256400),(256420),(256413),(203948),(203953),
      (203943),(203945),(131603),(131679),(250968),(249980),(256410),(207931),(226818),
      (256435),(250749),(251179),(251109),(250664),(251095),(227455),(250655),(227443),
      (250962),(250717),(299949),(257690),(227432)
),

pr_base AS (
    SELECT
        pr.pull_request_id,
        pr.repo_id,
        pr.pr_number,
        pr.pr_title,
        pr.created_at,
        pr.merged_at,
        pr.author_id,
        COALESCE(pr.merged_at, pr.created_at) AS pr_effective_at
    FROM aveloxis_data.pull_requests pr
    JOIN repo_filter rf
      ON rf.repo_id = pr.repo_id
),

prf_base AS (
    SELECT
        prf.pull_request_id,
        prf.repo_id,
        prf.pr_file_path,
        prf.pr_file_additions,
        prf.pr_file_deletions
    FROM aveloxis_data.pull_request_files prf
    JOIN repo_filter rf
      ON rf.repo_id = prf.repo_id
),

prf_dedup AS (
    SELECT
        repo_id,
        pull_request_id,
        pr_file_path,
        SUM(pr_file_additions) AS pr_file_additions,
        SUM(pr_file_deletions) AS pr_file_deletions
    FROM prf_base
    GROUP BY
        repo_id,
        pull_request_id,
        pr_file_path
)

SELECT
    pr.pull_request_id,
    pr.repo_id,
    pr.pr_number,
    pr.pr_title,
    pr.created_at,
    pr.merged_at,
    pr.author_id,

    COUNT(*) AS pr_file_count,
    SUM(prf.pr_file_additions) AS pr_total_additions,
    SUM(prf.pr_file_deletions) AS pr_total_deletions,
    SUM(prf.pr_file_additions + prf.pr_file_deletions) AS pr_total_churn,

    SUM(rl.code_lines) AS pr_total_code_lines,
    SUM(rl.total_lines) AS pr_total_lines,
    AVG(rl.code_complexity) AS pr_avg_file_complexity,
    MAX(rl.code_complexity) AS pr_max_file_complexity,
    SUM(rl.code_complexity * rl.code_lines) / NULLIF(SUM(rl.code_lines), 0) AS pr_weighted_avg_complexity

FROM pr_base pr
JOIN prf_dedup prf
  ON prf.pull_request_id = pr.pull_request_id
 AND prf.repo_id = pr.repo_id

LEFT JOIN LATERAL (
    /* rel_path := substring(file_path FROM '^/mnt/repos/hostedaugur/hosted/[^/]+/(.*)$') */

    SELECT picked.code_lines, picked.total_lines, picked.code_complexity
    FROM (
        SELECT
            b.code_lines,
            b.total_lines,
            b.code_complexity,
            ABS(EXTRACT(EPOCH FROM (b.rl_analysis_date - pr.pr_effective_at))) AS dist_sec
        FROM (
            SELECT
                rl1.rl_analysis_date,
                rl1.code_lines,
                rl1.total_lines,
                rl1.code_complexity
            FROM aveloxis_data.repo_labor rl1
            WHERE rl1.repo_id = pr.repo_id
              AND substring(rl1.file_path FROM '^/mnt/repos/hostedaugur/hosted/[^/]+/(.*)$')
                    = prf.pr_file_path
              AND rl1.rl_analysis_date <= pr.pr_effective_at
            ORDER BY rl1.rl_analysis_date DESC
            LIMIT 1
        ) b

        UNION ALL

        SELECT
            a.code_lines,
            a.total_lines,
            a.code_complexity,
            ABS(EXTRACT(EPOCH FROM (a.rl_analysis_date - pr.pr_effective_at))) AS dist_sec
        FROM (
            SELECT
                rl2.rl_analysis_date,
                rl2.code_lines,
                rl2.total_lines,
                rl2.code_complexity
            FROM aveloxis_data.repo_labor rl2
            WHERE rl2.repo_id = pr.repo_id
              AND substring(rl2.file_path FROM '^/mnt/repos/hostedaugur/hosted/[^/]+/(.*)$')
                    = prf.pr_file_path
              AND rl2.rl_analysis_date >= pr.pr_effective_at
            ORDER BY rl2.rl_analysis_date ASC
            LIMIT 1
        ) a
    ) picked
    ORDER BY picked.dist_sec
    LIMIT 1
) rl ON true

GROUP BY
    pr.pull_request_id,
    pr.repo_id,
    pr.pr_number,
    pr.pr_title,
    pr.created_at,
    pr.merged_at,
    pr.author_id

ORDER BY
    pr.repo_id,
    pr.pull_request_id;