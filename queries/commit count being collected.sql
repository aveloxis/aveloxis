/**
SELECT commit_count FROM repo_info WHERE repo_id IN (300699,299041,299287,298540,300022,301126,301234,299559,299678,300504,299907,301480,301049,300537,301004,299026,300120,299186,299202,300446,298298,301371,299961,300397,298487,298290,299495,301037,298532,299888,299613,298929,300694,300695,301331,301062,299695,298479,299231,298139,298883,299703,299738,298789,300515,301278,300374,301005,299924,301165,301182,298178,298521,299240,299591,298261,300616,300143,298713,300162,299171,298295,301132,298036,299682,299686,300541,300932,299461,299672,300715,299279,299148,300117); 

SELECT commit_count FROM repo_info WHERE repo_id IN (299985,298168,300317,299737,298920,53669,300753,300059,299554,299501,298115,300809,298362,300129,300951,300642,300096); 
**/
WITH latest AS (
SELECT r.*FROM repo_info r JOIN (
SELECT repo_id,MAX (data_collection_date) AS max_date FROM repo_info 
--WHERE 
--repo_id=ANY ($ 1) 
GROUP BY repo_id) M ON r.repo_id=M.repo_id AND r.data_collection_date=M.max_date WHERE r.repo_id IN (299985,298168,300317,299737,298920,53669,300753,300059,299554,299501,298115,300809,298362,300129,300951,300642,300096)-- = ANY($1)
)
SELECT DISTINCT ON (repo_id) repo_id,commit_count,data_collection_date FROM latest ORDER BY repo_id,commit_count DESC --; -- swap in a different tie-breaker if you prefer
;


WITH latest AS (
SELECT r.*FROM repo_info r JOIN (
SELECT repo_id,MAX (data_collection_date) AS max_date FROM repo_info 
--WHERE 
--repo_id=ANY ($ 1) 
GROUP BY repo_id) M ON r.repo_id=M.repo_id AND r.data_collection_date=M.max_date WHERE r.repo_id IN (299985	,
300809	,
53669	,
300753	,
300129	,
300317	,
298115	,
300951	,
300059	,
299434	,
300642	,
299737	,
298364	,
298429	,
298362	,
298168	,
299554	)-- = ANY($1)
) 
SELECT DISTINCT ON (repo_id) repo_id,commit_count,data_collection_date FROM latest ORDER BY repo_id,commit_count DESC;-- swap in a different tie-breaker if you typispreferred



WITH maxes AS (
  SELECT repo_id, MAX(data_collection_date) AS max_date
  FROM repo_info
  GROUP BY repo_id
),
a AS (
  SELECT r.repo_id, r.commit_count, r.data_collection_date
  FROM repo_info r
  JOIN maxes m USING (repo_id)
  WHERE r.data_collection_date = m.max_date
    AND r.repo_id IN (299985,298168,300317,299737,298920,53669,300753,300059,
                      299554,299501,298115,300809,298362,300129,300951,300642,300096)
),
b AS (
  SELECT r.repo_id, r.commit_count, r.data_collection_date
  FROM repo_info r
  JOIN maxes m USING (repo_id)
  WHERE r.data_collection_date = m.max_date
    AND r.repo_id IN (300699,299041,299287,298540,300022,301126,301234,299559,
                      299678,300504,299907,301480,301049,300537,301004,299026,
                      300120,299186,299202,300446,298298,301371,299961,300397,
                      298487,298290,299495,301037,298532,299888,299613,298929,
                      300694,300695,301331,301062,299695,298479,299231,298139,
                      298883,299703,299738,298789,300515,301278,300374,301005,
                      299924,301165,301182,298178,298521,299240,299591,298261,
                      300616,300143,298713,300162,299171,298295,301132,298036,
                      299682,299686,300541,300932,299461,299672,300715,299279,
                      299148,300117)
)
SELECT DISTINCT ON (repo_id)
       repo_id, commit_count, data_collection_date
FROM (
  SELECT * FROM a
  UNION ALL
  SELECT * FROM b
) u
ORDER BY repo_id, commit_count DESC;






select repo_id, commit_count from 
(
WITH latest AS (
SELECT r.*FROM repo_info r JOIN (
SELECT repo_id,MAX (data_collection_date) AS max_date FROM repo_info 
--WHERE 
--repo_id=ANY ($ 1) 
GROUP BY repo_id) M ON r.repo_id=M.repo_id AND r.data_collection_date=M.max_date 
) 
SELECT DISTINCT ON (repo_id) repo_id,commit_count,data_collection_date FROM latest ORDER BY repo_id,commit_count DESC
) a 
where commit_count is not null 
order by commit_count desc; 



WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) M
    ON r.repo_id = M.repo_id AND r.data_collection_date = M.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count
    FROM latest 
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
)
SELECT
    AVG(commit_count)::NUMERIC(12,2) AS mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY commit_count) AS median,
    MODE() WITHIN GROUP (ORDER BY commit_count) AS mode,
    STDDEV_POP(commit_count)::NUMERIC(12,2) AS std_dev
FROM deduped;




WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) M
      ON r.repo_id = M.repo_id
     AND r.data_collection_date = M.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count
    FROM latest 
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
),
stats AS (
    SELECT
        AVG(commit_count)::NUMERIC(12,2)                              AS mean,
        MODE() WITHIN GROUP (ORDER BY commit_count)                   AS mode,
        STDDEV_POP(commit_count)::NUMERIC(12,2)                       AS std_dev,
        PERCENTILE_CONT(ARRAY[0.25, 0.50, 0.75])
            WITHIN GROUP (ORDER BY commit_count)                      AS q
    FROM deduped
)
SELECT
    mean,
    q[1]::NUMERIC(12,2)                                              AS q1,
    q[2]::NUMERIC(12,2)                                              AS median,
    q[3]::NUMERIC(12,2)                                              AS q3,
    (q[3] - q[1])::NUMERIC(12,2)                                     AS iqr,
    mode,
    std_dev
FROM stats;





WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) m ON r.repo_id = m.repo_id
       AND r.data_collection_date = m.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count
    FROM latest
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
)
SELECT
    AVG(commit_count)::NUMERIC(12,2) AS mean,
    (PERCENTILE_CONT(ARRAY[0.25,0.50,0.75]) WITHIN GROUP (ORDER BY commit_count))[1]::NUMERIC(12,2) AS q1,
    (PERCENTILE_CONT(ARRAY[0.25,0.50,0.75]) WITHIN GROUP (ORDER BY commit_count))[2]::NUMERIC(12,2) AS median,
    (PERCENTILE_CONT(ARRAY[0.25,0.50,0.75]) WITHIN GROUP (ORDER BY commit_count))[3]::NUMERIC(12,2) AS q3,
    ((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY commit_count)) -
     (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY commit_count)))::NUMERIC(12,2) AS iqr,
    MODE() WITHIN GROUP (ORDER BY commit_count) AS mode,
    STDDEV_POP(commit_count)::NUMERIC(12,2) AS std_dev
FROM deduped;




WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) m ON r.repo_id = m.repo_id
       AND r.data_collection_date = m.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count
    FROM latest
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
)
SELECT
    AVG(commit_count)::NUMERIC(12,2) AS mean,
    MODE() WITHIN GROUP (ORDER BY commit_count) AS mode,
    STDDEV_POP(commit_count)::NUMERIC(12,2) AS std_dev,
    (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q1,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q2_median,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q3,
    (PERCENTILE_CONT(1.0)  WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q4_max,
    ((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY commit_count))
     - (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY commit_count)))::NUMERIC(12,2) AS iqr
FROM deduped;





WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) m ON r.repo_id = m.repo_id
       AND r.data_collection_date = m.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count::numeric AS commit_count
    FROM latest
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
),
-- Set your target value here (or bind it as a parameter)
param(v) AS (
    VALUES (1000::numeric)
),
-- Add the value into the series to compute its position
combined AS (
    SELECT commit_count, false AS is_param FROM deduped
    UNION ALL
    SELECT v AS commit_count, true AS is_param FROM param
),
positioned AS (
    SELECT
        *,
        CUME_DIST()   OVER (ORDER BY commit_count) AS cume_dist_val,
        PERCENT_RANK() OVER (ORDER BY commit_count) AS percent_rank_val
    FROM combined
),
val AS (
    SELECT
        commit_count AS eval_commit,
        cume_dist_val,
        percent_rank_val
    FROM positioned
    WHERE is_param
),
stats AS (
    SELECT
        AVG(commit_count)::NUMERIC(12,2) AS mean,
        MODE() WITHIN GROUP (ORDER BY commit_count) AS mode,
        STDDEV_POP(commit_count)::NUMERIC(12,2) AS std_dev,
        (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q1,
        (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q2_median,
        (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q3,
        (PERCENTILE_CONT(1.0)  WITHIN GROUP (ORDER BY commit_count))::NUMERIC(12,2) AS q4_max,
        ((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY commit_count))
         - (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY commit_count)))::NUMERIC(12,2) AS iqr
    FROM deduped
)
SELECT
    s.*,
    v.eval_commit,
    ROUND((100 * v.cume_dist_val)::numeric, 2)    AS eval_percentile_cume_pct,   -- % ≤ value
    ROUND((100 * v.percent_rank_val)::numeric, 2) AS eval_percent_rank_pct       -- rank-based %
FROM stats s
CROSS JOIN val v;





WITH latest AS (
    SELECT r.*
    FROM repo_info r
    JOIN (
        SELECT repo_id, MAX(data_collection_date) AS max_date
        FROM repo_info 
        GROUP BY repo_id
    ) m ON r.repo_id = m.repo_id
       AND r.data_collection_date = m.max_date
),
deduped AS (
    SELECT DISTINCT ON (repo_id) repo_id, commit_count::numeric AS commit_count
    FROM latest
    WHERE commit_count IS NOT NULL
    ORDER BY repo_id, commit_count DESC
),
max_val AS (
    SELECT MAX(commit_count)::int AS max_commit
    FROM deduped
),
steps AS (
    SELECT generate_series(0, (SELECT max_commit FROM max_val), 50) AS eval_commit
),
combined AS (
    SELECT commit_count, false AS is_step FROM deduped
    UNION ALL
    SELECT eval_commit, true AS is_step FROM steps
),
positioned AS (
    SELECT
        commit_count,
        is_step,
        CUME_DIST()   OVER (ORDER BY commit_count) AS cume_dist_val,
        PERCENT_RANK() OVER (ORDER BY commit_count) AS percent_rank_val
    FROM combined
)
SELECT
    commit_count AS eval_commit,
    ROUND((100 * cume_dist_val)::numeric, 2)    AS cume_percentile,
    ROUND((100 * percent_rank_val)::numeric, 2) AS rank_percentile
FROM positioned
WHERE is_step
ORDER BY eval_commit;
