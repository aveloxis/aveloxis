select * from repos where repo_name like '%pytorch%'; --56361



select repo_id, count(*) from 
(
select repo_id, cmt_commit_hash, count(*) from commits where repo_id=77099 group by repo_id, cmt_commit_hash
) a group by a.repo_id 

SELECT
                        distinct
                        r.repo_id as id,
                        c.cmt_commit_hash AS commits,
                        c.cmt_author_email AS author_email,
                        c.cmt_author_date AS date,
                        -- all timestamptz's are coerced to utc from their origin timezones.
                        timezone('utc', c.cmt_author_timestamp) AS author_timestamp,
                        timezone('utc', c.cmt_committer_timestamp) AS committer_timestamp

                    FROM
                        repos r
                    JOIN commits c
                        ON r.repo_id = c.repo_id
                    WHERE
                        c.repo_id in (56361)
                        and timezone('utc', c.cmt_author_timestamp) < now()
                        and timezone('utc', c.cmt_committer_timestamp) < now()
												order by committer_timestamp desc; 
