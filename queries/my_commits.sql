select  year, repo_id, action, login, count(*) as contributor_commit_total from 
(
SELECT
  commits.cmt_ght_author_id AS ID,
	date_part( 'year' :: TEXT, ( to_timestamp( ( commits.cmt_author_date ) :: TEXT, 'YYYY-MM-DD' :: TEXT )) :: DATE ) AS year,
   --AS created_at,
  commits.repo_id,
	commits.cmt_author_email as email, 
  'commit' :: TEXT AS ACTION,
  contributors.cntrb_login AS LOGIN 
FROM
  ( aveloxis_data.commits LEFT JOIN aveloxis_data.contributors ON ( ( ( contributors.cntrb_id ) :: TEXT = ( commits.cmt_ght_author_id ) :: TEXT ) ) ) 
WHERE
  commits.cmt_author_email in ('s@goggins.com', 'outdoors@acm.org')-- = 36113 
GROUP BY
  commits.cmt_commit_hash,
  commits.cmt_ght_author_id,
  commits.repo_id,
	commits.cmt_author_email,
  date_part( 'year' :: TEXT, ( to_timestamp( ( commits.cmt_author_date ) :: TEXT, 'YYYY-MM-DD' :: TEXT )) :: DATE ),
  'commit' :: TEXT,
  contributors.cntrb_login
ORDER BY year, repo_id, login ) a 
group by 
 year, repo_id, action, login 
order by repo_id, year, action ; 


select repo_name, cntrb_category, count(*) as counter from 
(
select * from contributors, contributor_repo
where contributors.cntrb_id=contributor_repo.cntrb_id 
and contributors.gh_login='sgoggins'
order by created_at) a 
group by a.repo_name, a.cntrb_category
order by counter desc; 