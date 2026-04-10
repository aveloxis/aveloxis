-- NOTE: This references an Augur materialized view not yet available in Aveloxis

select repo_id, stargazers_count, unique_contributor_count, stargazers_count/unique_contributor_count::float as ratio_stargazers_to_contribs, 
    case when unique_contributor_count > 75 and stargazers_count/unique_contributor_count::float < 2 then 'club'
      when unique_contributor_count > 75 and stargazers_count/unique_contributor_count::float > 2 and stargazers_count > 1000 then 'federation'
      when unique_contributor_count < 6 and stargazers_count > 100 then 'stadium'
      when unique_contributor_count < 6 and stargazers_count < 100 then 'toy'
      else 'contribMid'
		end as nadia_label
from 
(
	SELECT x.repo_id, star_count as stargazers_count, repo_contributor_count as unique_contributor_count from 
	(select 
			aveloxis_data.repo_info.repo_id,
			MAX ( data_collection_date ) AS last_collected
	FROM 
			aveloxis_data.repo_info
	group by aveloxis_data.repo_info.repo_id 
	) recent, 
	(        SELECT repo_id, COUNT(*) AS repo_contributor_count FROM
					(
					SELECT cntrb_id, repo_id, COUNT(*) FROM explorer_contributor_actions GROUP BY cntrb_id, repo_id
					) a
					GROUP BY repo_id
					ORDER BY repo_id) contributor_data, 
	 repo_info x 
	 WHERE
			recent.repo_id = contributor_data.repo_id and 
			recent.repo_id = x.repo_id 
			and x.data_collection_date > recent.last_collected - (5 * interval '1 minute')
	GROUP BY x.repo_id, x.star_count, contributor_data.repo_contributor_count
) z 
GROUP BY repo_id, z.stargazers_count, z.unique_contributor_count
order by ratio_stargazers_to_contribs desc
;