-- NOTE: This references an Augur materialized view not yet available in Aveloxis
CREATE  UNIQUE INDEX ON aveloxis_data.api_get_all_repo_prs(repo_id);
CREATE  UNIQUE INDEX ON aveloxis_data.api_get_all_repos_commits(repo_id);

select cntrb_id, repo_id, month, year, rank, count(*) as counter from augur_new_contributors group by cntrb_id, repo_id, month, year, rank order by counter desc; 

select * from aveloxis_ops.collection_status where core_data_last_collected < '2023-06-11 00:00:00'::TIMESTAMP;

