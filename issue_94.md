https://github.com/aveloxis/aveloxis/issues/94

@rodjbs ; Looking at your specific query, I think the contributor_repo table could be named better. 
```sql
SELECT r.repo_id, r.repo_name FROM repos r INNER JOIN contributor_repo cr ON r.repo_git = cr.repo_git ;
```

I think the documentation we have here can be more clear: https://aveloxis.readthedocs.io/en/latest/schema.html 

This table is filled by polling a user's event stream. GitHub keeps this data available for approximately 30 days. It is **not** a direct connection between the contributor and repositories that are part of your collection. The software process name, "contributor breadth", I think, makes this role more clear than the docs, and especially the table name, which is confusing. I can see where people would expect "contributor_repo" to reflect a relationship between the contributor and repos in your collection scope. 

I will open a patch for this query: 
```sql
SELECT repo_id, repo_owner, repo_name, primary_language FROM aveloxis_data.repos;
```

This also clearly is empty in my test database. The patch will backfill data for existing repos and I will respond here when its ready. 

