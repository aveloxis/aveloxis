create USER pauluk WITH PASSWORD 'TTxpk98T6?bK';
GRANT CONNECT ON DATABASE augur TO pauluk;
GRANT USAGE ON SCHEMA aveloxis_data TO pauluk;
GRANT USAGE ON SCHEMA spdx TO pauluk;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO pauluk;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO pauluk; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO pauluk;

Grant select on aveloxis_ops.user_groups to pauluk; 
grant select on aveloxis_ops.user_repos to pauluk; 
grant select on aveloxis_ops.users to pauluk; 