create USER troy WITH PASSWORD 'TTxpk98T6?bK';
GRANT CONNECT ON DATABASE augur TO troy;
GRANT USAGE ON SCHEMA aveloxis_data TO troy;
GRANT USAGE ON SCHEMA spdx TO troy;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO troy;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO troy; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO troy;

Grant select on aveloxis_ops.user_groups to troy; 
grant select on aveloxis_ops.user_repos to troy; 
grant select on aveloxis_ops.users to troy; 