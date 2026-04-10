create USER wenyi WITH PASSWORD 'omTY0I9';
GRANT CONNECT ON DATABASE augur TO wenyi;
GRANT USAGE ON SCHEMA aveloxis_data TO wenyi;
GRANT USAGE ON SCHEMA aveloxis_ops TO wenyi;
GRANT USAGE ON SCHEMA spdx TO wenyi;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO wenyi;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO wenyi; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO wenyi;

Grant select on aveloxis_ops.user_groups to wenyi; 
grant select on aveloxis_ops.user_repos to wenyi; 
grant select on aveloxis_ops.users to wenyi; 