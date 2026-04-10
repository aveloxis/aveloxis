create USER saratoga WITH PASSWORD 'TTxpk98T6?bK';
GRANT CONNECT ON DATABASE augur TO saratoga;
GRANT USAGE ON SCHEMA aveloxis_data TO saratoga;
GRANT USAGE ON SCHEMA spdx TO saratoga;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO saratoga;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO saratoga; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO saratoga;

Grant select on aveloxis_ops.user_groups to saratoga; 
grant select on aveloxis_ops.user_repos to saratoga; 
grant select on aveloxis_ops.users to saratoga;


create USER rak WITH PASSWORD '35jKrtB';
GRANT CONNECT ON DATABASE augur TO rak;
GRANT USAGE ON SCHEMA aveloxis_data TO rak;
GRANT USAGE ON SCHEMA aveloxis_ops TO rak;
GRANT USAGE ON SCHEMA spdx TO rak;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO rak;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO rak; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO rak;

Grant select on aveloxis_ops.user_groups to rak; 
grant select on aveloxis_ops.user_repos to rak; 
grant select on aveloxis_ops.users to rak; 

create USER kax WITH PASSWORD 'Zq*67^r';
GRANT CONNECT ON DATABASE augur TO kax;
GRANT USAGE ON SCHEMA aveloxis_data TO kax;
GRANT USAGE ON SCHEMA aveloxis_ops TO kax;
GRANT USAGE ON SCHEMA spdx TO kax;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO kax;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO kax; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO kax;

Grant select on aveloxis_ops.user_groups to kax; 
grant select on aveloxis_ops.user_repos to kax; 
grant select on aveloxis_ops.users to kax; 

CREATE USER gov WITH PASSWORD 'cableTV99!';
GRANT CONNECT ON DATABASE augur TO gov;
GRANT USAGE ON SCHEMA aveloxis_data TO gov;
GRANT USAGE ON SCHEMA spdx TO gov;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO gov;
grant select on aveloxis_ops.users to gov; 
grant select on aveloxis_ops.user_repos to gov; 
grant select on aveloxis_ops.user_groups to gov; 
grant usage on schema aveloxis_ops to gov; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO gov;  

CREATE USER remy WITH PASSWORD '8knotAugur!';
GRANT CONNECT ON DATABASE "fix-deadlock-2" TO remy;
GRANT USAGE ON SCHEMA aveloxis_data TO remy;
GRANT USAGE ON SCHEMA spdx TO remy;
GRANT SELECT ON ALL TABLES IN SCHEMA aveloxis_data TO remy;
GRANT SELECT ON ALL TABLES IN SCHEMA spdx TO remy; 
ALTER DEFAULT PRIVILEGES IN SCHEMA aveloxis_data
GRANT SELECT ON TABLES TO remy;


GRANT USAGE ON SCHEMA security TO kax;
GRANT all PRIVILEGES ON SCHEMA security TO kax;

