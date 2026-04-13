/*
 Navicat Premium Data Transfer

 Source Server         : postgres-17
 Source Server Type    : PostgreSQL
 Source Server Version : 170009
 Source Host           : chaoss.tv:5434
 Source Catalog        : augur
 Source Schema         : augur_operations

 Target Server Type    : PostgreSQL
 Target Server Version : 170009
 File Encoding         : 65001

 Date: 04/04/2026 15:07:31
*/


-- ----------------------------
-- Sequence structure for affiliations_corp_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."affiliations_corp_id_seq";
CREATE SEQUENCE "augur_operations"."affiliations_corp_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 620000
CACHE 1;
ALTER SEQUENCE "augur_operations"."affiliations_corp_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for augur_settings_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."augur_settings_id_seq";
CREATE SEQUENCE "augur_operations"."augur_settings_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."augur_settings_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for config_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."config_id_seq";
CREATE SEQUENCE "augur_operations"."config_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 32767
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."config_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for gh_worker_history_history_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."gh_worker_history_history_id_seq";
CREATE SEQUENCE "augur_operations"."gh_worker_history_history_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."gh_worker_history_history_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for subscription_types_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."subscription_types_id_seq";
CREATE SEQUENCE "augur_operations"."subscription_types_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."subscription_types_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for user_groups_group_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."user_groups_group_id_seq";
CREATE SEQUENCE "augur_operations"."user_groups_group_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."user_groups_group_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for users_user_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."users_user_id_seq";
CREATE SEQUENCE "augur_operations"."users_user_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;
ALTER SEQUENCE "augur_operations"."users_user_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for worker_oauth_oauth_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_operations"."worker_oauth_oauth_id_seq";
CREATE SEQUENCE "augur_operations"."worker_oauth_oauth_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1000
CACHE 1;
ALTER SEQUENCE "augur_operations"."worker_oauth_oauth_id_seq" OWNER TO "augur";

-- ----------------------------
-- Table structure for all
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."all";
CREATE TABLE "augur_operations"."all" (
  "Name" varchar COLLATE "pg_catalog"."default",
  "Bytes" varchar COLLATE "pg_catalog"."default",
  "Lines" varchar COLLATE "pg_catalog"."default",
  "Code" varchar COLLATE "pg_catalog"."default",
  "Comment" varchar COLLATE "pg_catalog"."default",
  "Blank" varchar COLLATE "pg_catalog"."default",
  "Complexity" varchar COLLATE "pg_catalog"."default",
  "Count" varchar COLLATE "pg_catalog"."default",
  "WeightedComplexity" varchar COLLATE "pg_catalog"."default",
  "Files" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_operations"."all" OWNER TO "augur";

-- ----------------------------
-- Table structure for augur_settings
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."augur_settings";
CREATE TABLE "augur_operations"."augur_settings" (
  "id" int8 NOT NULL DEFAULT nextval('"augur_operations".augur_settings_id_seq'::regclass),
  "setting" varchar COLLATE "pg_catalog"."default",
  "value" varchar COLLATE "pg_catalog"."default",
  "last_modified" timestamp(0) DEFAULT CURRENT_DATE
)
;
ALTER TABLE "augur_operations"."augur_settings" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."augur_settings" IS 'Augur settings include the schema version, and the Augur API Key as of 10/25/2020. Future augur settings may be stored in this table, which has the basic structure of a name-value pair. ';

-- ----------------------------
-- Table structure for client_applications
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."client_applications";
CREATE TABLE "augur_operations"."client_applications" (
  "id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "api_key" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "user_id" int4 NOT NULL,
  "name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "redirect_url" varchar COLLATE "pg_catalog"."default" NOT NULL
)
;
ALTER TABLE "augur_operations"."client_applications" OWNER TO "augur";

-- ----------------------------
-- Table structure for collection_status
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."collection_status";
CREATE TABLE "augur_operations"."collection_status" (
  "repo_id" int8 NOT NULL,
  "core_data_last_collected" timestamp(6),
  "core_status" varchar COLLATE "pg_catalog"."default" NOT NULL DEFAULT 'Pending'::character varying,
  "core_task_id" varchar COLLATE "pg_catalog"."default",
  "secondary_data_last_collected" timestamp(6),
  "secondary_status" varchar COLLATE "pg_catalog"."default" NOT NULL DEFAULT 'Pending'::character varying,
  "secondary_task_id" varchar COLLATE "pg_catalog"."default",
  "event_last_collected" timestamp(6),
  "facade_status" varchar COLLATE "pg_catalog"."default" NOT NULL DEFAULT 'Pending'::character varying,
  "facade_data_last_collected" timestamp(6),
  "facade_task_id" varchar COLLATE "pg_catalog"."default",
  "core_weight" int8,
  "facade_weight" int8,
  "secondary_weight" int8,
  "issue_pr_sum" int8,
  "commit_sum" int8,
  "ml_status" varchar COLLATE "pg_catalog"."default" NOT NULL DEFAULT 'Pending'::character varying,
  "ml_data_last_collected" timestamp(6),
  "ml_task_id" varchar COLLATE "pg_catalog"."default",
  "ml_weight" int8
)
;
ALTER TABLE "augur_operations"."collection_status" OWNER TO "augur";

-- ----------------------------
-- Table structure for config
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."config";
CREATE TABLE "augur_operations"."config" (
  "id" int2 NOT NULL DEFAULT nextval('"augur_operations".config_id_seq'::regclass),
  "section_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "setting_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "value" varchar COLLATE "pg_catalog"."default",
  "type" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_operations"."config" OWNER TO "augur";

-- ----------------------------
-- Table structure for github_users
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."github_users";
CREATE TABLE "augur_operations"."github_users" (
  "login" varchar(255) COLLATE "pg_catalog"."default",
  "email" varchar(255) COLLATE "pg_catalog"."default",
  "affiliation" varchar(255) COLLATE "pg_catalog"."default",
  "source" varchar(255) COLLATE "pg_catalog"."default",
  "commits" varchar(255) COLLATE "pg_catalog"."default",
  "location" varchar(255) COLLATE "pg_catalog"."default",
  "country_id" varchar(255) COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_operations"."github_users" OWNER TO "augur";

-- ----------------------------
-- Table structure for github_users_2
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."github_users_2";
CREATE TABLE "augur_operations"."github_users_2" (
  "login" varchar COLLATE "pg_catalog"."default",
  "email" varchar COLLATE "pg_catalog"."default",
  "affiliation" varchar COLLATE "pg_catalog"."default",
  "source" varchar COLLATE "pg_catalog"."default",
  "commits" varchar COLLATE "pg_catalog"."default",
  "location" varchar COLLATE "pg_catalog"."default",
  "country_id" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_operations"."github_users_2" OWNER TO "augur";

-- ----------------------------
-- Table structure for network_weighted_commits
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."network_weighted_commits";
CREATE TABLE "augur_operations"."network_weighted_commits" (
  "repo_id" int8,
  "cntrb_id" uuid,
  "weight" float8,
  "action_type" varchar COLLATE "pg_catalog"."default",
  "user_collection" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."network_weighted_commits" OWNER TO "augur";

-- ----------------------------
-- Table structure for network_weighted_issues
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."network_weighted_issues";
CREATE TABLE "augur_operations"."network_weighted_issues" (
  "repo_id" int8,
  "cntrb_id" uuid,
  "weight" float8,
  "action_type" varchar COLLATE "pg_catalog"."default",
  "user_collection" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."network_weighted_issues" OWNER TO "augur";

-- ----------------------------
-- Table structure for network_weighted_pr_reviews
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."network_weighted_pr_reviews";
CREATE TABLE "augur_operations"."network_weighted_pr_reviews" (
  "repo_id" int8,
  "cntrb_id" uuid,
  "weight" float8,
  "action_type" varchar COLLATE "pg_catalog"."default",
  "user_collection" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."network_weighted_pr_reviews" OWNER TO "augur";

-- ----------------------------
-- Table structure for network_weighted_prs
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."network_weighted_prs";
CREATE TABLE "augur_operations"."network_weighted_prs" (
  "repo_id" int8,
  "cntrb_id" uuid,
  "weight" float8,
  "action_type" varchar COLLATE "pg_catalog"."default",
  "user_collection" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."network_weighted_prs" OWNER TO "augur";

-- ----------------------------
-- Table structure for refresh_tokens
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."refresh_tokens";
CREATE TABLE "augur_operations"."refresh_tokens" (
  "id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "user_session_token" varchar COLLATE "pg_catalog"."default" NOT NULL
)
;
ALTER TABLE "augur_operations"."refresh_tokens" OWNER TO "augur";

-- ----------------------------
-- Table structure for repos_fetch_log
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."repos_fetch_log";
CREATE TABLE "augur_operations"."repos_fetch_log" (
  "repos_id" int4 NOT NULL,
  "status" varchar(128) COLLATE "pg_catalog"."default" NOT NULL,
  "date" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."repos_fetch_log" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."repos_fetch_log" IS 'For future use when we move all working tables to the augur_operations schema. ';

-- ----------------------------
-- Table structure for subscription_types
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."subscription_types";
CREATE TABLE "augur_operations"."subscription_types" (
  "id" int8 NOT NULL DEFAULT nextval('"augur_operations".subscription_types_id_seq'::regclass),
  "name" varchar COLLATE "pg_catalog"."default" NOT NULL
)
;
ALTER TABLE "augur_operations"."subscription_types" OWNER TO "augur";

-- ----------------------------
-- Table structure for subscriptions
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."subscriptions";
CREATE TABLE "augur_operations"."subscriptions" (
  "application_id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "type_id" int8 NOT NULL
)
;
ALTER TABLE "augur_operations"."subscriptions" OWNER TO "augur";

-- ----------------------------
-- Table structure for user_groups
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."user_groups";
CREATE TABLE "augur_operations"."user_groups" (
  "group_id" int8 NOT NULL DEFAULT nextval('"augur_operations".user_groups_group_id_seq'::regclass),
  "user_id" int4 NOT NULL,
  "name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "favorited" bool NOT NULL DEFAULT false
)
;
ALTER TABLE "augur_operations"."user_groups" OWNER TO "augur";

-- ----------------------------
-- Table structure for user_repos
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."user_repos";
CREATE TABLE "augur_operations"."user_repos" (
  "repo_id" int8 NOT NULL,
  "group_id" int8 NOT NULL
)
;
ALTER TABLE "augur_operations"."user_repos" OWNER TO "augur";

-- ----------------------------
-- Table structure for user_session_tokens
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."user_session_tokens";
CREATE TABLE "augur_operations"."user_session_tokens" (
  "token" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "user_id" int4 NOT NULL,
  "created_at" int8,
  "expiration" int8,
  "application_id" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_operations"."user_session_tokens" OWNER TO "augur";

-- ----------------------------
-- Table structure for users
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."users";
CREATE TABLE "augur_operations"."users" (
  "user_id" int4 NOT NULL DEFAULT nextval('"augur_operations".users_user_id_seq'::regclass),
  "login_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "login_hashword" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "text_phone" varchar COLLATE "pg_catalog"."default",
  "first_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "last_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "admin" bool NOT NULL,
  "email_verified" bool NOT NULL DEFAULT false
)
;
ALTER TABLE "augur_operations"."users" OWNER TO "augur";

-- ----------------------------
-- Table structure for worker_history
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."worker_history";
CREATE TABLE "augur_operations"."worker_history" (
  "history_id" int8 NOT NULL DEFAULT nextval('"augur_operations".gh_worker_history_history_id_seq'::regclass),
  "repo_id" int8,
  "worker" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "job_model" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "oauth_id" int4,
  "timestamp" timestamp(0) NOT NULL,
  "status" varchar(7) COLLATE "pg_catalog"."default" NOT NULL,
  "total_results" int4
)
;
ALTER TABLE "augur_operations"."worker_history" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."worker_history" IS 'This table stores the complete history of job execution, including success and failure. It is useful for troubleshooting. ';

-- ----------------------------
-- Table structure for worker_job
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."worker_job";
CREATE TABLE "augur_operations"."worker_job" (
  "job_model" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "state" int4 NOT NULL DEFAULT 0,
  "zombie_head" int4,
  "since_id_str" varchar(255) COLLATE "pg_catalog"."default" NOT NULL DEFAULT '0'::character varying,
  "description" varchar(255) COLLATE "pg_catalog"."default" DEFAULT 'None'::character varying,
  "last_count" int4,
  "last_run" timestamp(0) DEFAULT NULL::timestamp without time zone,
  "analysis_state" int4 DEFAULT 0,
  "oauth_id" int4 NOT NULL
)
;
ALTER TABLE "augur_operations"."worker_job" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."worker_job" IS 'This table stores the jobs workers collect data for. A job is found in the code, and in the augur.config.json under the construct of a “model”. ';

-- ----------------------------
-- Table structure for worker_oauth
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."worker_oauth";
CREATE TABLE "augur_operations"."worker_oauth" (
  "oauth_id" int8 NOT NULL DEFAULT nextval('"augur_operations".worker_oauth_oauth_id_seq'::regclass),
  "name" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "consumer_key" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "consumer_secret" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "access_token" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "access_token_secret" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "repo_directory" varchar COLLATE "pg_catalog"."default",
  "platform" varchar COLLATE "pg_catalog"."default" DEFAULT 'github'::character varying
)
;
ALTER TABLE "augur_operations"."worker_oauth" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."worker_oauth" IS 'This table stores credentials for retrieving data from platform API’s. Entries in this table must comply with the terms of service for each platform. ';

-- ----------------------------
-- Table structure for worker_oauth_copy1
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."worker_oauth_copy1";
CREATE TABLE "augur_operations"."worker_oauth_copy1" (
  "oauth_id" int8 NOT NULL DEFAULT nextval('"augur_operations".worker_oauth_oauth_id_seq'::regclass),
  "name" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "consumer_key" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "consumer_secret" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "access_token" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "access_token_secret" varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
  "repo_directory" varchar COLLATE "pg_catalog"."default",
  "platform" varchar COLLATE "pg_catalog"."default" DEFAULT 'github'::character varying
)
;
ALTER TABLE "augur_operations"."worker_oauth_copy1" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."worker_oauth_copy1" IS 'This table stores credentials for retrieving data from platform API’s. Entries in this table must comply with the terms of service for each platform. ';

-- ----------------------------
-- Table structure for worker_settings_facade
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."worker_settings_facade";
CREATE TABLE "augur_operations"."worker_settings_facade" (
  "id" int4 NOT NULL,
  "setting" varchar(32) COLLATE "pg_catalog"."default" NOT NULL,
  "value" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "last_modified" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_operations"."worker_settings_facade" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."worker_settings_facade" IS 'For future use when we move all working tables to the augur_operations schema. ';

-- ----------------------------
-- Table structure for working_commits
-- ----------------------------
DROP TABLE IF EXISTS "augur_operations"."working_commits";
CREATE TABLE "augur_operations"."working_commits" (
  "repos_id" int4 NOT NULL,
  "working_commit" varchar(40) COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying
)
;
ALTER TABLE "augur_operations"."working_commits" OWNER TO "augur";
COMMENT ON TABLE "augur_operations"."working_commits" IS 'For future use when we move all working tables to the augur_operations schema. ';

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_operations"."affiliations_corp_id_seq"', 620001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_operations"."augur_settings_id_seq"', 10002, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_operations"."config_id_seq"
OWNED BY "augur_operations"."config"."id";
SELECT setval('"augur_operations"."config_id_seq"', 10064, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_operations"."gh_worker_history_history_id_seq"', 10121, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_operations"."subscription_types_id_seq"
OWNED BY "augur_operations"."subscription_types"."id";
SELECT setval('"augur_operations"."subscription_types_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_operations"."user_groups_group_id_seq"
OWNED BY "augur_operations"."user_groups"."group_id";
SELECT setval('"augur_operations"."user_groups_group_id_seq"', 10302, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_operations"."users_user_id_seq"
OWNED BY "augur_operations"."users"."user_id";
SELECT setval('"augur_operations"."users_user_id_seq"', 10119, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_operations"."worker_oauth_oauth_id_seq"', 10001, false);

-- ----------------------------
-- Primary Key structure for table augur_settings
-- ----------------------------
ALTER TABLE "augur_operations"."augur_settings" ADD CONSTRAINT "augur_settings_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table client_applications
-- ----------------------------
ALTER TABLE "augur_operations"."client_applications" ADD CONSTRAINT "client_applications_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Checks structure for table collection_status
-- ----------------------------
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "core_secondary_dependency_check" CHECK (NOT (core_status::text = 'Pending'::text AND secondary_status::text = 'Collecting'::text));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "core_task_id_check" CHECK (NOT (core_task_id IS NOT NULL AND (core_status::text = ANY (ARRAY['Pending'::character varying::text, 'Success'::character varying::text, 'Error'::character varying::text]))) AND NOT (core_task_id IS NULL AND core_status::text = 'Collecting'::text));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "facade_data_last_collected_check" CHECK (NOT (facade_data_last_collected IS NULL AND facade_status::text = 'Success'::text) AND NOT (facade_data_last_collected IS NOT NULL AND (facade_status::text = ANY (ARRAY['Pending'::character varying::text, 'Initializing'::character varying::text]))));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "facade_task_id_check" CHECK (NOT (facade_task_id IS NOT NULL AND (facade_status::text = ANY (ARRAY['Pending'::character varying::text, 'Success'::character varying::text, 'Error'::character varying::text, 'Failed Clone'::character varying::text, 'Initializing'::character varying::text]))) AND NOT (facade_task_id IS NULL AND facade_status::text = 'Collecting'::text));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "secondary_data_last_collected_check" CHECK (NOT (secondary_data_last_collected IS NULL AND secondary_status::text = 'Success'::text) AND NOT (secondary_data_last_collected IS NOT NULL AND secondary_status::text = 'Pending'::text));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "secondary_task_id_check" CHECK (NOT (secondary_task_id IS NOT NULL AND (secondary_status::text = ANY (ARRAY['Pending'::character varying::text, 'Success'::character varying::text, 'Error'::character varying::text]))) AND NOT (secondary_task_id IS NULL AND secondary_status::text = 'Collecting'::text));
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "core_data_last_collected_check" CHECK (NOT (core_data_last_collected IS NULL AND core_status::text = 'Success'::text) AND NOT (core_data_last_collected IS NOT NULL AND core_status::text = 'Pending'::text));

-- ----------------------------
-- Primary Key structure for table collection_status
-- ----------------------------
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "collection_status_pkey" PRIMARY KEY ("repo_id");

-- ----------------------------
-- Uniques structure for table config
-- ----------------------------
ALTER TABLE "augur_operations"."config" ADD CONSTRAINT "unique-config-setting" UNIQUE ("section_name", "setting_name");

-- ----------------------------
-- Primary Key structure for table config
-- ----------------------------
ALTER TABLE "augur_operations"."config" ADD CONSTRAINT "config_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Uniques structure for table refresh_tokens
-- ----------------------------
ALTER TABLE "augur_operations"."refresh_tokens" ADD CONSTRAINT "refresh_token_user_session_token_id_unique" UNIQUE ("user_session_token");

-- ----------------------------
-- Primary Key structure for table refresh_tokens
-- ----------------------------
ALTER TABLE "augur_operations"."refresh_tokens" ADD CONSTRAINT "refresh_tokens_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Indexes structure for table repos_fetch_log
-- ----------------------------
CREATE INDEX "repos_id,statusops" ON "augur_operations"."repos_fetch_log" USING btree (
  "repos_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "status" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table subscription_types
-- ----------------------------
ALTER TABLE "augur_operations"."subscription_types" ADD CONSTRAINT "subscription_type_title_unique" UNIQUE ("name");

-- ----------------------------
-- Primary Key structure for table subscription_types
-- ----------------------------
ALTER TABLE "augur_operations"."subscription_types" ADD CONSTRAINT "subscription_types_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table subscriptions
-- ----------------------------
ALTER TABLE "augur_operations"."subscriptions" ADD CONSTRAINT "subscriptions_pkey" PRIMARY KEY ("application_id", "type_id");

-- ----------------------------
-- Indexes structure for table user_groups
-- ----------------------------
CREATE INDEX "idx_user_groups_user_group" ON "augur_operations"."user_groups" USING btree (
  "user_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "group_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table user_groups
-- ----------------------------
ALTER TABLE "augur_operations"."user_groups" ADD CONSTRAINT "user_groups_user_id_name_key" UNIQUE ("user_id", "name");

-- ----------------------------
-- Primary Key structure for table user_groups
-- ----------------------------
ALTER TABLE "augur_operations"."user_groups" ADD CONSTRAINT "user_groups_pkey" PRIMARY KEY ("group_id");

-- ----------------------------
-- Indexes structure for table user_repos
-- ----------------------------
CREATE INDEX "idx_user_repos_group_repo" ON "augur_operations"."user_repos" USING btree (
  "group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table user_repos
-- ----------------------------
ALTER TABLE "augur_operations"."user_repos" ADD CONSTRAINT "user_repos_pkey" PRIMARY KEY ("group_id", "repo_id");

-- ----------------------------
-- Primary Key structure for table user_session_tokens
-- ----------------------------
ALTER TABLE "augur_operations"."user_session_tokens" ADD CONSTRAINT "user_session_tokens_pkey" PRIMARY KEY ("token");

-- ----------------------------
-- Uniques structure for table users
-- ----------------------------
ALTER TABLE "augur_operations"."users" ADD CONSTRAINT "user-unique-email" UNIQUE ("email");
ALTER TABLE "augur_operations"."users" ADD CONSTRAINT "user-unique-name" UNIQUE ("login_name");
ALTER TABLE "augur_operations"."users" ADD CONSTRAINT "user-unique-phone" UNIQUE ("text_phone");

-- ----------------------------
-- Primary Key structure for table users
-- ----------------------------
ALTER TABLE "augur_operations"."users" ADD CONSTRAINT "users_pkey" PRIMARY KEY ("user_id");

-- ----------------------------
-- Primary Key structure for table worker_history
-- ----------------------------
ALTER TABLE "augur_operations"."worker_history" ADD CONSTRAINT "history_pkey" PRIMARY KEY ("history_id");

-- ----------------------------
-- Primary Key structure for table worker_job
-- ----------------------------
ALTER TABLE "augur_operations"."worker_job" ADD CONSTRAINT "job_pkey" PRIMARY KEY ("job_model");

-- ----------------------------
-- Primary Key structure for table worker_oauth
-- ----------------------------
ALTER TABLE "augur_operations"."worker_oauth" ADD CONSTRAINT "worker_oauth_pkey" PRIMARY KEY ("oauth_id");

-- ----------------------------
-- Primary Key structure for table worker_oauth_copy1
-- ----------------------------
ALTER TABLE "augur_operations"."worker_oauth_copy1" ADD CONSTRAINT "worker_oauth_copy1_pkey" PRIMARY KEY ("oauth_id");

-- ----------------------------
-- Primary Key structure for table worker_settings_facade
-- ----------------------------
ALTER TABLE "augur_operations"."worker_settings_facade" ADD CONSTRAINT "settings_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Foreign Keys structure for table client_applications
-- ----------------------------
ALTER TABLE "augur_operations"."client_applications" ADD CONSTRAINT "client_application_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "augur_operations"."users" ("user_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table collection_status
-- ----------------------------
ALTER TABLE "augur_operations"."collection_status" ADD CONSTRAINT "collection_status_repo_id_fk" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table refresh_tokens
-- ----------------------------
ALTER TABLE "augur_operations"."refresh_tokens" ADD CONSTRAINT "refresh_token_session_token_id_fkey" FOREIGN KEY ("user_session_token") REFERENCES "augur_operations"."user_session_tokens" ("token") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table subscriptions
-- ----------------------------
ALTER TABLE "augur_operations"."subscriptions" ADD CONSTRAINT "subscriptions_application_id_fkey" FOREIGN KEY ("application_id") REFERENCES "augur_operations"."client_applications" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_operations"."subscriptions" ADD CONSTRAINT "subscriptions_type_id_fkey" FOREIGN KEY ("type_id") REFERENCES "augur_operations"."subscription_types" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table user_groups
-- ----------------------------
ALTER TABLE "augur_operations"."user_groups" ADD CONSTRAINT "user_groups_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "augur_operations"."users" ("user_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table user_repos
-- ----------------------------
ALTER TABLE "augur_operations"."user_repos" ADD CONSTRAINT "user_repos_group_id_fkey" FOREIGN KEY ("group_id") REFERENCES "augur_operations"."user_groups" ("group_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_operations"."user_repos" ADD CONSTRAINT "user_repos_repo_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table user_session_tokens
-- ----------------------------
ALTER TABLE "augur_operations"."user_session_tokens" ADD CONSTRAINT "user_session_token_application_id_fkey" FOREIGN KEY ("application_id") REFERENCES "augur_operations"."client_applications" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_operations"."user_session_tokens" ADD CONSTRAINT "user_session_token_user_fk" FOREIGN KEY ("user_id") REFERENCES "augur_operations"."users" ("user_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
