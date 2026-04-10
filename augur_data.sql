/*
 Navicat Premium Data Transfer

 Source Server         : postgres-17
 Source Server Type    : PostgreSQL
 Source Server Version : 170009
 Source Host           : chaoss.tv:5434
 Source Catalog        : augur
 Source Schema         : augur_data

 Target Server Type    : PostgreSQL
 Target Server Version : 170009
 File Encoding         : 65001

 Date: 04/04/2026 15:06:49
*/


-- ----------------------------
-- Sequence structure for augur_data.repo_insights_ri_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."augur_data.repo_insights_ri_id_seq";
CREATE SEQUENCE "augur_data"."augur_data.repo_insights_ri_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."augur_data.repo_insights_ri_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for chaoss_metric_status_cms_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."chaoss_metric_status_cms_id_seq";
CREATE SEQUENCE "augur_data"."chaoss_metric_status_cms_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."chaoss_metric_status_cms_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for chaoss_user_chaoss_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."chaoss_user_chaoss_id_seq";
CREATE SEQUENCE "augur_data"."chaoss_user_chaoss_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."chaoss_user_chaoss_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for commit_comment_ref_cmt_comment_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."commit_comment_ref_cmt_comment_id_seq";
CREATE SEQUENCE "augur_data"."commit_comment_ref_cmt_comment_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."commit_comment_ref_cmt_comment_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for commit_parents_parent_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."commit_parents_parent_id_seq";
CREATE SEQUENCE "augur_data"."commit_parents_parent_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."commit_parents_parent_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for commits_cmt_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."commits_cmt_id_seq";
CREATE SEQUENCE "augur_data"."commits_cmt_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."commits_cmt_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributor_affiliations_ca_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributor_affiliations_ca_id_seq";
CREATE SEQUENCE "augur_data"."contributor_affiliations_ca_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."contributor_affiliations_ca_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributor_repo_cntrb_repo_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributor_repo_cntrb_repo_id_seq";
CREATE SEQUENCE "augur_data"."contributor_repo_cntrb_repo_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."contributor_repo_cntrb_repo_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributors_aliases_cntrb_a_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributors_aliases_cntrb_a_id_seq";
CREATE SEQUENCE "augur_data"."contributors_aliases_cntrb_a_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."contributors_aliases_cntrb_a_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributors_aliases_cntrb_alias_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributors_aliases_cntrb_alias_id_seq";
CREATE SEQUENCE "augur_data"."contributors_aliases_cntrb_alias_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."contributors_aliases_cntrb_alias_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributors_cntrb_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributors_cntrb_id_seq";
CREATE SEQUENCE "augur_data"."contributors_cntrb_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."contributors_cntrb_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for contributors_history_cntrb_history_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."contributors_history_cntrb_history_id_seq";
CREATE SEQUENCE "augur_data"."contributors_history_cntrb_history_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."contributors_history_cntrb_history_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for dei_badging_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."dei_badging_id_seq";
CREATE SEQUENCE "augur_data"."dei_badging_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."dei_badging_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for discourse_insights_msg_discourse_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."discourse_insights_msg_discourse_id_seq";
CREATE SEQUENCE "augur_data"."discourse_insights_msg_discourse_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."discourse_insights_msg_discourse_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for discourse_insights_msg_discourse_id_seq1
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."discourse_insights_msg_discourse_id_seq1";
CREATE SEQUENCE "augur_data"."discourse_insights_msg_discourse_id_seq1" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."discourse_insights_msg_discourse_id_seq1" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for issue_assignees_issue_assignee_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."issue_assignees_issue_assignee_id_seq";
CREATE SEQUENCE "augur_data"."issue_assignees_issue_assignee_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."issue_assignees_issue_assignee_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for issue_events_event_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."issue_events_event_id_seq";
CREATE SEQUENCE "augur_data"."issue_events_event_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."issue_events_event_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for issue_labels_issue_label_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."issue_labels_issue_label_id_seq";
CREATE SEQUENCE "augur_data"."issue_labels_issue_label_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."issue_labels_issue_label_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for issue_message_ref_issue_msg_ref_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."issue_message_ref_issue_msg_ref_id_seq";
CREATE SEQUENCE "augur_data"."issue_message_ref_issue_msg_ref_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."issue_message_ref_issue_msg_ref_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for issue_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."issue_seq";
CREATE SEQUENCE "augur_data"."issue_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 31000
CACHE 1;
ALTER SEQUENCE "augur_data"."issue_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for libraries_library_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."libraries_library_id_seq";
CREATE SEQUENCE "augur_data"."libraries_library_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."libraries_library_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for library_dependencies_lib_dependency_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."library_dependencies_lib_dependency_id_seq";
CREATE SEQUENCE "augur_data"."library_dependencies_lib_dependency_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."library_dependencies_lib_dependency_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for library_version_library_version_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."library_version_library_version_id_seq";
CREATE SEQUENCE "augur_data"."library_version_library_version_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."library_version_library_version_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for lstm_anomaly_models_model_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."lstm_anomaly_models_model_id_seq";
CREATE SEQUENCE "augur_data"."lstm_anomaly_models_model_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."lstm_anomaly_models_model_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for lstm_anomaly_results_result_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."lstm_anomaly_results_result_id_seq";
CREATE SEQUENCE "augur_data"."lstm_anomaly_results_result_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."lstm_anomaly_results_result_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for message_analysis_msg_analysis_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."message_analysis_msg_analysis_id_seq";
CREATE SEQUENCE "augur_data"."message_analysis_msg_analysis_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."message_analysis_msg_analysis_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for message_analysis_summary_msg_summary_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."message_analysis_summary_msg_summary_id_seq";
CREATE SEQUENCE "augur_data"."message_analysis_summary_msg_summary_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."message_analysis_summary_msg_summary_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for message_msg_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."message_msg_id_seq";
CREATE SEQUENCE "augur_data"."message_msg_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."message_msg_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for message_sentiment_msg_analysis_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."message_sentiment_msg_analysis_id_seq";
CREATE SEQUENCE "augur_data"."message_sentiment_msg_analysis_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."message_sentiment_msg_analysis_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for message_sentiment_summary_msg_summary_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."message_sentiment_summary_msg_summary_id_seq";
CREATE SEQUENCE "augur_data"."message_sentiment_summary_msg_summary_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."message_sentiment_summary_msg_summary_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for platform_pltfrm_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."platform_pltfrm_id_seq";
CREATE SEQUENCE "augur_data"."platform_pltfrm_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."platform_pltfrm_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_analysis_pull_request_analysis_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_analysis_pull_request_analysis_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_analysis_pull_request_analysis_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_analysis_pull_request_analysis_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_assignees_pr_assignee_map_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_assignees_pr_assignee_map_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_assignees_pr_assignee_map_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_assignees_pr_assignee_map_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_commits_pr_cmt_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_commits_pr_cmt_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_commits_pr_cmt_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_commits_pr_cmt_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_events_pr_event_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_events_pr_event_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_events_pr_event_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_events_pr_event_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_files_pr_file_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_files_pr_file_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_files_pr_file_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25150
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_files_pr_file_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_labels_pr_label_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_labels_pr_label_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_labels_pr_label_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_labels_pr_label_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_message_ref_pr_msg_ref_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_message_ref_pr_msg_ref_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_message_ref_pr_msg_ref_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_message_ref_pr_msg_ref_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_meta_pr_repo_meta_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_meta_pr_repo_meta_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_meta_pr_repo_meta_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_meta_pr_repo_meta_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_repo_pr_repo_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_repo_pr_repo_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_repo_pr_repo_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_repo_pr_repo_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_review_message_ref_pr_review_msg_ref_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_review_message_ref_pr_review_msg_ref_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_review_message_ref_pr_review_msg_ref_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_review_message_ref_pr_review_msg_ref_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_reviewers_pr_reviewer_map_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_reviewers_pr_reviewer_map_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_reviewers_pr_reviewer_map_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_reviewers_pr_reviewer_map_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_reviews_pr_review_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_reviews_pr_review_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_reviews_pr_review_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_reviews_pr_review_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_request_teams_pr_team_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_request_teams_pr_team_id_seq";
CREATE SEQUENCE "augur_data"."pull_request_teams_pr_team_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_request_teams_pr_team_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for pull_requests_pull_request_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."pull_requests_pull_request_id_seq";
CREATE SEQUENCE "augur_data"."pull_requests_pull_request_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."pull_requests_pull_request_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for releases_release_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."releases_release_id_seq";
CREATE SEQUENCE "augur_data"."releases_release_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."releases_release_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_badging_badge_collection_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_badging_badge_collection_id_seq";
CREATE SEQUENCE "augur_data"."repo_badging_badge_collection_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25012
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_badging_badge_collection_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_clones_data_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_clones_data_id_seq";
CREATE SEQUENCE "augur_data"."repo_clones_data_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_clones_data_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_cluster_messages_msg_cluster_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_cluster_messages_msg_cluster_id_seq";
CREATE SEQUENCE "augur_data"."repo_cluster_messages_msg_cluster_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_cluster_messages_msg_cluster_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_dependencies_repo_dependencies_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_dependencies_repo_dependencies_id_seq";
CREATE SEQUENCE "augur_data"."repo_dependencies_repo_dependencies_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_dependencies_repo_dependencies_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_deps_libyear_repo_deps_libyear_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_deps_libyear_repo_deps_libyear_id_seq";
CREATE SEQUENCE "augur_data"."repo_deps_libyear_repo_deps_libyear_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_deps_libyear_repo_deps_libyear_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_deps_scorecard_repo_deps_scorecard_id_seq1
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_deps_scorecard_repo_deps_scorecard_id_seq1";
CREATE SEQUENCE "augur_data"."repo_deps_scorecard_repo_deps_scorecard_id_seq1" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_deps_scorecard_repo_deps_scorecard_id_seq1" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_group_insights_rgi_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_group_insights_rgi_id_seq";
CREATE SEQUENCE "augur_data"."repo_group_insights_rgi_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_group_insights_rgi_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_groups_list_serve_rgls_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_groups_list_serve_rgls_id_seq";
CREATE SEQUENCE "augur_data"."repo_groups_list_serve_rgls_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_groups_list_serve_rgls_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_groups_repo_group_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_groups_repo_group_id_seq";
CREATE SEQUENCE "augur_data"."repo_groups_repo_group_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_groups_repo_group_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_info_repo_info_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_info_repo_info_id_seq";
CREATE SEQUENCE "augur_data"."repo_info_repo_info_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_info_repo_info_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_insights_records_ri_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_insights_records_ri_id_seq";
CREATE SEQUENCE "augur_data"."repo_insights_records_ri_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_insights_records_ri_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_insights_ri_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_insights_ri_id_seq";
CREATE SEQUENCE "augur_data"."repo_insights_ri_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_insights_ri_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_labor_repo_labor_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_labor_repo_labor_id_seq";
CREATE SEQUENCE "augur_data"."repo_labor_repo_labor_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_labor_repo_labor_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_meta_rmeta_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_meta_rmeta_id_seq";
CREATE SEQUENCE "augur_data"."repo_meta_rmeta_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_meta_rmeta_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_repo_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_repo_id_seq";
CREATE SEQUENCE "augur_data"."repo_repo_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_repo_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_sbom_scans_rsb_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_sbom_scans_rsb_id_seq";
CREATE SEQUENCE "augur_data"."repo_sbom_scans_rsb_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_sbom_scans_rsb_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_stats_rstat_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_stats_rstat_id_seq";
CREATE SEQUENCE "augur_data"."repo_stats_rstat_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 25430
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_stats_rstat_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_test_coverage_repo_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_test_coverage_repo_id_seq";
CREATE SEQUENCE "augur_data"."repo_test_coverage_repo_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_test_coverage_repo_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for repo_topic_repo_topic_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."repo_topic_repo_topic_id_seq";
CREATE SEQUENCE "augur_data"."repo_topic_repo_topic_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."repo_topic_repo_topic_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for topic_model_event_event_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."topic_model_event_event_id_seq";
CREATE SEQUENCE "augur_data"."topic_model_event_event_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."topic_model_event_event_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for topic_words_topic_words_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."topic_words_topic_words_id_seq";
CREATE SEQUENCE "augur_data"."topic_words_topic_words_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."topic_words_topic_words_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for unresolved_commit_emails_email_unresolved_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."unresolved_commit_emails_email_unresolved_id_seq";
CREATE SEQUENCE "augur_data"."unresolved_commit_emails_email_unresolved_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."unresolved_commit_emails_email_unresolved_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for utility_log_id_seq
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."utility_log_id_seq";
CREATE SEQUENCE "augur_data"."utility_log_id_seq" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."utility_log_id_seq" OWNER TO "augur";

-- ----------------------------
-- Sequence structure for utility_log_id_seq1
-- ----------------------------
DROP SEQUENCE IF EXISTS "augur_data"."utility_log_id_seq1";
CREATE SEQUENCE "augur_data"."utility_log_id_seq1" 
INCREMENT 1
MINVALUE  1
MAXVALUE 9223372036854775807
START 1
CACHE 1;
ALTER SEQUENCE "augur_data"."utility_log_id_seq1" OWNER TO "augur";

-- ----------------------------
-- Table structure for _transfer_testing
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."_transfer_testing";
CREATE TABLE "augur_data"."_transfer_testing" (
  "repo_id" int8 NOT NULL
)
;
ALTER TABLE "augur_data"."_transfer_testing" OWNER TO "augur";

-- ----------------------------
-- Table structure for _transfer_training
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."_transfer_training";
CREATE TABLE "augur_data"."_transfer_training" (
  "repo_id" int8
)
;
ALTER TABLE "augur_data"."_transfer_training" OWNER TO "augur";

-- ----------------------------
-- Table structure for akl;fjlk;a
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."akl;fjlk;a";
CREATE TABLE "augur_data"."akl;fjlk;a" (
  "id" int4 NOT NULL DEFAULT nextval('"augur_data".dei_badging_id_seq'::regclass),
  "badging_id" int4 NOT NULL,
  "level" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "repo_id" int8 NOT NULL
)
;
ALTER TABLE "augur_data"."akl;fjlk;a" OWNER TO "augur";

-- ----------------------------
-- Table structure for analysis_log
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."analysis_log";
CREATE TABLE "augur_data"."analysis_log" (
  "repos_id" int4 NOT NULL,
  "status" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "date_attempted" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."analysis_log" OWNER TO "augur";

-- ----------------------------
-- Table structure for chaoss_metric_status
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."chaoss_metric_status";
CREATE TABLE "augur_data"."chaoss_metric_status" (
  "cms_id" int8 NOT NULL DEFAULT nextval('"augur_data".chaoss_metric_status_cms_id_seq'::regclass),
  "cm_group" varchar COLLATE "pg_catalog"."default",
  "cm_source" varchar COLLATE "pg_catalog"."default",
  "cm_type" varchar COLLATE "pg_catalog"."default",
  "cm_backend_status" varchar COLLATE "pg_catalog"."default",
  "cm_frontend_status" varchar COLLATE "pg_catalog"."default",
  "cm_defined" bool,
  "cm_api_endpoint_repo" varchar COLLATE "pg_catalog"."default",
  "cm_api_endpoint_rg" varchar COLLATE "pg_catalog"."default",
  "cm_name" varchar COLLATE "pg_catalog"."default",
  "cm_working_group" varchar COLLATE "pg_catalog"."default",
  "cm_info" json,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cm_working_group_focus_area" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_data"."chaoss_metric_status" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."chaoss_metric_status" IS 'This table used to track CHAOSS Metric implementations in Augur, but due to the constantly changing location of that information, it is for the moment not actively populated. ';

-- ----------------------------
-- Table structure for chaoss_user
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."chaoss_user";
CREATE TABLE "augur_data"."chaoss_user" (
  "chaoss_id" int8 NOT NULL DEFAULT nextval('"augur_data".chaoss_user_chaoss_id_seq'::regclass),
  "chaoss_login_name" varchar COLLATE "pg_catalog"."default",
  "chaoss_login_hashword" varchar COLLATE "pg_catalog"."default",
  "chaoss_email" varchar COLLATE "pg_catalog"."default",
  "chaoss_text_phone" varchar COLLATE "pg_catalog"."default",
  "chaoss_first_name" varchar COLLATE "pg_catalog"."default",
  "chaoss_last_name" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT now()
)
;
ALTER TABLE "augur_data"."chaoss_user" OWNER TO "augur";

-- ----------------------------
-- Table structure for commit_comment_ref
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."commit_comment_ref";
CREATE TABLE "augur_data"."commit_comment_ref" (
  "cmt_comment_id" int8 NOT NULL DEFAULT nextval('"augur_data".commit_comment_ref_cmt_comment_id_seq'::regclass),
  "cmt_id" int8 NOT NULL,
  "repo_id" int8,
  "msg_id" int8 NOT NULL,
  "user_id" int8 NOT NULL,
  "body" text COLLATE "pg_catalog"."default",
  "line" int8,
  "position" int8,
  "commit_comment_src_node_id" varchar COLLATE "pg_catalog"."default",
  "cmt_comment_src_id" int8 NOT NULL,
  "created_at" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."commit_comment_ref" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."commit_comment_ref"."commit_comment_src_node_id" IS 'For data provenance, we store the source node ID if it exists. ';
COMMENT ON COLUMN "augur_data"."commit_comment_ref"."cmt_comment_src_id" IS 'For data provenance, we store the source ID if it exists. ';

-- ----------------------------
-- Table structure for commit_messages
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."commit_messages";
CREATE TABLE "augur_data"."commit_messages" (
  "cmt_msg_id" int8 NOT NULL DEFAULT nextval('"augur_data".commits_cmt_id_seq'::regclass),
  "repo_id" int8 NOT NULL,
  "cmt_msg" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_hash" varchar(80) COLLATE "pg_catalog"."default" NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
TABLESPACE "fast_postgres"
;
ALTER TABLE "augur_data"."commit_messages" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."commit_messages" IS 'This table holds commit messages';

-- ----------------------------
-- Table structure for commit_parents
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."commit_parents";
CREATE TABLE "augur_data"."commit_parents" (
  "cmt_id" int8 NOT NULL,
  "parent_id" int8 NOT NULL DEFAULT nextval('"augur_data".commit_parents_parent_id_seq'::regclass),
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."commit_parents" OWNER TO "augur";

-- ----------------------------
-- Table structure for commits
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."commits";
CREATE TABLE "augur_data"."commits" (
  "cmt_id" int8 NOT NULL DEFAULT nextval('"augur_data".commits_cmt_id_seq'::regclass),
  "repo_id" int8 NOT NULL,
  "cmt_commit_hash" varchar(80) COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_author_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_author_raw_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_author_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_author_date" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_author_affiliation" varchar COLLATE "pg_catalog"."default",
  "cmt_committer_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_committer_raw_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_committer_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_committer_date" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_committer_affiliation" varchar COLLATE "pg_catalog"."default",
  "cmt_added" int4 NOT NULL,
  "cmt_removed" int4 NOT NULL,
  "cmt_whitespace" int4 NOT NULL,
  "cmt_filename" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cmt_date_attempted" timestamp(0) NOT NULL,
  "cmt_ght_committer_id" int4,
  "cmt_ght_committed_at" timestamp(0),
  "cmt_committer_timestamp" timestamptz(0),
  "cmt_author_timestamp" timestamptz(0),
  "cmt_author_platform_username" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cmt_ght_author_id" uuid
)
TABLESPACE "fast_postgres"
;
ALTER TABLE "augur_data"."commits" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."commits" IS 'Commits.
Each row represents changes to one FILE within a single commit. So you will encounter multiple rows per commit hash in many cases. ';

-- ----------------------------
-- Table structure for contributor_affiliations
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."contributor_affiliations";
CREATE TABLE "augur_data"."contributor_affiliations" (
  "ca_id" int8 NOT NULL DEFAULT nextval('"augur_data".contributor_affiliations_ca_id_seq'::regclass),
  "ca_domain" varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
  "ca_start_date" date DEFAULT '1970-01-01'::date,
  "ca_last_used" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "ca_affiliation" varchar COLLATE "pg_catalog"."default",
  "ca_active" int2 DEFAULT 1,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."contributor_affiliations" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."contributor_affiliations" IS 'This table exists outside of relations with other tables. The purpose is to provide a dynamic, owner maintained (and augur augmented) list of affiliations. This table is processed in affiliation information in the DM_ tables generated when Augur is finished counting commits using the Facade Worker. ';

-- ----------------------------
-- Table structure for contributor_repo
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."contributor_repo";
CREATE TABLE "augur_data"."contributor_repo" (
  "cntrb_repo_id" int8 NOT NULL DEFAULT nextval('"augur_data".contributor_repo_cntrb_repo_id_seq'::regclass),
  "repo_git" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "repo_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "gh_repo_id" int8 NOT NULL,
  "cntrb_category" varchar COLLATE "pg_catalog"."default",
  "event_id" int8,
  "created_at" timestamp(0),
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid NOT NULL
)
;
ALTER TABLE "augur_data"."contributor_repo" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."contributor_repo"."repo_git" IS 'Similar to cntrb_id, we need this data for the table to have meaningful data. ';
COMMENT ON COLUMN "augur_data"."contributor_repo"."cntrb_id" IS 'This is not null because what is the point without the contributor in this table? ';
COMMENT ON TABLE "augur_data"."contributor_repo" IS 'Developed in Partnership with Andrew Brain. 
From: [
  {
    "login": "octocat",
    "id": 1,
    "node_id": "MDQ6VXNlcjE=",
    "avatar_url": "https://github.com/images/error/octocat_happy.gif",
    "gravatar_id": "",
    "url": "https://api.github.com/users/octocat",
    "html_url": "https://github.com/octocat",
    "followers_url": "https://api.github.com/users/octocat/followers",
    "following_url": "https://api.github.com/users/octocat/following{/other_user}",
    "gists_url": "https://api.github.com/users/octocat/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/octocat/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/octocat/subscriptions",
    "organizations_url": "https://api.github.com/users/octocat/orgs",
    "repos_url": "https://api.github.com/users/octocat/repos",
    "events_url": "https://api.github.com/users/octocat/events{/privacy}",
    "received_events_url": "https://api.github.com/users/octocat/received_events",
    "type": "User",
    "site_admin": false
  }
]
';

-- ----------------------------
-- Table structure for contributors
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."contributors";
CREATE TABLE "augur_data"."contributors" (
  "cntrb_login" varchar COLLATE "pg_catalog"."default",
  "cntrb_email" varchar COLLATE "pg_catalog"."default",
  "cntrb_full_name" varchar COLLATE "pg_catalog"."default",
  "cntrb_company" varchar COLLATE "pg_catalog"."default",
  "cntrb_created_at" timestamp(0),
  "cntrb_type" varchar COLLATE "pg_catalog"."default",
  "cntrb_fake" int2 DEFAULT 0,
  "cntrb_deleted" int2 DEFAULT 0,
  "cntrb_long" numeric(11,8) DEFAULT NULL::numeric,
  "cntrb_lat" numeric(10,8) DEFAULT NULL::numeric,
  "cntrb_country_code" char(3) COLLATE "pg_catalog"."default" DEFAULT NULL::bpchar,
  "cntrb_state" varchar COLLATE "pg_catalog"."default",
  "cntrb_city" varchar COLLATE "pg_catalog"."default",
  "cntrb_location" varchar COLLATE "pg_catalog"."default",
  "cntrb_canonical" varchar COLLATE "pg_catalog"."default",
  "cntrb_last_used" timestamptz(0) DEFAULT NULL::timestamp with time zone,
  "gh_user_id" int8,
  "gh_login" varchar COLLATE "pg_catalog"."default",
  "gh_url" varchar COLLATE "pg_catalog"."default",
  "gh_html_url" varchar COLLATE "pg_catalog"."default",
  "gh_node_id" varchar COLLATE "pg_catalog"."default",
  "gh_avatar_url" varchar COLLATE "pg_catalog"."default",
  "gh_gravatar_id" varchar COLLATE "pg_catalog"."default",
  "gh_followers_url" varchar COLLATE "pg_catalog"."default",
  "gh_following_url" varchar COLLATE "pg_catalog"."default",
  "gh_gists_url" varchar COLLATE "pg_catalog"."default",
  "gh_starred_url" varchar COLLATE "pg_catalog"."default",
  "gh_subscriptions_url" varchar COLLATE "pg_catalog"."default",
  "gh_organizations_url" varchar COLLATE "pg_catalog"."default",
  "gh_repos_url" varchar COLLATE "pg_catalog"."default",
  "gh_events_url" varchar COLLATE "pg_catalog"."default",
  "gh_received_events_url" varchar COLLATE "pg_catalog"."default",
  "gh_type" varchar COLLATE "pg_catalog"."default",
  "gh_site_admin" varchar COLLATE "pg_catalog"."default",
  "gl_web_url" varchar COLLATE "pg_catalog"."default",
  "gl_avatar_url" varchar COLLATE "pg_catalog"."default",
  "gl_state" varchar COLLATE "pg_catalog"."default",
  "gl_username" varchar COLLATE "pg_catalog"."default",
  "gl_full_name" varchar COLLATE "pg_catalog"."default",
  "gl_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid NOT NULL
)
TABLESPACE "speed"
;
ALTER TABLE "augur_data"."contributors" OWNER TO "augur";

-- ----------------------------
-- Table structure for contributors_aliases
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."contributors_aliases";
CREATE TABLE "augur_data"."contributors_aliases" (
  "cntrb_alias_id" int8 NOT NULL DEFAULT nextval('"augur_data".contributors_aliases_cntrb_alias_id_seq'::regclass),
  "canonical_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "alias_email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "cntrb_active" int2 NOT NULL DEFAULT 1,
  "cntrb_last_modified" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid NOT NULL
)
;
ALTER TABLE "augur_data"."contributors_aliases" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."contributors_aliases" IS 'Every open source user may have more than one email used to make contributions over time. Augur selects the first email it encounters for a user as its “canonical_email”. 

The canonical_email is also added to the contributors_aliases table, with the canonical_email and alias_email being identical.  Using this strategy, an email search will only need to join the alias table for basic email information, and can then more easily map the canonical email from each alias row to the same, more detailed information in the contributors table for a user. ';

-- ----------------------------
-- Table structure for contributors_old
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."contributors_old";
CREATE TABLE "augur_data"."contributors_old" (
  "cntrb_login" varchar COLLATE "pg_catalog"."default",
  "cntrb_email" varchar COLLATE "pg_catalog"."default",
  "cntrb_full_name" varchar COLLATE "pg_catalog"."default",
  "cntrb_company" varchar COLLATE "pg_catalog"."default",
  "cntrb_created_at" timestamp(0),
  "cntrb_type" varchar COLLATE "pg_catalog"."default",
  "cntrb_fake" int2 DEFAULT 0,
  "cntrb_deleted" int2 DEFAULT 0,
  "cntrb_long" numeric(11,8) DEFAULT NULL::numeric,
  "cntrb_lat" numeric(10,8) DEFAULT NULL::numeric,
  "cntrb_country_code" char(3) COLLATE "pg_catalog"."default" DEFAULT NULL::bpchar,
  "cntrb_state" varchar COLLATE "pg_catalog"."default",
  "cntrb_city" varchar COLLATE "pg_catalog"."default",
  "cntrb_location" varchar COLLATE "pg_catalog"."default",
  "cntrb_canonical" varchar COLLATE "pg_catalog"."default",
  "cntrb_last_used" timestamptz(0) DEFAULT NULL::timestamp with time zone,
  "gh_user_id" int8,
  "gh_login" varchar COLLATE "pg_catalog"."default",
  "gh_url" varchar COLLATE "pg_catalog"."default",
  "gh_html_url" varchar COLLATE "pg_catalog"."default",
  "gh_node_id" varchar COLLATE "pg_catalog"."default",
  "gh_avatar_url" varchar COLLATE "pg_catalog"."default",
  "gh_gravatar_id" varchar COLLATE "pg_catalog"."default",
  "gh_followers_url" varchar COLLATE "pg_catalog"."default",
  "gh_following_url" varchar COLLATE "pg_catalog"."default",
  "gh_gists_url" varchar COLLATE "pg_catalog"."default",
  "gh_starred_url" varchar COLLATE "pg_catalog"."default",
  "gh_subscriptions_url" varchar COLLATE "pg_catalog"."default",
  "gh_organizations_url" varchar COLLATE "pg_catalog"."default",
  "gh_repos_url" varchar COLLATE "pg_catalog"."default",
  "gh_events_url" varchar COLLATE "pg_catalog"."default",
  "gh_received_events_url" varchar COLLATE "pg_catalog"."default",
  "gh_type" varchar COLLATE "pg_catalog"."default",
  "gh_site_admin" varchar COLLATE "pg_catalog"."default",
  "gl_web_url" varchar COLLATE "pg_catalog"."default",
  "gl_avatar_url" varchar COLLATE "pg_catalog"."default",
  "gl_state" varchar COLLATE "pg_catalog"."default",
  "gl_username" varchar COLLATE "pg_catalog"."default",
  "gl_full_name" varchar COLLATE "pg_catalog"."default",
  "gl_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid NOT NULL
)
TABLESPACE "speed"
;
ALTER TABLE "augur_data"."contributors_old" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."contributors_old"."cntrb_login" IS 'Will be a double population with the same value as gh_login for github, but the local value for other systems. ';
COMMENT ON COLUMN "augur_data"."contributors_old"."cntrb_email" IS 'This needs to be here for matching contributor ids, which are augur, to the commit information. ';
COMMENT ON COLUMN "augur_data"."contributors_old"."cntrb_type" IS 'Present in another models. It is not currently used in Augur. ';
COMMENT ON COLUMN "augur_data"."contributors_old"."gh_login" IS 'populated with the github user name for github originated data. ';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_web_url" IS '“web_url” value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_avatar_url" IS '“avatar_url” value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_state" IS '“state” value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_username" IS '“username” value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_full_name" IS '“name” value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON COLUMN "augur_data"."contributors_old"."gl_id" IS '"id" value from these API calls to GitLab, all for the same user

https://gitlab.com/api/v4/users?username=computationalmystic
https://gitlab.com/api/v4/users?search=s@goggins.com
https://gitlab.com/api/v4/users?search=outdoors@acm.org

[
  {
    "id": 5481034,
    "name": "sean goggins",
    "username": "computationalmystic",
    "state": "active",
    "avatar_url": "https://secure.gravatar.com/avatar/fb1fb43953a6059df2fe8d94b21d575c?s=80&d=identicon",
    "web_url": "https://gitlab.com/computationalmystic"
  }
]';
COMMENT ON TABLE "augur_data"."contributors_old" IS 'For GitHub, this should be repeated from gh_login. for other systems, it should be that systems login. 
Github now allows a user to change their login name, but their user id remains the same in this case. So, the natural key is the combination of id and login, but there should never be repeated logins. ';

-- ----------------------------
-- Table structure for discourse_insights
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."discourse_insights";
CREATE TABLE "augur_data"."discourse_insights" (
  "msg_discourse_id" int8 NOT NULL DEFAULT nextval('"augur_data".discourse_insights_msg_discourse_id_seq1'::regclass),
  "msg_id" int8,
  "discourse_act" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."discourse_insights" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."discourse_insights" IS 'This table is populated by the “Discourse_Analysis_Worker”. It examines sequential discourse, using computational linguistic methods, to draw statistical inferences regarding the discourse in a particular comment thread. ';

-- ----------------------------
-- Table structure for dm_repo_annual
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_annual";
CREATE TABLE "augur_data"."dm_repo_annual" (
  "repo_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_annual" OWNER TO "augur";

-- ----------------------------
-- Table structure for dm_repo_group_annual
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_group_annual";
CREATE TABLE "augur_data"."dm_repo_group_annual" (
  "repo_group_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_group_annual" OWNER TO "augur";

-- ----------------------------
-- Table structure for dm_repo_group_monthly
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_group_monthly";
CREATE TABLE "augur_data"."dm_repo_group_monthly" (
  "repo_group_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "month" int2 NOT NULL,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_group_monthly" OWNER TO "augur";

-- ----------------------------
-- Table structure for dm_repo_group_weekly
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_group_weekly";
CREATE TABLE "augur_data"."dm_repo_group_weekly" (
  "repo_group_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "week" int2 NOT NULL,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_group_weekly" OWNER TO "augur";

-- ----------------------------
-- Table structure for dm_repo_monthly
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_monthly";
CREATE TABLE "augur_data"."dm_repo_monthly" (
  "repo_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "month" int2 NOT NULL,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_monthly" OWNER TO "augur";

-- ----------------------------
-- Table structure for dm_repo_weekly
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."dm_repo_weekly";
CREATE TABLE "augur_data"."dm_repo_weekly" (
  "repo_id" int8 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "affiliation" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "week" int2 NOT NULL,
  "year" int2 NOT NULL,
  "added" int8 NOT NULL,
  "removed" int8 NOT NULL,
  "whitespace" int8 NOT NULL,
  "files" int8 NOT NULL,
  "patches" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."dm_repo_weekly" OWNER TO "augur";

-- ----------------------------
-- Table structure for exclude
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."exclude";
CREATE TABLE "augur_data"."exclude" (
  "id" int4 NOT NULL,
  "projects_id" int4 NOT NULL,
  "email" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "domain" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying
)
;
ALTER TABLE "augur_data"."exclude" OWNER TO "augur";

-- ----------------------------
-- Table structure for historical_repo_urls
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."historical_repo_urls";
CREATE TABLE "augur_data"."historical_repo_urls" (
  "repo_id" int8 NOT NULL,
  "git_url" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "date_collected" timestamptz(6) DEFAULT now()
)
;
ALTER TABLE "augur_data"."historical_repo_urls" OWNER TO "augur";

-- ----------------------------
-- Table structure for issue_assignees
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."issue_assignees";
CREATE TABLE "augur_data"."issue_assignees" (
  "issue_assignee_id" int8 NOT NULL DEFAULT nextval('"augur_data".issue_assignees_issue_assignee_id_seq'::regclass),
  "issue_id" int8,
  "repo_id" int8,
  "issue_assignee_src_id" int8,
  "issue_assignee_src_node" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."issue_assignees" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."issue_assignees"."issue_assignee_src_id" IS 'This ID comes from the source. In the case of GitHub, it is the id that is the first field returned from the issue events API in the issue_assignees embedded JSON object. We may discover it is an ID for the person themselves; but my hypothesis is that its not.';
COMMENT ON COLUMN "augur_data"."issue_assignees"."issue_assignee_src_node" IS 'This character based identifier comes from the source. In the case of GitHub, it is the id that is the second field returned from the issue events API in the issue_assignees embedded JSON object. We may discover it is an ID for the person themselves; but my hypothesis is that its not.';

-- ----------------------------
-- Table structure for issue_events
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."issue_events";
CREATE TABLE "augur_data"."issue_events" (
  "event_id" int8 NOT NULL DEFAULT nextval('"augur_data".issue_events_event_id_seq'::regclass),
  "issue_id" int8 NOT NULL,
  "repo_id" int8,
  "action" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "action_commit_hash" varchar COLLATE "pg_catalog"."default",
  "created_at" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "node_id" varchar COLLATE "pg_catalog"."default",
  "node_url" varchar COLLATE "pg_catalog"."default",
  "platform_id" int8 NOT NULL,
  "issue_event_src_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."issue_events" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."issue_events"."node_id" IS 'This should be renamed to issue_event_src_node_id, as its the varchar identifier in GitHub and likely common in other sources as well. However, since it was created before we came to this naming standard and workers are built around it, we have it simply named as node_id. Anywhere you see node_id in the schema, it comes from GitHubs terminology.';
COMMENT ON COLUMN "augur_data"."issue_events"."issue_event_src_id" IS 'This ID comes from the source. In the case of GitHub, it is the id that is the first field returned from the issue events API';

-- ----------------------------
-- Table structure for issue_labels
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."issue_labels";
CREATE TABLE "augur_data"."issue_labels" (
  "issue_label_id" int8 NOT NULL DEFAULT nextval('"augur_data".issue_labels_issue_label_id_seq'::regclass),
  "issue_id" int8,
  "repo_id" int8,
  "label_text" varchar COLLATE "pg_catalog"."default",
  "label_description" varchar COLLATE "pg_catalog"."default",
  "label_color" varchar COLLATE "pg_catalog"."default",
  "label_src_id" int8,
  "label_src_node_id" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."issue_labels" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."issue_labels"."label_src_id" IS 'This character based identifier (node) comes from the source. In the case of GitHub, it is the id that is the second field returned from the issue events API JSON subsection for issues.';

-- ----------------------------
-- Table structure for issue_message_ref
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."issue_message_ref";
CREATE TABLE "augur_data"."issue_message_ref" (
  "issue_msg_ref_id" int8 NOT NULL DEFAULT nextval('"augur_data".issue_message_ref_issue_msg_ref_id_seq'::regclass),
  "issue_id" int8,
  "repo_id" int8,
  "msg_id" int8,
  "issue_msg_ref_src_node_id" varchar COLLATE "pg_catalog"."default",
  "issue_msg_ref_src_comment_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."issue_message_ref" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."issue_message_ref"."issue_msg_ref_src_node_id" IS 'This character based identifier comes from the source. In the case of GitHub, it is the id that is the first field returned from the issue comments API';
COMMENT ON COLUMN "augur_data"."issue_message_ref"."issue_msg_ref_src_comment_id" IS 'This ID comes from the source. In the case of GitHub, it is the id that is the first field returned from the issue comments API';

-- ----------------------------
-- Table structure for issues
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."issues";
CREATE TABLE "augur_data"."issues" (
  "issue_id" int8 NOT NULL DEFAULT nextval('"augur_data".issue_seq'::regclass),
  "repo_id" int8,
  "pull_request" int8,
  "pull_request_id" int8,
  "created_at" timestamp(0),
  "issue_title" varchar COLLATE "pg_catalog"."default",
  "issue_body" varchar COLLATE "pg_catalog"."default",
  "comment_count" int8,
  "updated_at" timestamp(0),
  "closed_at" timestamp(0),
  "due_on" timestamp(0),
  "repository_url" varchar COLLATE "pg_catalog"."default",
  "issue_url" varchar COLLATE "pg_catalog"."default",
  "labels_url" varchar COLLATE "pg_catalog"."default",
  "comments_url" varchar COLLATE "pg_catalog"."default",
  "events_url" varchar COLLATE "pg_catalog"."default",
  "html_url" varchar COLLATE "pg_catalog"."default",
  "issue_state" varchar COLLATE "pg_catalog"."default",
  "issue_node_id" varchar COLLATE "pg_catalog"."default",
  "gh_issue_number" int8,
  "gh_issue_id" int8,
  "gh_user_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "reporter_id" uuid,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."issues" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."issues"."reporter_id" IS 'The ID of the person who opened the issue. ';
COMMENT ON COLUMN "augur_data"."issues"."cntrb_id" IS 'The ID of the person who closed the issue. ';

-- ----------------------------
-- Table structure for libraries
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."libraries";
CREATE TABLE "augur_data"."libraries" (
  "library_id" int8 NOT NULL DEFAULT nextval('"augur_data".libraries_library_id_seq'::regclass),
  "repo_id" int8,
  "platform" varchar COLLATE "pg_catalog"."default",
  "name" varchar COLLATE "pg_catalog"."default",
  "created_timestamp" timestamp(0) DEFAULT NULL::timestamp without time zone,
  "updated_timestamp" timestamp(0) DEFAULT NULL::timestamp without time zone,
  "library_description" varchar(2000) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "keywords" varchar COLLATE "pg_catalog"."default",
  "library_homepage" varchar(1000) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "license" varchar COLLATE "pg_catalog"."default",
  "version_count" int4,
  "latest_release_timestamp" varchar COLLATE "pg_catalog"."default",
  "latest_release_number" varchar COLLATE "pg_catalog"."default",
  "package_manager_id" varchar COLLATE "pg_catalog"."default",
  "dependency_count" int4,
  "dependent_library_count" int4,
  "primary_language" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."libraries" OWNER TO "augur";

-- ----------------------------
-- Table structure for library_dependencies
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."library_dependencies";
CREATE TABLE "augur_data"."library_dependencies" (
  "lib_dependency_id" int8 NOT NULL DEFAULT nextval('"augur_data".library_dependencies_lib_dependency_id_seq'::regclass),
  "library_id" int8,
  "manifest_platform" varchar COLLATE "pg_catalog"."default",
  "manifest_filepath" varchar(1000) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "manifest_kind" varchar COLLATE "pg_catalog"."default",
  "repo_id_branch" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."library_dependencies" OWNER TO "augur";

-- ----------------------------
-- Table structure for library_version
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."library_version";
CREATE TABLE "augur_data"."library_version" (
  "library_version_id" int8 NOT NULL DEFAULT nextval('"augur_data".library_version_library_version_id_seq'::regclass),
  "library_id" int8,
  "library_platform" varchar COLLATE "pg_catalog"."default",
  "version_number" varchar COLLATE "pg_catalog"."default",
  "version_release_date" timestamp(0) DEFAULT NULL::timestamp without time zone,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."library_version" OWNER TO "augur";

-- ----------------------------
-- Table structure for lstm_anomaly_models
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."lstm_anomaly_models";
CREATE TABLE "augur_data"."lstm_anomaly_models" (
  "model_id" int8 NOT NULL DEFAULT nextval('"augur_data".lstm_anomaly_models_model_id_seq'::regclass),
  "model_name" varchar COLLATE "pg_catalog"."default",
  "model_description" varchar COLLATE "pg_catalog"."default",
  "look_back_days" int8,
  "training_days" int8,
  "batch_size" int8,
  "metric" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."lstm_anomaly_models" OWNER TO "augur";

-- ----------------------------
-- Table structure for lstm_anomaly_results
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."lstm_anomaly_results";
CREATE TABLE "augur_data"."lstm_anomaly_results" (
  "result_id" int8 NOT NULL DEFAULT nextval('"augur_data".lstm_anomaly_results_result_id_seq'::regclass),
  "repo_id" int8,
  "repo_category" varchar COLLATE "pg_catalog"."default",
  "model_id" int8,
  "metric" varchar COLLATE "pg_catalog"."default",
  "contamination_factor" float8,
  "mean_absolute_error" float8,
  "remarks" varchar COLLATE "pg_catalog"."default",
  "metric_field" varchar COLLATE "pg_catalog"."default",
  "mean_absolute_actual_value" float8,
  "mean_absolute_prediction_value" float8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."lstm_anomaly_results" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."lstm_anomaly_results"."metric_field" IS 'This is a listing of all of the endpoint fields included in the generation of the metric. Sometimes there is one, sometimes there is more than one. This will list them all. ';

-- ----------------------------
-- Table structure for message
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."message";
CREATE TABLE "augur_data"."message" (
  "msg_id" int8 NOT NULL DEFAULT nextval('"augur_data".message_msg_id_seq'::regclass),
  "rgls_id" int8,
  "platform_msg_id" int8,
  "platform_node_id" varchar COLLATE "pg_catalog"."default",
  "repo_id" int8,
  "msg_text" varchar COLLATE "pg_catalog"."default",
  "msg_timestamp" timestamp(0),
  "msg_sender_email" varchar COLLATE "pg_catalog"."default",
  "msg_header" varchar COLLATE "pg_catalog"."default",
  "pltfrm_id" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
TABLESPACE "speed"
;
ALTER TABLE "augur_data"."message" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."message"."cntrb_id" IS 'Not populated for mailing lists. Populated for GitHub issues. ';

-- ----------------------------
-- Table structure for message_analysis
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."message_analysis";
CREATE TABLE "augur_data"."message_analysis" (
  "msg_analysis_id" int8 NOT NULL DEFAULT nextval('"augur_data".message_analysis_msg_analysis_id_seq'::regclass),
  "msg_id" int8,
  "worker_run_id" int8,
  "sentiment_score" float8,
  "reconstruction_error" float8,
  "novelty_flag" bool,
  "feedback_flag" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."message_analysis" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."message_analysis"."worker_run_id" IS 'This column is used to indicate analyses run by a worker during the same execution period, and is useful for grouping, and time series analysis.  ';
COMMENT ON COLUMN "augur_data"."message_analysis"."sentiment_score" IS 'A sentiment analysis score. Zero is neutral, negative numbers are negative sentiment, and positive numbers are positive sentiment. ';
COMMENT ON COLUMN "augur_data"."message_analysis"."reconstruction_error" IS 'Each message is converted to a 250 dimension doc2vec vector, so the reconstruction error is the difference between what the predicted vector and the actual vector.';
COMMENT ON COLUMN "augur_data"."message_analysis"."novelty_flag" IS 'This is an analysis of the degree to which the message is novel when compared to other messages in a repository.  For example when bots are producing numerous identical messages, the novelty score is low. It would also be a low novelty score when several people are making the same comment. ';
COMMENT ON COLUMN "augur_data"."message_analysis"."feedback_flag" IS 'This exists to provide the user with an opportunity provide feedback on the resulting the sentiment scores. ';

-- ----------------------------
-- Table structure for message_analysis_summary
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."message_analysis_summary";
CREATE TABLE "augur_data"."message_analysis_summary" (
  "msg_summary_id" int8 NOT NULL DEFAULT nextval('"augur_data".message_analysis_summary_msg_summary_id_seq'::regclass),
  "repo_id" int8,
  "worker_run_id" int8,
  "positive_ratio" float8,
  "negative_ratio" float8,
  "novel_count" int8,
  "period" timestamp(0),
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."message_analysis_summary" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."message_analysis_summary"."worker_run_id" IS 'This value should reflect the worker_run_id for the messages summarized in the table. There is not a relation between these two tables for that purpose because its not *really*, relationally a concept unless we create a third table for "worker_run_id", which we determined was unnecessarily complex. ';
COMMENT ON COLUMN "augur_data"."message_analysis_summary"."novel_count" IS 'The number of messages identified as novel during the analyzed period';
COMMENT ON COLUMN "augur_data"."message_analysis_summary"."period" IS 'The whole timeline is divided into periods based on the definition of time period for analysis, which is user specified. Timestamp of the first period to look at, until the end of messages at the data of execution. ';
COMMENT ON TABLE "augur_data"."message_analysis_summary" IS 'In a relationally perfect world, we would have a table called “message_analysis_run” the incremented the “worker_run_id” for both message_analysis and message_analysis_summary. For now, we decided this was overkill. ';

-- ----------------------------
-- Table structure for message_sentiment
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."message_sentiment";
CREATE TABLE "augur_data"."message_sentiment" (
  "msg_analysis_id" int8 NOT NULL DEFAULT nextval('"augur_data".message_sentiment_msg_analysis_id_seq'::regclass),
  "msg_id" int8,
  "worker_run_id" int8,
  "sentiment_score" float8,
  "reconstruction_error" float8,
  "novelty_flag" bool,
  "feedback_flag" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."message_sentiment" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."message_sentiment"."worker_run_id" IS 'This column is used to indicate analyses run by a worker during the same execution period, and is useful for grouping, and time series analysis.  ';
COMMENT ON COLUMN "augur_data"."message_sentiment"."sentiment_score" IS 'A sentiment analysis score. Zero is neutral, negative numbers are negative sentiment, and positive numbers are positive sentiment. ';
COMMENT ON COLUMN "augur_data"."message_sentiment"."reconstruction_error" IS 'Each message is converted to a 250 dimension doc2vec vector, so the reconstruction error is the difference between what the predicted vector and the actual vector.';
COMMENT ON COLUMN "augur_data"."message_sentiment"."novelty_flag" IS 'This is an analysis of the degree to which the message is novel when compared to other messages in a repository.  For example when bots are producing numerous identical messages, the novelty score is low. It would also be a low novelty score when several people are making the same comment. ';
COMMENT ON COLUMN "augur_data"."message_sentiment"."feedback_flag" IS 'This exists to provide the user with an opportunity provide feedback on the resulting the sentiment scores. ';

-- ----------------------------
-- Table structure for message_sentiment_summary
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."message_sentiment_summary";
CREATE TABLE "augur_data"."message_sentiment_summary" (
  "msg_summary_id" int8 NOT NULL DEFAULT nextval('"augur_data".message_sentiment_summary_msg_summary_id_seq'::regclass),
  "repo_id" int8,
  "worker_run_id" int8,
  "positive_ratio" float8,
  "negative_ratio" float8,
  "novel_count" int8,
  "period" timestamp(0),
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."message_sentiment_summary" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."message_sentiment_summary"."worker_run_id" IS 'This value should reflect the worker_run_id for the messages summarized in the table. There is not a relation between these two tables for that purpose because its not *really*, relationally a concept unless we create a third table for "worker_run_id", which we determined was unnecessarily complex. ';
COMMENT ON COLUMN "augur_data"."message_sentiment_summary"."novel_count" IS 'The number of messages identified as novel during the analyzed period';
COMMENT ON COLUMN "augur_data"."message_sentiment_summary"."period" IS 'The whole timeline is divided into periods based on the definition of time period for analysis, which is user specified. Timestamp of the first period to look at, until the end of messages at the data of execution. ';
COMMENT ON TABLE "augur_data"."message_sentiment_summary" IS 'In a relationally perfect world, we would have a table called “message_sentiment_run” the incremented the “worker_run_id” for both message_sentiment and message_sentiment_summary. For now, we decided this was overkill. ';

-- ----------------------------
-- Table structure for network_beyond_augur
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."network_beyond_augur";
CREATE TABLE "augur_data"."network_beyond_augur" (
  "cntrb_id" uuid,
  "repo_git" varchar COLLATE "pg_catalog"."default",
  "repo_name" varchar COLLATE "pg_catalog"."default",
  "action" varchar COLLATE "pg_catalog"."default",
  "action_year" float8,
  "action_quarter" numeric,
  "counter" int8
)
;
ALTER TABLE "augur_data"."network_beyond_augur" OWNER TO "augur";

-- ----------------------------
-- Table structure for network_beyond_augur_dependencies
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."network_beyond_augur_dependencies";
CREATE TABLE "augur_data"."network_beyond_augur_dependencies" (
  "cntrb_id" uuid,
  "repo_git" varchar COLLATE "pg_catalog"."default",
  "repo_name" varchar COLLATE "pg_catalog"."default",
  "action" varchar COLLATE "pg_catalog"."default",
  "action_year" float8,
  "action_quarter" numeric,
  "counter" int8
)
;
ALTER TABLE "augur_data"."network_beyond_augur_dependencies" OWNER TO "augur";

-- ----------------------------
-- Table structure for platform
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."platform";
CREATE TABLE "augur_data"."platform" (
  "pltfrm_id" int8 NOT NULL DEFAULT nextval('"augur_data".platform_pltfrm_id_seq'::regclass),
  "pltfrm_name" varchar COLLATE "pg_catalog"."default",
  "pltfrm_version" varchar COLLATE "pg_catalog"."default",
  "pltfrm_release_date" date,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."platform" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_analysis
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_analysis";
CREATE TABLE "augur_data"."pull_request_analysis" (
  "pull_request_analysis_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_analysis_pull_request_analysis_id_seq'::regclass),
  "pull_request_id" int8,
  "merge_probability" numeric(256,250),
  "mechanism" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamptz(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_analysis" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_analysis"."pull_request_id" IS 'It would be better if the pull request worker is run first to fetch the latest PRs before analyzing';
COMMENT ON COLUMN "augur_data"."pull_request_analysis"."merge_probability" IS 'Indicates the probability of the PR being merged';
COMMENT ON COLUMN "augur_data"."pull_request_analysis"."mechanism" IS 'the ML model used for prediction (It is XGBoost Classifier at present)';

-- ----------------------------
-- Table structure for pull_request_assignees
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_assignees";
CREATE TABLE "augur_data"."pull_request_assignees" (
  "pr_assignee_map_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_assignees_pr_assignee_map_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "pr_assignee_src_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "contrib_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_assignees" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_commits
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_commits";
CREATE TABLE "augur_data"."pull_request_commits" (
  "pr_cmt_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_commits_pr_cmt_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "pr_cmt_sha" varchar COLLATE "pg_catalog"."default",
  "pr_cmt_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_cmt_message" varchar COLLATE "pg_catalog"."default",
  "pr_cmt_comments_url" varchar COLLATE "pg_catalog"."default",
  "pr_cmt_timestamp" timestamp(0),
  "pr_cmt_author_email" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "pr_cmt_author_cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_commits" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_commits"."pr_cmt_sha" IS 'This is the commit SHA for a pull request commit. If the PR is not to the master branch of the main repository (or, in rare cases, from it), then you will NOT find a corresponding commit SHA in the commit table. (see table comment for further explanation). ';
COMMENT ON TABLE "augur_data"."pull_request_commits" IS 'Pull request commits are an enumeration of each commit associated with a pull request. 
Not all pull requests are from a branch or fork into master. 
The commits table intends to count only commits that end up in the master branch (i.e., part of the deployed code base for a project).
Therefore, there will be commit “SHA”’s in this table that are no associated with a commit SHA in the commits table. 
In cases where the PR is to the master branch of a project, you will find a match. In cases where the PR does not involve the master branch, you will not find a corresponding commit SHA in the commits table. This is expected. ';

-- ----------------------------
-- Table structure for pull_request_events
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_events";
CREATE TABLE "augur_data"."pull_request_events" (
  "pr_event_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_events_pr_event_id_seq'::regclass),
  "pull_request_id" int8 NOT NULL,
  "repo_id" int8,
  "action" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "action_commit_hash" varchar COLLATE "pg_catalog"."default",
  "created_at" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "issue_event_src_id" int8,
  "node_id" varchar COLLATE "pg_catalog"."default",
  "node_url" varchar COLLATE "pg_catalog"."default",
  "platform_id" int8 NOT NULL DEFAULT 25150,
  "pr_platform_event_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_events" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_events"."issue_event_src_id" IS 'This ID comes from the source. In the case of GitHub, it is the id that is the first field returned from the issue events API';
COMMENT ON COLUMN "augur_data"."pull_request_events"."node_id" IS 'This should be renamed to issue_event_src_node_id, as its the varchar identifier in GitHub and likely common in other sources as well. However, since it was created before we came to this naming standard and workers are built around it, we have it simply named as node_id. Anywhere you see node_id in the schema, it comes from GitHubs terminology.';

-- ----------------------------
-- Table structure for pull_request_files
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_files";
CREATE TABLE "augur_data"."pull_request_files" (
  "pr_file_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_files_pr_file_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "pr_file_additions" int8,
  "pr_file_deletions" int8,
  "pr_file_path" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_files" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."pull_request_files" IS 'Pull request commits are an enumeration of each commit associated with a pull request. 
Not all pull requests are from a branch or fork into master. 
The commits table intends to count only commits that end up in the master branch (i.e., part of the deployed code base for a project).
Therefore, there will be commit “SHA”’s in this table that are no associated with a commit SHA in the commits table. 
In cases where the PR is to the master branch of a project, you will find a match. In cases where the PR does not involve the master branch, you will not find a corresponding commit SHA in the commits table. This is expected. ';

-- ----------------------------
-- Table structure for pull_request_labels
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_labels";
CREATE TABLE "augur_data"."pull_request_labels" (
  "pr_label_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_labels_pr_label_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "pr_src_id" int8,
  "pr_src_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_src_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_description" varchar COLLATE "pg_catalog"."default",
  "pr_src_color" varchar COLLATE "pg_catalog"."default",
  "pr_src_default_bool" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_labels" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_message_ref
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_message_ref";
CREATE TABLE "augur_data"."pull_request_message_ref" (
  "pr_msg_ref_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_message_ref_pr_msg_ref_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "msg_id" int8,
  "pr_message_ref_src_comment_id" int8,
  "pr_message_ref_src_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_issue_url" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_message_ref" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_meta
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_meta";
CREATE TABLE "augur_data"."pull_request_meta" (
  "pr_repo_meta_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_meta_pr_repo_meta_id_seq'::regclass),
  "pull_request_id" int8,
  "repo_id" int8,
  "pr_head_or_base" varchar COLLATE "pg_catalog"."default",
  "pr_src_meta_label" varchar COLLATE "pg_catalog"."default",
  "pr_src_meta_ref" varchar COLLATE "pg_catalog"."default",
  "pr_sha" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_meta" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_meta"."pr_head_or_base" IS 'Each pull request should have one and only one head record; and one and only one base record. ';
COMMENT ON COLUMN "augur_data"."pull_request_meta"."pr_src_meta_label" IS 'This is a representation of the repo:branch information in the pull request. Head is issuing the pull request and base is taking the pull request. For example:  (We do not store all of this)

 "head": {
      "label": "chaoss:pull-request-worker",
      "ref": "pull-request-worker",
      "sha": "6b380c3d6d625616f79d702612ebab6d204614f2",
      "user": {
        "login": "chaoss",
        "id": 29740296,
        "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",
        "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",
        "gravatar_id": "",
        "url": "https://api.github.com/users/chaoss",
        "html_url": "https://github.com/chaoss",
        "followers_url": "https://api.github.com/users/chaoss/followers",
        "following_url": "https://api.github.com/users/chaoss/following{/other_user}",
        "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",
        "organizations_url": "https://api.github.com/users/chaoss/orgs",
        "repos_url": "https://api.github.com/users/chaoss/repos",
        "events_url": "https://api.github.com/users/chaoss/events{/privacy}",
        "received_events_url": "https://api.github.com/users/chaoss/received_events",
        "type": "Organization",
        "site_admin": false
      },
      "repo": {
        "id": 78134122,
        "node_id": "MDEwOlJlcG9zaXRvcnk3ODEzNDEyMg==",
        "name": "augur",
        "full_name": "chaoss/augur",
        "private": false,
        "owner": {
          "login": "chaoss",
          "id": 29740296,
          "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",
          "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",
          "gravatar_id": "",
          "url": "https://api.github.com/users/chaoss",
          "html_url": "https://github.com/chaoss",
          "followers_url": "https://api.github.com/users/chaoss/followers",
          "following_url": "https://api.github.com/users/chaoss/following{/other_user}",
          "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",
          "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",
          "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",
          "organizations_url": "https://api.github.com/users/chaoss/orgs",
          "repos_url": "https://api.github.com/users/chaoss/repos",
          "events_url": "https://api.github.com/users/chaoss/events{/privacy}",
          "received_events_url": "https://api.github.com/users/chaoss/received_events",
          "type": "Organization",
          "site_admin": false
        },
        "html_url": "https://github.com/chaoss/augur",
        "description": "Python library and web service for Open Source Software Health and Sustainability metrics & data collection.",
        "fork": false,
        "url": "https://api.github.com/repos/chaoss/augur",
        "forks_url": "https://api.github.com/repos/chaoss/augur/forks",
        "keys_url": "https://api.github.com/repos/chaoss/augur/keys{/key_id}",
        "collaborators_url": "https://api.github.com/repos/chaoss/augur/collaborators{/collaborator}",
        "teams_url": "https://api.github.com/repos/chaoss/augur/teams",
        "hooks_url": "https://api.github.com/repos/chaoss/augur/hooks",
        "issue_events_url": "https://api.github.com/repos/chaoss/augur/issues/events{/number}",
        "events_url": "https://api.github.com/repos/chaoss/augur/events",
        "assignees_url": "https://api.github.com/repos/chaoss/augur/assignees{/user}",
        "branches_url": "https://api.github.com/repos/chaoss/augur/branches{/branch}",
        "tags_url": "https://api.github.com/repos/chaoss/augur/tags",
        "blobs_url": "https://api.github.com/repos/chaoss/augur/git/blobs{/sha}",
        "git_tags_url": "https://api.github.com/repos/chaoss/augur/git/tags{/sha}",
        "git_refs_url": "https://api.github.com/repos/chaoss/augur/git/refs{/sha}",
        "trees_url": "https://api.github.com/repos/chaoss/augur/git/trees{/sha}",
        "statuses_url": "https://api.github.com/repos/chaoss/augur/statuses/{sha}",
        "languages_url": "https://api.github.com/repos/chaoss/augur/languages",
        "stargazers_url": "https://api.github.com/repos/chaoss/augur/stargazers",
        "contributors_url": "https://api.github.com/repos/chaoss/augur/contributors",
        "subscribers_url": "https://api.github.com/repos/chaoss/augur/subscribers",
        "subscription_url": "https://api.github.com/repos/chaoss/augur/subscription",
        "commits_url": "https://api.github.com/repos/chaoss/augur/commits{/sha}",
        "git_commits_url": "https://api.github.com/repos/chaoss/augur/git/commits{/sha}",
        "comments_url": "https://api.github.com/repos/chaoss/augur/comments{/number}",
        "issue_comment_url": "https://api.github.com/repos/chaoss/augur/issues/comments{/number}",
        "contents_url": "https://api.github.com/repos/chaoss/augur/contents/{+path}",
        "compare_url": "https://api.github.com/repos/chaoss/augur/compare/{base}...{head}",
        "merges_url": "https://api.github.com/repos/chaoss/augur/merges",
        "archive_url": "https://api.github.com/repos/chaoss/augur/{archive_format}{/ref}",
        "downloads_url": "https://api.github.com/repos/chaoss/augur/downloads",
        "issues_url": "https://api.github.com/repos/chaoss/augur/issues{/number}",
        "pulls_url": "https://api.github.com/repos/chaoss/augur/pulls{/number}",
        "milestones_url": "https://api.github.com/repos/chaoss/augur/milestones{/number}",
        "notifications_url": "https://api.github.com/repos/chaoss/augur/notifications{?since,all,participating}",
        "labels_url": "https://api.github.com/repos/chaoss/augur/labels{/name}",
        "releases_url": "https://api.github.com/repos/chaoss/augur/releases{/id}",
        "deployments_url": "https://api.github.com/repos/chaoss/augur/deployments",
        "created_at": "2017-01-05T17:34:54Z",
        "updated_at": "2019-11-15T00:56:12Z",
        "pushed_at": "2019-12-02T06:27:26Z",
        "git_url": "git://github.com/chaoss/augur.git",
        "ssh_url": "git@github.com:chaoss/augur.git",
        "clone_url": "https://github.com/chaoss/augur.git",
        "svn_url": "https://github.com/chaoss/augur",
        "homepage": "http://augur.osshealth.io/",
        "size": 82004,
        "stargazers_count": 153,
        "watchers_count": 153,
        "language": "Python",
        "has_issues": true,
        "has_projects": false,
        "has_downloads": true,
        "has_wiki": false,
        "has_pages": true,
        "forks_count": 205,
        "mirror_url": null,
        "archived": false,
        "disabled": false,
        "open_issues_count": 14,
        "license": {
          "key": "mit",
          "name": "MIT License",
          "spdx_id": "MIT",
          "url": "https://api.github.com/licenses/mit",
          "node_id": "MDc6TGljZW5zZTEz"
        },
        "forks": 205,
        "open_issues": 14,
        "watchers": 153,
        "default_branch": "master"
      }
    },
    "base": {
      "label": "chaoss:dev",
      "ref": "dev",
      "sha": "bfd2d34b51659613dd842cf83c3873f7699c2a0e",
      "user": {
        "login": "chaoss",
        "id": 29740296,
        "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",
        "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",
        "gravatar_id": "",
        "url": "https://api.github.com/users/chaoss",
        "html_url": "https://github.com/chaoss",
        "followers_url": "https://api.github.com/users/chaoss/followers",
        "following_url": "https://api.github.com/users/chaoss/following{/other_user}",
        "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",
        "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",
        "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",
        "organizations_url": "https://api.github.com/users/chaoss/orgs",
        "repos_url": "https://api.github.com/users/chaoss/repos",
        "events_url": "https://api.github.com/users/chaoss/events{/privacy}",
        "received_events_url": "https://api.github.com/users/chaoss/received_events",
        "type": "Organization",
        "site_admin": false
      },
      "repo": {
        "id": 78134122,
        "node_id": "MDEwOlJlcG9zaXRvcnk3ODEzNDEyMg==",
        "name": "augur",
        "full_name": "chaoss/augur",
        "private": false,
        "owner": {
          "login": "chaoss",
          "id": 29740296,
          "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",
          "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",
          "gravatar_id": "",
          "url": "https://api.github.com/users/chaoss",
          "html_url": "https://github.com/chaoss",
          "followers_url": "https://api.github.com/users/chaoss/followers",
          "following_url": "https://api.github.com/users/chaoss/following{/other_user}",
          "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",
          "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",
          "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",
          "organizations_url": "https://api.github.com/users/chaoss/orgs",
          "repos_url": "https://api.github.com/users/chaoss/repos",
          "events_url": "https://api.github.com/users/chaoss/events{/privacy}",
          "received_events_url": "https://api.github.com/users/chaoss/received_events",
          "type": "Organization",
          "site_admin": false
        },
';
COMMENT ON TABLE "augur_data"."pull_request_meta" IS 'Pull requests contain referencing metadata.  There are a few columns that are discrete. There are also head and base designations for the repo on each side of the pull request. Similar functions exist in GitLab, though the language here is based on GitHub. The JSON Being adapted to as of the development of this schema is here:      "base": {       "label": "chaoss:dev",       "ref": "dev",       "sha": "dc6c6f3947f7dc84ecba3d8bda641ef786e7027d",       "user": {         "login": "chaoss",         "id": 29740296,         "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",         "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",         "gravatar_id": "",         "url": "https://api.github.com/users/chaoss",         "html_url": "https://github.com/chaoss",         "followers_url": "https://api.github.com/users/chaoss/followers",         "following_url": "https://api.github.com/users/chaoss/following{/other_user}",         "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",         "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",         "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",         "organizations_url": "https://api.github.com/users/chaoss/orgs",         "repos_url": "https://api.github.com/users/chaoss/repos",         "events_url": "https://api.github.com/users/chaoss/events{/privacy}",         "received_events_url": "https://api.github.com/users/chaoss/received_events",         "type": "Organization",         "site_admin": false       },       "repo": {         "id": 78134122,         "node_id": "MDEwOlJlcG9zaXRvcnk3ODEzNDEyMg==",         "name": "augur",         "full_name": "chaoss/augur",         "private": false,         "owner": {           "login": "chaoss",           "id": 29740296,           "node_id": "MDEyOk9yZ2FuaXphdGlvbjI5NzQwMjk2",           "avatar_url": "https://avatars2.githubusercontent.com/u/29740296?v=4",           "gravatar_id": "",           "url": "https://api.github.com/users/chaoss",           "html_url": "https://github.com/chaoss",           "followers_url": "https://api.github.com/users/chaoss/followers",           "following_url": "https://api.github.com/users/chaoss/following{/other_user}",           "gists_url": "https://api.github.com/users/chaoss/gists{/gist_id}",           "starred_url": "https://api.github.com/users/chaoss/starred{/owner}{/repo}",           "subscriptions_url": "https://api.github.com/users/chaoss/subscriptions",           "organizations_url": "https://api.github.com/users/chaoss/orgs",           "repos_url": "https://api.github.com/users/chaoss/repos",           "events_url": "https://api.github.com/users/chaoss/events{/privacy}",           "received_events_url": "https://api.github.com/users/chaoss/received_events",           "type": "Organization",           "site_admin": false         }, ';

-- ----------------------------
-- Table structure for pull_request_repo
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_repo";
CREATE TABLE "augur_data"."pull_request_repo" (
  "pr_repo_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_repo_pr_repo_id_seq'::regclass),
  "pr_repo_meta_id" int8,
  "pr_repo_head_or_base" varchar COLLATE "pg_catalog"."default",
  "pr_src_repo_id" int8,
  "pr_src_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_repo_name" varchar COLLATE "pg_catalog"."default",
  "pr_repo_full_name" varchar COLLATE "pg_catalog"."default",
  "pr_repo_private_bool" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "pr_cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_repo" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_repo"."pr_repo_head_or_base" IS 'For ease of validation checking, we should determine if the repository referenced is the head or base of the pull request. Each pull request should have one and only one of these, which is not enforceable easily in the database.';
COMMENT ON TABLE "augur_data"."pull_request_repo" IS 'This table is for storing information about forks that exist as part of a pull request. Generally we do not want to track these like ordinary repositories. ';

-- ----------------------------
-- Table structure for pull_request_review_message_ref
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_review_message_ref";
CREATE TABLE "augur_data"."pull_request_review_message_ref" (
  "pr_review_msg_ref_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_review_message_ref_pr_review_msg_ref_id_seq'::regclass),
  "pr_review_id" int8 NOT NULL,
  "repo_id" int8,
  "msg_id" int8 NOT NULL,
  "pr_review_msg_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_src_id" int8,
  "pr_review_msg_src_id" int8,
  "pr_review_msg_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_diff_hunk" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_path" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_position" int8,
  "pr_review_msg_original_position" int8,
  "pr_review_msg_commit_id" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_original_commit_id" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_updated_at" timestamp(6),
  "pr_review_msg_html_url" varchar COLLATE "pg_catalog"."default",
  "pr_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_author_association" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_start_line" int8,
  "pr_review_msg_original_start_line" int8,
  "pr_review_msg_start_side" varchar COLLATE "pg_catalog"."default",
  "pr_review_msg_line" int8,
  "pr_review_msg_original_line" int8,
  "pr_review_msg_side" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_review_message_ref" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_reviewers
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_reviewers";
CREATE TABLE "augur_data"."pull_request_reviewers" (
  "pr_reviewer_map_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_reviewers_pr_reviewer_map_id_seq'::regclass),
  "pull_request_id" int8,
  "pr_source_id" int8,
  "repo_id" int8,
  "pr_reviewer_src_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid
)
;
ALTER TABLE "augur_data"."pull_request_reviewers" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_request_reviewers"."pr_source_id" IS 'The platform ID for the pull/merge request. Used as part of the natural key, along with pr_reviewer_src_id in this table. ';
COMMENT ON COLUMN "augur_data"."pull_request_reviewers"."pr_reviewer_src_id" IS 'The platform ID for the pull/merge request reviewer. Used as part of the natural key, along with pr_source_id in this table. ';

-- ----------------------------
-- Table structure for pull_request_reviews
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_reviews";
CREATE TABLE "augur_data"."pull_request_reviews" (
  "pr_review_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_reviews_pr_review_id_seq'::regclass),
  "pull_request_id" int8 NOT NULL,
  "repo_id" int8,
  "pr_review_author_association" varchar COLLATE "pg_catalog"."default",
  "pr_review_state" varchar COLLATE "pg_catalog"."default",
  "pr_review_body" varchar COLLATE "pg_catalog"."default",
  "pr_review_submitted_at" timestamp(6),
  "pr_review_src_id" int8,
  "pr_review_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_review_html_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_pull_request_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_commit_id" varchar COLLATE "pg_catalog"."default",
  "platform_id" int8 DEFAULT 25150,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "cntrb_id" uuid NOT NULL
)
;
ALTER TABLE "augur_data"."pull_request_reviews" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_request_teams
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_request_teams";
CREATE TABLE "augur_data"."pull_request_teams" (
  "pr_team_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_request_teams_pr_team_id_seq'::regclass),
  "pull_request_id" int8,
  "pr_src_team_id" int8,
  "pr_src_team_node" varchar COLLATE "pg_catalog"."default",
  "pr_src_team_url" varchar COLLATE "pg_catalog"."default",
  "pr_team_name" varchar COLLATE "pg_catalog"."default",
  "pr_team_slug" varchar COLLATE "pg_catalog"."default",
  "pr_team_description" varchar COLLATE "pg_catalog"."default",
  "pr_team_privacy" varchar COLLATE "pg_catalog"."default",
  "pr_team_permission" varchar COLLATE "pg_catalog"."default",
  "pr_team_src_members_url" varchar COLLATE "pg_catalog"."default",
  "pr_team_src_repositories_url" varchar COLLATE "pg_catalog"."default",
  "pr_team_parent_id" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."pull_request_teams" OWNER TO "augur";

-- ----------------------------
-- Table structure for pull_requests
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."pull_requests";
CREATE TABLE "augur_data"."pull_requests" (
  "pull_request_id" int8 NOT NULL DEFAULT nextval('"augur_data".pull_requests_pull_request_id_seq'::regclass),
  "repo_id" int8 DEFAULT 0,
  "pr_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_id" int8,
  "pr_src_node_id" varchar COLLATE "pg_catalog"."default",
  "pr_html_url" varchar COLLATE "pg_catalog"."default",
  "pr_diff_url" varchar COLLATE "pg_catalog"."default",
  "pr_patch_url" varchar COLLATE "pg_catalog"."default",
  "pr_issue_url" varchar COLLATE "pg_catalog"."default",
  "pr_augur_issue_id" int8,
  "pr_src_number" int8,
  "pr_src_state" varchar COLLATE "pg_catalog"."default",
  "pr_src_locked" bool,
  "pr_src_title" varchar COLLATE "pg_catalog"."default",
  "pr_body" text COLLATE "pg_catalog"."default",
  "pr_created_at" timestamp(0),
  "pr_updated_at" timestamp(0),
  "pr_closed_at" timestamp(0),
  "pr_merged_at" timestamp(0),
  "pr_merge_commit_sha" varchar COLLATE "pg_catalog"."default",
  "pr_teams" int8,
  "pr_milestone" varchar COLLATE "pg_catalog"."default",
  "pr_commits_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_comments_url" varchar COLLATE "pg_catalog"."default",
  "pr_review_comment_url" varchar COLLATE "pg_catalog"."default",
  "pr_comments_url" varchar COLLATE "pg_catalog"."default",
  "pr_statuses_url" varchar COLLATE "pg_catalog"."default",
  "pr_meta_head_id" varchar COLLATE "pg_catalog"."default",
  "pr_meta_base_id" varchar COLLATE "pg_catalog"."default",
  "pr_src_issue_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_comments_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_review_comments_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_commits_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_statuses_url" varchar COLLATE "pg_catalog"."default",
  "pr_src_author_association" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "pr_augur_contributor_id" uuid
)
TABLESPACE "speed"
;
ALTER TABLE "augur_data"."pull_requests" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_src_id" IS 'The pr_src_id is unique across all of github.';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_augur_issue_id" IS 'This is to link to the augur stored related issue';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_src_number" IS 'The pr_src_number is unique within a repository.';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_teams" IS 'One to many with pull request teams. ';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_review_comment_url" IS 'This is a field with limited utility. It does expose how to access a specific comment if needed with parameters. If the source changes URL structure, it may be useful';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_meta_head_id" IS 'The metadata for the head repo that links to the pull_request_meta table. ';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_meta_base_id" IS 'The metadata for the base repo that links to the pull_request_meta table. ';
COMMENT ON COLUMN "augur_data"."pull_requests"."pr_augur_contributor_id" IS 'This is to link to the augur contributor record. ';

-- ----------------------------
-- Table structure for releases
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."releases";
CREATE TABLE "augur_data"."releases" (
  "release_id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "repo_id" int8 NOT NULL,
  "release_name" varchar COLLATE "pg_catalog"."default",
  "release_description" varchar COLLATE "pg_catalog"."default",
  "release_author" varchar COLLATE "pg_catalog"."default",
  "release_created_at" timestamp(6),
  "release_published_at" timestamp(6),
  "release_updated_at" timestamp(6),
  "release_is_draft" bool,
  "release_is_prerelease" bool,
  "release_tag_name" varchar COLLATE "pg_catalog"."default",
  "release_url" varchar COLLATE "pg_catalog"."default",
  "tag_only" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(6) DEFAULT CURRENT_TIMESTAMP,
  "user_data.age" varchar(255) COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_data"."releases" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo";
CREATE TABLE "augur_data"."repo" (
  "repo_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_repo_id_seq'::regclass),
  "repo_group_id" int8 NOT NULL,
  "repo_git" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "repo_path" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "repo_name" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "repo_added" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "repo_type" varchar COLLATE "pg_catalog"."default" DEFAULT ''::character varying,
  "url" varchar COLLATE "pg_catalog"."default",
  "owner_id" int4,
  "description" varchar COLLATE "pg_catalog"."default",
  "primary_language" varchar COLLATE "pg_catalog"."default",
  "created_at" varchar COLLATE "pg_catalog"."default",
  "forked_from" varchar COLLATE "pg_catalog"."default",
  "updated_at" timestamp(0),
  "repo_archived_date_collected" timestamptz(0),
  "repo_archived" int4,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "repo_src_id" int8
)
;
ALTER TABLE "augur_data"."repo" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo"."repo_type" IS 'This field is intended to indicate if the repository is the "main instance" of a repository in cases where implementations choose to add the same repository to more than one repository group. In cases where the repository group is of rg_type Github Organization then this repo_type should be "primary". In other cases the repo_type should probably be "user created". We made this a varchar in order to hold open the possibility that there are additional repo_types we have not thought about. ';
COMMENT ON TABLE "augur_data"."repo" IS 'This table is a combination of the columns in Facade’s repo table and GHTorrent’s projects table. ';

-- ----------------------------
-- Table structure for repo_badging
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_badging";
CREATE TABLE "augur_data"."repo_badging" (
  "badge_collection_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_badging_badge_collection_id_seq'::regclass),
  "repo_id" int8,
  "created_at" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "data" jsonb
)
;
ALTER TABLE "augur_data"."repo_badging" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."repo_badging" IS 'This will be collected from the LF’s Badging API
https://bestpractices.coreinfrastructure.org/projects.json?pq=https%3A%2F%2Fgithub.com%2Fchaoss%2Faugur
';

-- ----------------------------
-- Table structure for repo_clones_data
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_clones_data";
CREATE TABLE "augur_data"."repo_clones_data" (
  "repo_clone_data_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_clones_data_id_seq'::regclass),
  "repo_id" int8 NOT NULL,
  "unique_clones" int8,
  "count_clones" int8,
  "clone_data_timestamp" timestamp(6)
)
;
ALTER TABLE "augur_data"."repo_clones_data" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_cluster_messages
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_cluster_messages";
CREATE TABLE "augur_data"."repo_cluster_messages" (
  "msg_cluster_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_cluster_messages_msg_cluster_id_seq'::regclass),
  "repo_id" int8,
  "cluster_content" int4,
  "cluster_mechanism" int4,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_cluster_messages" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_dependencies
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_dependencies";
CREATE TABLE "augur_data"."repo_dependencies" (
  "repo_dependencies_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_dependencies_repo_dependencies_id_seq'::regclass),
  "repo_id" int8,
  "dep_name" varchar COLLATE "pg_catalog"."default",
  "dep_count" int4,
  "dep_language" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_dependencies" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo_dependencies"."repo_id" IS 'Foreign key for repo id. ';
COMMENT ON COLUMN "augur_data"."repo_dependencies"."dep_name" IS 'Name of the dependency found in project. ';
COMMENT ON COLUMN "augur_data"."repo_dependencies"."dep_count" IS 'Number of times the dependency was found. ';
COMMENT ON COLUMN "augur_data"."repo_dependencies"."dep_language" IS 'Language of the dependency. ';
COMMENT ON TABLE "augur_data"."repo_dependencies" IS 'Contains the dependencies for a repo.';

-- ----------------------------
-- Table structure for repo_deps_libyear
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_deps_libyear";
CREATE TABLE "augur_data"."repo_deps_libyear" (
  "repo_deps_libyear_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_deps_libyear_repo_deps_libyear_id_seq'::regclass),
  "repo_id" int8,
  "name" varchar COLLATE "pg_catalog"."default",
  "requirement" varchar COLLATE "pg_catalog"."default",
  "type" varchar COLLATE "pg_catalog"."default",
  "package_manager" varchar COLLATE "pg_catalog"."default",
  "current_version" varchar COLLATE "pg_catalog"."default",
  "latest_version" varchar COLLATE "pg_catalog"."default",
  "current_release_date" varchar COLLATE "pg_catalog"."default",
  "latest_release_date" varchar COLLATE "pg_catalog"."default",
  "libyear" float8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_deps_libyear" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_deps_scorecard
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_deps_scorecard";
CREATE TABLE "augur_data"."repo_deps_scorecard" (
  "repo_deps_scorecard_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_deps_scorecard_repo_deps_scorecard_id_seq1'::regclass),
  "repo_id" int8,
  "name" varchar COLLATE "pg_catalog"."default",
  "score" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "scorecard_check_details" jsonb
)
;
ALTER TABLE "augur_data"."repo_deps_scorecard" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_group_insights
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_group_insights";
CREATE TABLE "augur_data"."repo_group_insights" (
  "rgi_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_group_insights_rgi_id_seq'::regclass),
  "repo_group_id" int8,
  "rgi_metric" varchar COLLATE "pg_catalog"."default",
  "rgi_value" varchar COLLATE "pg_catalog"."default",
  "cms_id" int8,
  "rgi_fresh" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_group_insights" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo_group_insights"."rgi_fresh" IS 'false if the date is before the statistic that triggered the insight, true if after. This allows us to automatically display only "fresh insights" and avoid displaying "stale insights". The insight worker will populate this table. ';
COMMENT ON TABLE "augur_data"."repo_group_insights" IS 'This table is output from an analytical worker inside of Augur. It runs through the different metrics on a REPOSITORY_GROUP and identifies the five to ten most “interesting” metrics as defined by some kind of delta or other factor. The algorithm is going to evolve. 

Worker Design Notes: The idea is that the "insight worker" will scan through a bunch of active metrics or "synthetic metrics" to list the most important insights. ';

-- ----------------------------
-- Table structure for repo_groups
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_groups";
CREATE TABLE "augur_data"."repo_groups" (
  "repo_group_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_groups_repo_group_id_seq'::regclass),
  "rg_name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "rg_description" varchar COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "rg_website" varchar(128) COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "rg_recache" int2 DEFAULT 1,
  "rg_last_modified" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "rg_type" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."repo_groups" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."repo_groups" IS 'rg_type is intended to be either a GitHub Organization or a User Created Repo Group. ';

-- ----------------------------
-- Table structure for repo_groups_list_serve
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_groups_list_serve";
CREATE TABLE "augur_data"."repo_groups_list_serve" (
  "rgls_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_groups_list_serve_rgls_id_seq'::regclass),
  "repo_group_id" int8 NOT NULL,
  "rgls_name" varchar COLLATE "pg_catalog"."default",
  "rgls_description" varchar(3000) COLLATE "pg_catalog"."default",
  "rgls_sponsor" varchar COLLATE "pg_catalog"."default",
  "rgls_email" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."repo_groups_list_serve" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_info
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_info";
CREATE TABLE "augur_data"."repo_info" (
  "repo_info_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_info_repo_info_id_seq'::regclass),
  "repo_id" int8 NOT NULL,
  "last_updated" timestamp(0) DEFAULT NULL::timestamp without time zone,
  "issues_enabled" varchar COLLATE "pg_catalog"."default",
  "open_issues" int4,
  "pull_requests_enabled" varchar COLLATE "pg_catalog"."default",
  "wiki_enabled" varchar COLLATE "pg_catalog"."default",
  "pages_enabled" varchar COLLATE "pg_catalog"."default",
  "fork_count" int4,
  "default_branch" varchar COLLATE "pg_catalog"."default",
  "watchers_count" int4,
  "UUID" int4,
  "license" varchar COLLATE "pg_catalog"."default",
  "stars_count" int4,
  "committers_count" int4,
  "issue_contributors_count" varchar COLLATE "pg_catalog"."default",
  "changelog_file" varchar COLLATE "pg_catalog"."default",
  "contributing_file" varchar COLLATE "pg_catalog"."default",
  "license_file" varchar COLLATE "pg_catalog"."default",
  "code_of_conduct_file" varchar COLLATE "pg_catalog"."default",
  "security_issue_file" varchar COLLATE "pg_catalog"."default",
  "security_audit_file" varchar COLLATE "pg_catalog"."default",
  "status" varchar COLLATE "pg_catalog"."default",
  "keywords" varchar COLLATE "pg_catalog"."default",
  "commit_count" int8,
  "issues_count" int8,
  "issues_closed" int8,
  "pull_request_count" int8,
  "pull_requests_open" int8,
  "pull_requests_closed" int8,
  "pull_requests_merged" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_info" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_insights
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_insights";
CREATE TABLE "augur_data"."repo_insights" (
  "ri_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_insights_ri_id_seq'::regclass),
  "repo_id" int8,
  "ri_metric" varchar COLLATE "pg_catalog"."default",
  "ri_value" varchar COLLATE "pg_catalog"."default",
  "ri_date" timestamp(0),
  "ri_fresh" bool,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP,
  "ri_score" numeric,
  "ri_field" varchar COLLATE "pg_catalog"."default",
  "ri_detection_method" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_data"."repo_insights" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo_insights"."ri_fresh" IS 'false if the date is before the statistic that triggered the insight, true if after. This allows us to automatically display only "fresh insights" and avoid displaying "stale insights". The insight worker will populate this table. ';
COMMENT ON TABLE "augur_data"."repo_insights" IS 'This table is output from an analytical worker inside of Augur. It runs through the different metrics on a repository and identifies the five to ten most “interesting” metrics as defined by some kind of delta or other factor. The algorithm is going to evolve. 

Worker Design Notes: The idea is that the "insight worker" will scan through a bunch of active metrics or "synthetic metrics" to list the most important insights. ';

-- ----------------------------
-- Table structure for repo_insights_records
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_insights_records";
CREATE TABLE "augur_data"."repo_insights_records" (
  "ri_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_insights_records_ri_id_seq'::regclass),
  "repo_id" int8,
  "ri_metric" varchar COLLATE "pg_catalog"."default",
  "ri_field" varchar COLLATE "pg_catalog"."default",
  "ri_value" varchar COLLATE "pg_catalog"."default",
  "ri_date" timestamp(6),
  "ri_score" float8,
  "ri_detection_method" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(6) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_insights_records" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_id" IS 'Primary key. ';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."repo_id" IS 'Refers to repo table primary key. Will have a foreign key';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_metric" IS 'The metric endpoint';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_field" IS 'The field in the metric endpoint';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_value" IS 'The value of the endpoint in ri_field';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_date" IS 'The date the insight is for; in other words, some anomaly occurred on this date. ';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_score" IS 'A Score, derived from the algorithm used. ';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."ri_detection_method" IS 'A confidence interval or other expression of the type of threshold and the value of a threshold met in order for it to be "an insight". Example. "95% confidence interval". ';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."tool_source" IS 'Standard Augur Metadata';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."tool_version" IS 'Standard Augur Metadata';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."data_source" IS 'Standard Augur Metadata';
COMMENT ON COLUMN "augur_data"."repo_insights_records"."data_collection_date" IS 'Standard Augur Metadata';

-- ----------------------------
-- Table structure for repo_labor
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_labor";
CREATE TABLE "augur_data"."repo_labor" (
  "repo_labor_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_labor_repo_labor_id_seq'::regclass),
  "repo_id" int8,
  "repo_clone_date" timestamp(0),
  "rl_analysis_date" timestamp(0),
  "programming_language" varchar COLLATE "pg_catalog"."default",
  "file_path" varchar COLLATE "pg_catalog"."default",
  "file_name" varchar COLLATE "pg_catalog"."default",
  "total_lines" int4,
  "code_lines" int4,
  "comment_lines" int4,
  "blank_lines" int4,
  "code_complexity" int4,
  "repo_url" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."repo_labor" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."repo_labor"."repo_url" IS 'This is a convenience column to simplify analysis against external datasets';
COMMENT ON TABLE "augur_data"."repo_labor" IS 'repo_labor is a derivative of tables used to store scc code and complexity counting statistics that are inputs to labor analysis, which are components of CHAOSS value metric calculations. ';

-- ----------------------------
-- Table structure for repo_meta
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_meta";
CREATE TABLE "augur_data"."repo_meta" (
  "repo_id" int8 NOT NULL,
  "rmeta_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_meta_rmeta_id_seq'::regclass),
  "rmeta_name" varchar COLLATE "pg_catalog"."default",
  "rmeta_value" varchar COLLATE "pg_catalog"."default" DEFAULT 0,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."repo_meta" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."repo_meta" IS 'Project Languages';

-- ----------------------------
-- Table structure for repo_sbom_scans
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_sbom_scans";
CREATE TABLE "augur_data"."repo_sbom_scans" (
  "rsb_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_sbom_scans_rsb_id_seq'::regclass),
  "repo_id" int8,
  "sbom_scan" json
)
;
ALTER TABLE "augur_data"."repo_sbom_scans" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_stats
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_stats";
CREATE TABLE "augur_data"."repo_stats" (
  "repo_id" int8 NOT NULL,
  "rstat_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_stats_rstat_id_seq'::regclass),
  "rstat_name" varchar(400) COLLATE "pg_catalog"."default",
  "rstat_value" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0)
)
;
ALTER TABLE "augur_data"."repo_stats" OWNER TO "augur";
COMMENT ON TABLE "augur_data"."repo_stats" IS 'Project Watchers';

-- ----------------------------
-- Table structure for repo_test_coverage
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_test_coverage";
CREATE TABLE "augur_data"."repo_test_coverage" (
  "repo_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_test_coverage_repo_id_seq'::regclass),
  "repo_clone_date" timestamp(0),
  "rtc_analysis_date" timestamp(0),
  "programming_language" varchar COLLATE "pg_catalog"."default",
  "file_path" varchar COLLATE "pg_catalog"."default",
  "file_name" varchar COLLATE "pg_catalog"."default",
  "testing_tool" varchar COLLATE "pg_catalog"."default",
  "file_statement_count" int8,
  "file_subroutine_count" int8,
  "file_statements_tested" int8,
  "file_subroutines_tested" int8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_test_coverage" OWNER TO "augur";

-- ----------------------------
-- Table structure for repo_topic
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repo_topic";
CREATE TABLE "augur_data"."repo_topic" (
  "repo_topic_id" int8 NOT NULL DEFAULT nextval('"augur_data".repo_topic_repo_topic_id_seq'::regclass),
  "repo_id" int8,
  "topic_id" int4,
  "topic_prob" float8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repo_topic" OWNER TO "augur";

-- ----------------------------
-- Table structure for repos_fetch_log
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."repos_fetch_log";
CREATE TABLE "augur_data"."repos_fetch_log" (
  "repos_id" int4 NOT NULL,
  "status" varchar(128) COLLATE "pg_catalog"."default" NOT NULL,
  "date" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."repos_fetch_log" OWNER TO "augur";

-- ----------------------------
-- Table structure for settings
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."settings";
CREATE TABLE "augur_data"."settings" (
  "id" int4 NOT NULL,
  "setting" varchar(32) COLLATE "pg_catalog"."default" NOT NULL,
  "value" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "last_modified" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."settings" OWNER TO "augur";

-- ----------------------------
-- Table structure for topic_model_event
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."topic_model_event";
CREATE TABLE "augur_data"."topic_model_event" (
  "event_id" int8 NOT NULL DEFAULT nextval('"augur_data".topic_model_event_event_id_seq'::regclass),
  "ts" timestamptz(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "repo_id" int4,
  "model_id" uuid,
  "event" text COLLATE "pg_catalog"."default" NOT NULL,
  "level" text COLLATE "pg_catalog"."default" NOT NULL DEFAULT 'INFO'::text,
  "payload" jsonb NOT NULL
)
;
ALTER TABLE "augur_data"."topic_model_event" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."topic_model_event"."event_id" IS 'Unique identifier for the event';
COMMENT ON COLUMN "augur_data"."topic_model_event"."ts" IS 'Timestamp when the event occurred';
COMMENT ON COLUMN "augur_data"."topic_model_event"."repo_id" IS 'Repository associated with this event';
COMMENT ON COLUMN "augur_data"."topic_model_event"."model_id" IS 'Topic model associated with this event';
COMMENT ON COLUMN "augur_data"."topic_model_event"."event" IS 'Event type or name';
COMMENT ON COLUMN "augur_data"."topic_model_event"."level" IS 'Log level (INFO, WARNING, ERROR, etc.)';
COMMENT ON COLUMN "augur_data"."topic_model_event"."payload" IS 'Event payload data';

-- ----------------------------
-- Table structure for topic_model_meta
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."topic_model_meta";
CREATE TABLE "augur_data"."topic_model_meta" (
  "model_id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "repo_id" int8,
  "model_method" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "num_topics" int4 NOT NULL,
  "num_words_per_topic" int4 NOT NULL,
  "training_parameters" json NOT NULL,
  "model_file_paths" json NOT NULL,
  "parameters_hash" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "coherence_score" float8 NOT NULL DEFAULT 0.0,
  "perplexity_score" float8 NOT NULL DEFAULT 0.0,
  "topic_diversity" float8 NOT NULL DEFAULT 0.0,
  "quality" json NOT NULL DEFAULT '{}'::jsonb,
  "training_message_count" int8 NOT NULL,
  "data_fingerprint" json NOT NULL,
  "visualization_data" json,
  "training_start_time" timestamptz(6) NOT NULL,
  "training_end_time" timestamptz(6) NOT NULL,
  "data_collection_date" timestamptz(6) DEFAULT CURRENT_TIMESTAMP,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "augur_data"."topic_model_meta" OWNER TO "augur";
COMMENT ON COLUMN "augur_data"."topic_model_meta"."model_id" IS 'Unique identifier for the topic model';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."repo_id" IS 'Repository this model was trained on';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."model_method" IS 'Method used for topic modeling (e.g., ''NMF_COUNT'', ''LDA_TFIDF'')';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."num_topics" IS 'Number of topics in the model';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."num_words_per_topic" IS 'Number of words per topic';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."training_parameters" IS 'JSON object containing training parameters';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."model_file_paths" IS 'JSON object containing paths to model artifacts';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."parameters_hash" IS 'Hash of parameters for deduplication';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."coherence_score" IS 'Coherence score of the model';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."perplexity_score" IS 'Perplexity score of the model';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."topic_diversity" IS 'Topic diversity score';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."quality" IS 'Quality metrics';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."training_message_count" IS 'Number of messages used for training';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."data_fingerprint" IS 'Fingerprint of training data';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."visualization_data" IS 'JSON object containing visualization data for the model';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."training_start_time" IS 'When training started';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."training_end_time" IS 'When training ended';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."tool_source" IS 'Standard Augur Metadata';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."tool_version" IS 'Standard Augur Metadata';
COMMENT ON COLUMN "augur_data"."topic_model_meta"."data_source" IS 'Standard Augur Metadata';

-- ----------------------------
-- Table structure for topic_words
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."topic_words";
CREATE TABLE "augur_data"."topic_words" (
  "topic_words_id" int8 NOT NULL DEFAULT nextval('"augur_data".topic_words_topic_words_id_seq'::regclass),
  "topic_id" int8,
  "word" varchar COLLATE "pg_catalog"."default",
  "word_prob" float8,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."topic_words" OWNER TO "augur";

-- ----------------------------
-- Table structure for unknown_cache
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."unknown_cache";
CREATE TABLE "augur_data"."unknown_cache" (
  "type" varchar(10) COLLATE "pg_catalog"."default" NOT NULL,
  "repo_group_id" int4 NOT NULL,
  "email" varchar(128) COLLATE "pg_catalog"."default" NOT NULL,
  "domain" varchar(128) COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying,
  "added" int8 NOT NULL,
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."unknown_cache" OWNER TO "augur";

-- ----------------------------
-- Table structure for unresolved_commit_emails
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."unresolved_commit_emails";
CREATE TABLE "augur_data"."unresolved_commit_emails" (
  "email_unresolved_id" int8 NOT NULL DEFAULT nextval('"augur_data".unresolved_commit_emails_email_unresolved_id_seq'::regclass),
  "email" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "name" varchar COLLATE "pg_catalog"."default",
  "tool_source" varchar COLLATE "pg_catalog"."default",
  "tool_version" varchar COLLATE "pg_catalog"."default",
  "data_source" varchar COLLATE "pg_catalog"."default",
  "data_collection_date" timestamp(0) DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."unresolved_commit_emails" OWNER TO "augur";

-- ----------------------------
-- Table structure for utility_log
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."utility_log";
CREATE TABLE "augur_data"."utility_log" (
  "id" int8 NOT NULL DEFAULT nextval('"augur_data".utility_log_id_seq1'::regclass),
  "level" varchar(8) COLLATE "pg_catalog"."default" NOT NULL,
  "status" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "attempted" timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;
ALTER TABLE "augur_data"."utility_log" OWNER TO "augur";

-- ----------------------------
-- Table structure for working_commits
-- ----------------------------
DROP TABLE IF EXISTS "augur_data"."working_commits";
CREATE TABLE "augur_data"."working_commits" (
  "repos_id" int4 NOT NULL,
  "working_commit" varchar(40) COLLATE "pg_catalog"."default" DEFAULT 'NULL'::character varying
)
;
ALTER TABLE "augur_data"."working_commits" OWNER TO "augur";

-- ----------------------------
-- Function structure for parallel_delete
-- ----------------------------
DROP FUNCTION IF EXISTS "augur_data"."parallel_delete"("batch_size" int4, "worker_id" int4, "total_workers" int4);
CREATE OR REPLACE FUNCTION "augur_data"."parallel_delete"("batch_size" int4, "worker_id" int4, "total_workers" int4)
  RETURNS "pg_catalog"."void" AS $BODY$
DECLARE
    rows_deleted INT;
BEGIN
    LOOP
        -- Delete in batches while ensuring non-overlapping processing
        DELETE FROM pull_request_reviews 
        WHERE repo_id IN (
            SELECT repo_id FROM _temp_repo_ids 
            WHERE MOD(repo_id, total_workers) = worker_id
            LIMIT batch_size 
            FOR UPDATE SKIP LOCKED
        );

        -- Count how many rows were deleted
        GET DIAGNOSTICS rows_deleted = ROW_COUNT;

        -- If no rows were deleted, exit the loop
        EXIT WHEN rows_deleted = 0;

        COMMIT;
    END LOOP;
END $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION "augur_data"."parallel_delete"("batch_size" int4, "worker_id" int4, "total_workers" int4) OWNER TO "augur";

-- ----------------------------
-- Procedure structure for refresh_aggregates
-- ----------------------------
DROP PROCEDURE IF EXISTS "augur_data"."refresh_aggregates"();
CREATE OR REPLACE PROCEDURE "augur_data"."refresh_aggregates"()
 AS $BODY$
    begin
        perform pg_advisory_lock(124);
        execute 'REFRESH MATERIALIZED VIEW "augur_data"."issue_reporter_created_at"';
        perform pg_advisory_unlock(124);
    end;
$BODY$
  LANGUAGE plpgsql;
ALTER PROCEDURE "augur_data"."refresh_aggregates"() OWNER TO "augur";

-- ----------------------------
-- Function structure for user_mat_views
-- ----------------------------
DROP FUNCTION IF EXISTS "augur_data"."user_mat_views"();
CREATE OR REPLACE FUNCTION "augur_data"."user_mat_views"()
  RETURNS "pg_catalog"."trigger" AS $BODY$
BEGIN
  REFRESH MATERIALIZED VIEW augur_data.explorer_user_repos; 
	RETURN NULL; 

END$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION "augur_data"."user_mat_views"() OWNER TO "augur";

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."augur_data.repo_insights_ri_id_seq"', 25431, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."chaoss_metric_status_cms_id_seq"', 10082, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_data"."chaoss_user_chaoss_id_seq"
OWNED BY "augur_data"."chaoss_user"."chaoss_id";
SELECT setval('"augur_data"."chaoss_user_chaoss_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."commit_comment_ref_cmt_comment_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."commit_parents_parent_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."commits_cmt_id_seq"', 487165811049, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributor_affiliations_ca_id_seq"', 35165, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributor_repo_cntrb_repo_id_seq"', 878680694, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributors_aliases_cntrb_a_id_seq"', 25431, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributors_aliases_cntrb_alias_id_seq"', 1096890, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributors_cntrb_id_seq"', 25431, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."contributors_history_cntrb_history_id_seq"', 25431, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_data"."dei_badging_id_seq"
OWNED BY "augur_data"."akl;fjlk;a"."id";
SELECT setval('"augur_data"."dei_badging_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."discourse_insights_msg_discourse_id_seq"', 2, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."discourse_insights_msg_discourse_id_seq1"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."issue_assignees_issue_assignee_id_seq"', 127833165, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."issue_events_event_id_seq"', 1276892357, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."issue_labels_issue_label_id_seq"', 656130835, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."issue_message_ref_issue_msg_ref_id_seq"', 956092741, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."issue_seq"', 386235955, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."libraries_library_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."library_dependencies_lib_dependency_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."library_version_library_version_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."lstm_anomaly_models_model_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."lstm_anomaly_results_result_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."message_analysis_msg_analysis_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."message_analysis_summary_msg_summary_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."message_msg_id_seq"', 2383127525, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."message_sentiment_msg_analysis_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."message_sentiment_summary_msg_summary_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."platform_pltfrm_id_seq"', 10003, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_analysis_pull_request_analysis_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_assignees_pr_assignee_map_id_seq"', 134640329, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_commits_pr_cmt_id_seq"', 606972575, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_events_pr_event_id_seq"', 2169300316, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_files_pr_file_id_seq"', 1983998100, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_labels_pr_label_id_seq"', 840469729, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_message_ref_pr_msg_ref_id_seq"', 958096956, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_meta_pr_repo_meta_id_seq"', 1169582780, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_repo_pr_repo_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_review_message_ref_pr_review_msg_ref_id_seq"', 435696789, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_reviewers_pr_reviewer_map_id_seq"', 335858744, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_reviews_pr_review_id_seq"', 150884566950, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_request_teams_pr_team_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."pull_requests_pull_request_id_seq"', 594352749, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."releases_release_id_seq"', 2, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_badging_badge_collection_id_seq"', 1245809, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_clones_data_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_cluster_messages_msg_cluster_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_dependencies_repo_dependencies_id_seq"', 130956044, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_deps_libyear_repo_deps_libyear_id_seq"', 13634966, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_deps_scorecard_repo_deps_scorecard_id_seq1"', 34949727, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_group_insights_rgi_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_groups_list_serve_rgls_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_groups_repo_group_id_seq"', 36420, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_info_repo_info_id_seq"', 3827443, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_insights_records_ri_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_insights_ri_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_labor_repo_labor_id_seq"', 2449859733, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_meta_rmeta_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_repo_id_seq"', 311492, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_sbom_scans_rsb_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_stats_rstat_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_test_coverage_repo_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."repo_topic_repo_topic_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
ALTER SEQUENCE "augur_data"."topic_model_event_event_id_seq"
OWNED BY "augur_data"."topic_model_event"."event_id";
SELECT setval('"augur_data"."topic_model_event_event_id_seq"', 2, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."topic_words_topic_words_id_seq"', 10001, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."unresolved_commit_emails_email_unresolved_id_seq"', 5202860, true);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."utility_log_id_seq"', 2, false);

-- ----------------------------
-- Alter sequences owned by
-- ----------------------------
SELECT setval('"augur_data"."utility_log_id_seq1"', 80603851, true);

-- ----------------------------
-- Primary Key structure for table _transfer_testing
-- ----------------------------
ALTER TABLE "augur_data"."_transfer_testing" ADD CONSTRAINT "_transfer_testing_pkey" PRIMARY KEY ("repo_id");

-- ----------------------------
-- Primary Key structure for table akl;fjlk;a
-- ----------------------------
ALTER TABLE "augur_data"."akl;fjlk;a" ADD CONSTRAINT "dei_badging_pkey" PRIMARY KEY ("id", "repo_id");

-- ----------------------------
-- Indexes structure for table analysis_log
-- ----------------------------
CREATE INDEX "repos_id" ON "augur_data"."analysis_log" USING btree (
  "repos_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table chaoss_metric_status
-- ----------------------------
ALTER TABLE "augur_data"."chaoss_metric_status" ADD CONSTRAINT "chaoss_metric_status_pkey" PRIMARY KEY ("cms_id");

-- ----------------------------
-- Uniques structure for table chaoss_user
-- ----------------------------
ALTER TABLE "augur_data"."chaoss_user" ADD CONSTRAINT "chaoss_unique_email_key" UNIQUE ("chaoss_email");

-- ----------------------------
-- Primary Key structure for table chaoss_user
-- ----------------------------
ALTER TABLE "augur_data"."chaoss_user" ADD CONSTRAINT "chaoss_user_pkey" PRIMARY KEY ("chaoss_id");

-- ----------------------------
-- Indexes structure for table commit_comment_ref
-- ----------------------------
CREATE INDEX "comment_id" ON "augur_data"."commit_comment_ref" USING btree (
  "cmt_comment_src_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "cmt_comment_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table commit_comment_ref
-- ----------------------------
ALTER TABLE "augur_data"."commit_comment_ref" ADD CONSTRAINT "commitcomment" UNIQUE ("cmt_comment_src_id");

-- ----------------------------
-- Primary Key structure for table commit_comment_ref
-- ----------------------------
ALTER TABLE "augur_data"."commit_comment_ref" ADD CONSTRAINT "commit_comment_ref_pkey" PRIMARY KEY ("cmt_comment_id");

-- ----------------------------
-- Uniques structure for table commit_messages
-- ----------------------------
ALTER TABLE "augur_data"."commit_messages" ADD CONSTRAINT "commit-message-insert-unique" UNIQUE ("repo_id", "cmt_hash");

-- ----------------------------
-- Primary Key structure for table commit_messages
-- ----------------------------
ALTER TABLE "augur_data"."commit_messages" ADD CONSTRAINT "commit_messages_pkey" PRIMARY KEY ("cmt_msg_id");

-- ----------------------------
-- Indexes structure for table commit_parents
-- ----------------------------
CREATE INDEX "commit_parents_ibfk_1" ON "augur_data"."commit_parents" USING btree (
  "cmt_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "commit_parents_ibfk_2" ON "augur_data"."commit_parents" USING btree (
  "parent_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table commit_parents
-- ----------------------------
ALTER TABLE "augur_data"."commit_parents" ADD CONSTRAINT "commit_parents_pkey" PRIMARY KEY ("cmt_id", "parent_id");

-- ----------------------------
-- Indexes structure for table commits
-- ----------------------------
CREATE INDEX "author_affiliation" ON "augur_data"."commits" USING btree (
  "cmt_author_affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "author_cntrb_id" ON "augur_data"."commits" USING btree (
  "cmt_ght_author_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "author_email,author_affiliation,author_date" ON "augur_data"."commits" USING btree (
  "cmt_author_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "cmt_author_affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "cmt_author_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "author_raw_email" ON "augur_data"."commits" USING btree (
  "cmt_author_raw_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "committed" ON "augur_data"."commits" USING btree (
  "cmt_id" "pg_catalog"."int8_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "commits_idx_cmt_email_cmt_date_cmt_name" ON "augur_data"."commits" USING btree (
  "cmt_author_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "cmt_author_date" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "cmt_author_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "committer_affiliation" ON "augur_data"."commits" USING btree (
  "cmt_committer_affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "committer_raw_email" ON "augur_data"."commits" USING btree (
  "cmt_committer_raw_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";
CREATE INDEX "idx_commits_hash_metrics" ON "augur_data"."commits" USING btree (
  "cmt_commit_hash" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "cmt_added" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "cmt_removed" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "cmt_filename" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,commit" ON "augur_data"."commits" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "cmt_commit_hash" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) TABLESPACE "speed";

-- ----------------------------
-- Primary Key structure for table commits
-- ----------------------------
ALTER TABLE "augur_data"."commits" ADD CONSTRAINT "commits_pkey" PRIMARY KEY ("cmt_id") USING INDEX TABLESPACE "speed";

-- ----------------------------
-- Uniques structure for table contributor_affiliations
-- ----------------------------
ALTER TABLE "augur_data"."contributor_affiliations" ADD CONSTRAINT "unique_domain" UNIQUE ("ca_domain");
COMMENT ON CONSTRAINT "unique_domain" ON "augur_data"."contributor_affiliations" IS 'Only one row should exist for any given top level domain or subdomain. ';

-- ----------------------------
-- Primary Key structure for table contributor_affiliations
-- ----------------------------
ALTER TABLE "augur_data"."contributor_affiliations" ADD CONSTRAINT "contributor_affiliations_pkey" PRIMARY KEY ("ca_id");

-- ----------------------------
-- Uniques structure for table contributor_repo
-- ----------------------------
ALTER TABLE "augur_data"."contributor_repo" ADD CONSTRAINT "eventer" UNIQUE ("event_id", "tool_version");

-- ----------------------------
-- Primary Key structure for table contributor_repo
-- ----------------------------
ALTER TABLE "augur_data"."contributor_repo" ADD CONSTRAINT "cntrb_repo_id_key" PRIMARY KEY ("cntrb_repo_id");

-- ----------------------------
-- Indexes structure for table contributors
-- ----------------------------
CREATE INDEX "cnt_fullname_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_full_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "cntrb_canonical_idx_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_canonical" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "cntrb_theemail_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "contributor_worker_email_finder_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "contributor_worker_fullname_finder_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_full_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "contributors_idx_cntrb_email3_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "gh_login_new2" ON "augur_data"."contributors" USING btree (
  "gh_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "idx_contributors_bot_login2" ON "augur_data"."contributors" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
) WHERE cntrb_login::text ~~ '%[bot]'::text;
CREATE INDEX "login_contributor_idx_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "login_new2" ON "augur_data"."contributors" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table contributors
-- ----------------------------
ALTER TABLE "augur_data"."contributors" ADD CONSTRAINT "contributors_pkey" PRIMARY KEY ("cntrb_id");

-- ----------------------------
-- Uniques structure for table contributors_aliases
-- ----------------------------
ALTER TABLE "augur_data"."contributors_aliases" ADD CONSTRAINT "contributor-alias-unique" UNIQUE ("alias_email");

-- ----------------------------
-- Primary Key structure for table contributors_aliases
-- ----------------------------
ALTER TABLE "augur_data"."contributors_aliases" ADD CONSTRAINT "contributors_aliases_pkey" PRIMARY KEY ("cntrb_alias_id");

-- ----------------------------
-- Indexes structure for table contributors_old
-- ----------------------------
CREATE INDEX "cnt-fullname" ON "augur_data"."contributors_old" USING hash (
  "cntrb_full_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops"
);
CREATE INDEX "cntrb-theemail" ON "augur_data"."contributors_old" USING hash (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops"
);
CREATE INDEX "cntrb_canonica-idx11" ON "augur_data"."contributors_old" USING btree (
  "cntrb_canonical" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "cntrb_login_platform_index" ON "augur_data"."contributors_old" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "contributor_worker_email_finder" ON "augur_data"."contributors_old" USING brin (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_minmax_ops"
);
CREATE INDEX "contributor_worker_fullname_finder" ON "augur_data"."contributors_old" USING brin (
  "cntrb_full_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_minmax_ops"
);
CREATE INDEX "contributors_idx_cntrb_email3" ON "augur_data"."contributors_old" USING btree (
  "cntrb_email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "gh_login" ON "augur_data"."contributors_old" USING btree (
  "gh_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS FIRST
);
CREATE INDEX "login" ON "augur_data"."contributors_old" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "login-contributor-idx" ON "augur_data"."contributors_old" USING btree (
  "cntrb_login" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table contributors_old
-- ----------------------------
ALTER TABLE "augur_data"."contributors_old" ADD CONSTRAINT "GL-UNIQUE-B" UNIQUE ("gl_id") DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."contributors_old" ADD CONSTRAINT "GL-UNIQUE-C" UNIQUE ("gl_username") DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Primary Key structure for table contributors_old
-- ----------------------------
ALTER TABLE "augur_data"."contributors_old" ADD CONSTRAINT "contributors-pk" PRIMARY KEY ("cntrb_id");

-- ----------------------------
-- Primary Key structure for table discourse_insights
-- ----------------------------
ALTER TABLE "augur_data"."discourse_insights" ADD CONSTRAINT "discourse_insights_pkey" PRIMARY KEY ("msg_discourse_id");

-- ----------------------------
-- Indexes structure for table dm_repo_annual
-- ----------------------------
CREATE INDEX "repo_id,affiliation_copy_1" ON "augur_data"."dm_repo_annual" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,email_copy_1" ON "augur_data"."dm_repo_annual" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table dm_repo_group_annual
-- ----------------------------
CREATE INDEX "projects_id,affiliation_copy_1" ON "augur_data"."dm_repo_group_annual" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,email_copy_1" ON "augur_data"."dm_repo_group_annual" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table dm_repo_group_monthly
-- ----------------------------
CREATE INDEX "projects_id,affiliation_copy_2" ON "augur_data"."dm_repo_group_monthly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,email_copy_2" ON "augur_data"."dm_repo_group_monthly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,year,affiliation_copy_1" ON "augur_data"."dm_repo_group_monthly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,year,email_copy_1" ON "augur_data"."dm_repo_group_monthly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table dm_repo_group_weekly
-- ----------------------------
CREATE INDEX "projects_id,affiliation" ON "augur_data"."dm_repo_group_weekly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,email" ON "augur_data"."dm_repo_group_weekly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,year,affiliation" ON "augur_data"."dm_repo_group_weekly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "projects_id,year,email" ON "augur_data"."dm_repo_group_weekly" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table dm_repo_monthly
-- ----------------------------
CREATE INDEX "repo_id,affiliation_copy_2" ON "augur_data"."dm_repo_monthly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,email_copy_2" ON "augur_data"."dm_repo_monthly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,year,affiliation_copy_1" ON "augur_data"."dm_repo_monthly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,year,email_copy_1" ON "augur_data"."dm_repo_monthly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Indexes structure for table dm_repo_weekly
-- ----------------------------
CREATE INDEX "repo_id,affiliation" ON "augur_data"."dm_repo_weekly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,email" ON "augur_data"."dm_repo_weekly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,year,affiliation" ON "augur_data"."dm_repo_weekly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "affiliation" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_id,year,email" ON "augur_data"."dm_repo_weekly" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "year" "pg_catalog"."int2_ops" ASC NULLS LAST,
  "email" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table exclude
-- ----------------------------
ALTER TABLE "augur_data"."exclude" ADD CONSTRAINT "exclude_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Primary Key structure for table historical_repo_urls
-- ----------------------------
ALTER TABLE "augur_data"."historical_repo_urls" ADD CONSTRAINT "historical_repo_urls_pkey" PRIMARY KEY ("repo_id", "git_url");

-- ----------------------------
-- Indexes structure for table issue_assignees
-- ----------------------------
CREATE INDEX "issue-cntrb-assign-idx-1" ON "augur_data"."issue_assignees" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table issue_assignees
-- ----------------------------
ALTER TABLE "augur_data"."issue_assignees" ADD CONSTRAINT "issue-assignee-insert-unique" UNIQUE ("issue_assignee_src_id", "issue_id");

-- ----------------------------
-- Primary Key structure for table issue_assignees
-- ----------------------------
ALTER TABLE "augur_data"."issue_assignees" ADD CONSTRAINT "issue_assignees_pkey" PRIMARY KEY ("issue_assignee_id");

-- ----------------------------
-- Indexes structure for table issue_events
-- ----------------------------
CREATE INDEX "idx_issue_events_issue" ON "augur_data"."issue_events" USING btree (
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issue-cntrb-idx2" ON "augur_data"."issue_events" USING btree (
  "issue_event_src_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issue_events_ibfk_1" ON "augur_data"."issue_events" USING btree (
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issue_events_ibfk_2" ON "augur_data"."issue_events" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table issue_events
-- ----------------------------
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "unique_event_id_key" UNIQUE ("issue_id", "issue_event_src_id");
COMMENT ON CONSTRAINT "unique_event_id_key" ON "augur_data"."issue_events" IS 'Natural key for issue events. ';

-- ----------------------------
-- Primary Key structure for table issue_events
-- ----------------------------
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "issue_events_pkey" PRIMARY KEY ("event_id");

-- ----------------------------
-- Uniques structure for table issue_labels
-- ----------------------------
ALTER TABLE "augur_data"."issue_labels" ADD CONSTRAINT "unique_issue_label" UNIQUE ("label_src_id", "issue_id");

-- ----------------------------
-- Primary Key structure for table issue_labels
-- ----------------------------
ALTER TABLE "augur_data"."issue_labels" ADD CONSTRAINT "issue_labels_pkey" PRIMARY KEY ("issue_label_id");

-- ----------------------------
-- Indexes structure for table issue_message_ref
-- ----------------------------
CREATE INDEX "idx_issue_message_ref_issue" ON "augur_data"."issue_message_ref" USING btree (
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issue-repoid-index-go" ON "augur_data"."issue_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issue_message_ref_repo_id_issue_id_idx" ON "augur_data"."issue_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table issue_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."issue_message_ref" ADD CONSTRAINT "issue-message-ref-insert-unique" UNIQUE ("issue_msg_ref_src_comment_id", "issue_id");

-- ----------------------------
-- Primary Key structure for table issue_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."issue_message_ref" ADD CONSTRAINT "issue_message_ref_pkey" PRIMARY KEY ("issue_msg_ref_id");

-- ----------------------------
-- Indexes structure for table issues
-- ----------------------------
CREATE INDEX "idx_issues_repo" ON "augur_data"."issues" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "idx_issues_repo_nonpr" ON "augur_data"."issues" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST
) WHERE pull_request IS NULL;
CREATE INDEX "issue-cntrb-dix2" ON "augur_data"."issues" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);
CREATE INDEX "issues_ibfk_1" ON "augur_data"."issues" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issues_ibfk_2" ON "augur_data"."issues" USING btree (
  "reporter_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);
CREATE INDEX "issues_ibfk_4" ON "augur_data"."issues" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "issues_repo_created_idx" ON "augur_data"."issues" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "created_at" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "issues_repo_id_issue_id_created_at_idx" ON "augur_data"."issues" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "issue_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "created_at" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table issues
-- ----------------------------
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "issue-insert-unique" UNIQUE ("issue_url");
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "unique-issue" UNIQUE ("repo_id", "gh_issue_id");

-- ----------------------------
-- Primary Key structure for table issues
-- ----------------------------
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "issues_pkey" PRIMARY KEY ("issue_id");

-- ----------------------------
-- Primary Key structure for table libraries
-- ----------------------------
ALTER TABLE "augur_data"."libraries" ADD CONSTRAINT "libraries_pkey" PRIMARY KEY ("library_id");

-- ----------------------------
-- Indexes structure for table library_dependencies
-- ----------------------------
CREATE INDEX "REPO_DEP" ON "augur_data"."library_dependencies" USING btree (
  "library_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table library_dependencies
-- ----------------------------
ALTER TABLE "augur_data"."library_dependencies" ADD CONSTRAINT "library_dependencies_pkey" PRIMARY KEY ("lib_dependency_id");

-- ----------------------------
-- Primary Key structure for table library_version
-- ----------------------------
ALTER TABLE "augur_data"."library_version" ADD CONSTRAINT "library_version_pkey" PRIMARY KEY ("library_version_id");

-- ----------------------------
-- Primary Key structure for table lstm_anomaly_models
-- ----------------------------
ALTER TABLE "augur_data"."lstm_anomaly_models" ADD CONSTRAINT "lstm_anomaly_models_pkey" PRIMARY KEY ("model_id");

-- ----------------------------
-- Primary Key structure for table lstm_anomaly_results
-- ----------------------------
ALTER TABLE "augur_data"."lstm_anomaly_results" ADD CONSTRAINT "lstm_anomaly_results_pkey" PRIMARY KEY ("result_id");

-- ----------------------------
-- Indexes structure for table message
-- ----------------------------
CREATE INDEX "idx_message_msg_cntrbxx" ON "augur_data"."message" USING btree (
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST,
  "msg_timestamp" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "idx_message_timestamp" ON "augur_data"."message" USING btree (
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_timestamp" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "message_date_index_messages" ON "augur_data"."message" USING btree (
  "msg_timestamp" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "message_repo_id_msg_id_idx" ON "augur_data"."message" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "message_repo_id_msg_timestamp_idx" ON "augur_data"."message" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_timestamp" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE UNIQUE INDEX "messagegrouper" ON "augur_data"."message" USING btree (
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "rgls_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "msg-cntrb-id-idx" ON "augur_data"."message" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);
CREATE INDEX "platformgrouper" ON "augur_data"."message" USING btree (
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "pltfrm_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "repo_message_reopid" ON "augur_data"."message" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table message
-- ----------------------------
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "message-insert-unique" UNIQUE ("platform_msg_id", "pltfrm_id");

-- ----------------------------
-- Primary Key structure for table message
-- ----------------------------
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "message_pkey" PRIMARY KEY ("msg_id");

-- ----------------------------
-- Primary Key structure for table message_analysis
-- ----------------------------
ALTER TABLE "augur_data"."message_analysis" ADD CONSTRAINT "message_analysis_pkey" PRIMARY KEY ("msg_analysis_id");

-- ----------------------------
-- Primary Key structure for table message_analysis_summary
-- ----------------------------
ALTER TABLE "augur_data"."message_analysis_summary" ADD CONSTRAINT "message_analysis_summary_pkey" PRIMARY KEY ("msg_summary_id");

-- ----------------------------
-- Primary Key structure for table message_sentiment
-- ----------------------------
ALTER TABLE "augur_data"."message_sentiment" ADD CONSTRAINT "message_sentiment_pkey" PRIMARY KEY ("msg_analysis_id");

-- ----------------------------
-- Primary Key structure for table message_sentiment_summary
-- ----------------------------
ALTER TABLE "augur_data"."message_sentiment_summary" ADD CONSTRAINT "message_sentiment_summary_pkey" PRIMARY KEY ("msg_summary_id");

-- ----------------------------
-- Indexes structure for table platform
-- ----------------------------
CREATE UNIQUE INDEX "plat" ON "augur_data"."platform" USING btree (
  "pltfrm_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table platform
-- ----------------------------
ALTER TABLE "augur_data"."platform" ADD CONSTRAINT "theplat" PRIMARY KEY ("pltfrm_id");

-- ----------------------------
-- Indexes structure for table pull_request_analysis
-- ----------------------------
CREATE INDEX "pr_anal_idx" ON "augur_data"."pull_request_analysis" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "probability_idx" ON "augur_data"."pull_request_analysis" USING btree (
  "merge_probability" "pg_catalog"."numeric_ops" DESC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table pull_request_analysis
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_analysis" ADD CONSTRAINT "pull_request_analysis_pkey" PRIMARY KEY ("pull_request_analysis_id");

-- ----------------------------
-- Indexes structure for table pull_request_assignees
-- ----------------------------
CREATE INDEX "pr_meta_cntrb-idx" ON "augur_data"."pull_request_assignees" USING btree (
  "contrib_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_assignees
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_assignees" ADD CONSTRAINT "assignees-unique" UNIQUE ("pull_request_id", "pr_assignee_src_id");

-- ----------------------------
-- Primary Key structure for table pull_request_assignees
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_assignees" ADD CONSTRAINT "pull_request_assignees_pkey" PRIMARY KEY ("pr_assignee_map_id");

-- ----------------------------
-- Indexes structure for table pull_request_commits
-- ----------------------------
CREATE INDEX "idx_pr_commits_composite" ON "augur_data"."pull_request_commits" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "pr_cmt_sha" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "idx_pr_commits_filtered" ON "augur_data"."pull_request_commits" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "pr_cmt_sha" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_commits
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_commits" ADD CONSTRAINT "pr_commit_nk" UNIQUE ("pull_request_id", "repo_id", "pr_cmt_sha");

-- ----------------------------
-- Primary Key structure for table pull_request_commits
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_commits" ADD CONSTRAINT "pull_request_commits_pkey" PRIMARY KEY ("pr_cmt_id");

-- ----------------------------
-- Indexes structure for table pull_request_events
-- ----------------------------
CREATE INDEX "idx_pr_events_composite" ON "augur_data"."pull_request_events" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "action" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "idx_pr_events_pr_action" ON "augur_data"."pull_request_events" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "action" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "pr_events_ibfk_1" ON "augur_data"."pull_request_events" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "pr_events_ibfk_2" ON "augur_data"."pull_request_events" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_events
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "pr-unique-event" UNIQUE ("node_id");
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "pr_events_repo_id_event_src_id_unique" UNIQUE ("repo_id", "issue_event_src_id");
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "unique-pr-event-id" UNIQUE ("platform_id", "node_id");

-- ----------------------------
-- Primary Key structure for table pull_request_events
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "pr_events_pkey" PRIMARY KEY ("pr_event_id");

-- ----------------------------
-- Indexes structure for table pull_request_files
-- ----------------------------
CREATE INDEX "pr_id_pr_files" ON "augur_data"."pull_request_files" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_files
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_files" ADD CONSTRAINT "pr_files_repo_unq" UNIQUE ("repo_id", "pull_request_id", "pr_file_path");
ALTER TABLE "augur_data"."pull_request_files" ADD CONSTRAINT "prfiles_unique" UNIQUE ("pull_request_id", "repo_id", "pr_file_path");

-- ----------------------------
-- Primary Key structure for table pull_request_files
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_files" ADD CONSTRAINT "pull_request_files_pkey" PRIMARY KEY ("pr_file_id");

-- ----------------------------
-- Uniques structure for table pull_request_labels
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_labels" ADD CONSTRAINT "unique-pr-src-label-id" UNIQUE ("pr_src_id", "pull_request_id");

-- ----------------------------
-- Primary Key structure for table pull_request_labels
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_labels" ADD CONSTRAINT "pull_request_labels_pkey" PRIMARY KEY ("pr_label_id");

-- ----------------------------
-- Indexes structure for table pull_request_message_ref
-- ----------------------------
CREATE INDEX "idx_pr_message_ref_composite" ON "augur_data"."pull_request_message_ref" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "idx_pr_message_ref_pr_msgxx" ON "augur_data"."pull_request_message_ref" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "repo-pr-message--id" ON "augur_data"."pull_request_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_message_ref" ADD CONSTRAINT "pull-request-message-ref-insert-unique" UNIQUE ("pr_message_ref_src_comment_id", "pull_request_id");

-- ----------------------------
-- Primary Key structure for table pull_request_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_message_ref" ADD CONSTRAINT "pull_request_message_ref_pkey" PRIMARY KEY ("pr_msg_ref_id");

-- ----------------------------
-- Indexes structure for table pull_request_meta
-- ----------------------------
CREATE INDEX "pr_meta-cntrbid-idx" ON "augur_data"."pull_request_meta" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_meta
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_meta" ADD CONSTRAINT "pull-request-meta-insert-unique" UNIQUE ("pull_request_id", "pr_head_or_base", "pr_sha");

-- ----------------------------
-- Primary Key structure for table pull_request_meta
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_meta" ADD CONSTRAINT "pull_request_meta_pkey" PRIMARY KEY ("pr_repo_meta_id");

-- ----------------------------
-- Indexes structure for table pull_request_repo
-- ----------------------------
CREATE INDEX "pr-cntrb-idx-repo" ON "augur_data"."pull_request_repo" USING btree (
  "pr_cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table pull_request_repo
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_repo" ADD CONSTRAINT "pull_request_repo_pkey" PRIMARY KEY ("pr_repo_id");

-- ----------------------------
-- Indexes structure for table pull_request_review_message_ref
-- ----------------------------
CREATE INDEX "pull_request_review_message_ref_repo_id_idx" ON "augur_data"."pull_request_review_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "pull_request_review_message_ref_repo_id_msg_id_idx" ON "augur_data"."pull_request_review_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "msg_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "review_pr_repo_index" ON "augur_data"."pull_request_review_message_ref" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_review_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_review_message_ref" ADD CONSTRAINT "pull-request-review-message-ref-insert-unique" UNIQUE ("pr_review_msg_src_id");

-- ----------------------------
-- Primary Key structure for table pull_request_review_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_review_message_ref" ADD CONSTRAINT "pr_review_msg_ref_id" PRIMARY KEY ("pr_review_msg_ref_id");

-- ----------------------------
-- Indexes structure for table pull_request_reviewers
-- ----------------------------
CREATE INDEX "pr-reviewers-cntrb-idx1" ON "augur_data"."pull_request_reviewers" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_reviewers
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviewers" ADD CONSTRAINT "unique_pr_src_reviewer_key" UNIQUE ("pull_request_id", "pr_reviewer_src_id");

-- ----------------------------
-- Primary Key structure for table pull_request_reviewers
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviewers" ADD CONSTRAINT "pull_request_reviewers_pkey" PRIMARY KEY ("pr_reviewer_map_id");

-- ----------------------------
-- Indexes structure for table pull_request_reviews
-- ----------------------------
CREATE INDEX "pr_id_pr_reviews" ON "augur_data"."pull_request_reviews" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "pr_rev_cntrb" ON "augur_data"."pull_request_reviews" USING btree (
  "cntrb_id" "pg_catalog"."uuid_ops" ASC NULLS LAST
);
CREATE INDEX "pr_rev_repo" ON "augur_data"."pull_request_reviews" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_request_reviews
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "pr_review_unique" UNIQUE ("pr_review_src_id");

-- ----------------------------
-- Primary Key structure for table pull_request_reviews
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "pull_request_review_id" PRIMARY KEY ("pr_review_id");

-- ----------------------------
-- Primary Key structure for table pull_request_teams
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_teams" ADD CONSTRAINT "pull_request_teams_pkey" PRIMARY KEY ("pr_team_id");

-- ----------------------------
-- Indexes structure for table pull_requests
-- ----------------------------
CREATE INDEX "id_node" ON "augur_data"."pull_requests" USING btree (
  "pr_src_id" "pg_catalog"."int8_ops" DESC NULLS FIRST,
  "pr_src_node_id" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" DESC NULLS LAST
);
CREATE INDEX "pr_ID_prs_table" ON "augur_data"."pull_requests" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "pull_requests_id_idx" ON "augur_data"."pull_requests" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "pull_requests_idx_repo_id_data_datex" ON "augur_data"."pull_requests" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "data_collection_date" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "pull_requests_repo_date_idx" ON "augur_data"."pull_requests" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "data_collection_date" "pg_catalog"."timestamp_ops" DESC NULLS FIRST
);
CREATE INDEX "pull_requests_repo_id_idx" ON "augur_data"."pull_requests" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table pull_requests
-- ----------------------------
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "pull-request-insert-unique" UNIQUE ("pr_url");
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "unique-pr" UNIQUE ("repo_id", "pr_src_id");
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "unique-prx" UNIQUE ("repo_id", "pr_src_id");

-- ----------------------------
-- Primary Key structure for table pull_requests
-- ----------------------------
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "pull_requests_pkey" PRIMARY KEY ("pull_request_id");

-- ----------------------------
-- Primary Key structure for table releases
-- ----------------------------
ALTER TABLE "augur_data"."releases" ADD CONSTRAINT "releases_pkey" PRIMARY KEY ("release_id");

-- ----------------------------
-- Indexes structure for table repo
-- ----------------------------
CREATE INDEX "forked" ON "augur_data"."repo" USING btree (
  "forked_from" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "idx_repo_name" ON "augur_data"."repo" USING btree (
  "repo_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_git_lower_idx" ON "augur_data"."repo" USING btree (
  lower(repo_git::text) COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_idx_repo_id_repo_namex" ON "augur_data"."repo" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "repo_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repo_repo_id_idx" ON "augur_data"."repo" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "repogitindexrep" ON "augur_data"."repo" USING btree (
  "repo_git" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "reponameindex" ON "augur_data"."repo" USING hash (
  "repo_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops"
);
CREATE INDEX "reponameindexbtree" ON "augur_data"."repo" USING btree (
  "repo_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "rggrouponrepoindex" ON "augur_data"."repo" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE UNIQUE INDEX "therepo" ON "augur_data"."repo" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table repo
-- ----------------------------
ALTER TABLE "augur_data"."repo" ADD CONSTRAINT "repo_git-unique" UNIQUE ("repo_git");
ALTER TABLE "augur_data"."repo" ADD CONSTRAINT "repo_src_id_unique" UNIQUE ("repo_src_id");

-- ----------------------------
-- Primary Key structure for table repo
-- ----------------------------
ALTER TABLE "augur_data"."repo" ADD CONSTRAINT "repounique" PRIMARY KEY ("repo_id");

-- ----------------------------
-- Primary Key structure for table repo_badging
-- ----------------------------
ALTER TABLE "augur_data"."repo_badging" ADD CONSTRAINT "repo_badging_pkey" PRIMARY KEY ("badge_collection_id");

-- ----------------------------
-- Primary Key structure for table repo_clones_data
-- ----------------------------
ALTER TABLE "augur_data"."repo_clones_data" ADD CONSTRAINT "repo_clones_data_pkey" PRIMARY KEY ("repo_clone_data_id");

-- ----------------------------
-- Primary Key structure for table repo_cluster_messages
-- ----------------------------
ALTER TABLE "augur_data"."repo_cluster_messages" ADD CONSTRAINT "repo_cluster_messages_pkey" PRIMARY KEY ("msg_cluster_id");

-- ----------------------------
-- Uniques structure for table repo_dependencies
-- ----------------------------
ALTER TABLE "augur_data"."repo_dependencies" ADD CONSTRAINT "deps-insert-unique" UNIQUE ("repo_id", "dep_name", "data_collection_date");

-- ----------------------------
-- Primary Key structure for table repo_dependencies
-- ----------------------------
ALTER TABLE "augur_data"."repo_dependencies" ADD CONSTRAINT "repo_dependencies_pkey" PRIMARY KEY ("repo_dependencies_id");

-- ----------------------------
-- Uniques structure for table repo_deps_libyear
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_libyear" ADD CONSTRAINT "deps-libyear-insert-unique" UNIQUE ("repo_id", "name", "data_collection_date");

-- ----------------------------
-- Primary Key structure for table repo_deps_libyear
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_libyear" ADD CONSTRAINT "repo_deps_libyear_pkey" PRIMARY KEY ("repo_deps_libyear_id");

-- ----------------------------
-- Uniques structure for table repo_deps_scorecard
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_scorecard" ADD CONSTRAINT "deps_scorecard_new_unique" UNIQUE ("repo_id", "name", "data_collection_date");

-- ----------------------------
-- Primary Key structure for table repo_deps_scorecard
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_scorecard" ADD CONSTRAINT "repo_deps_scorecard_pkey1" PRIMARY KEY ("repo_deps_scorecard_id");

-- ----------------------------
-- Primary Key structure for table repo_group_insights
-- ----------------------------
ALTER TABLE "augur_data"."repo_group_insights" ADD CONSTRAINT "repo_group_insights_pkey" PRIMARY KEY ("rgi_id");

-- ----------------------------
-- Indexes structure for table repo_groups
-- ----------------------------
CREATE INDEX "idx_repo_groups_name" ON "augur_data"."repo_groups" USING btree (
  "rg_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE UNIQUE INDEX "rgidm" ON "augur_data"."repo_groups" USING btree (
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);
CREATE INDEX "rgnameindex" ON "augur_data"."repo_groups" USING btree (
  "rg_name" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table repo_groups
-- ----------------------------
ALTER TABLE "augur_data"."repo_groups" ADD CONSTRAINT "rgid" PRIMARY KEY ("repo_group_id");

-- ----------------------------
-- Indexes structure for table repo_groups_list_serve
-- ----------------------------
CREATE UNIQUE INDEX "lister" ON "augur_data"."repo_groups_list_serve" USING btree (
  "rgls_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "repo_group_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table repo_groups_list_serve
-- ----------------------------
ALTER TABLE "augur_data"."repo_groups_list_serve" ADD CONSTRAINT "rglistserve" UNIQUE ("rgls_id", "repo_group_id");

-- ----------------------------
-- Primary Key structure for table repo_groups_list_serve
-- ----------------------------
ALTER TABLE "augur_data"."repo_groups_list_serve" ADD CONSTRAINT "repo_groups_list_serve_pkey" PRIMARY KEY ("rgls_id");

-- ----------------------------
-- Indexes structure for table repo_info
-- ----------------------------
CREATE INDEX "repo_info_idx_repo_id_data_date_1x" ON "augur_data"."repo_info" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "data_collection_date" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);
CREATE INDEX "repo_info_idx_repo_id_data_datex" ON "augur_data"."repo_info" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "data_collection_date" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table repo_info
-- ----------------------------
ALTER TABLE "augur_data"."repo_info" ADD CONSTRAINT "repo_info_pkey" PRIMARY KEY ("repo_info_id");

-- ----------------------------
-- Primary Key structure for table repo_insights
-- ----------------------------
ALTER TABLE "augur_data"."repo_insights" ADD CONSTRAINT "repo_insights_pkey" PRIMARY KEY ("ri_id");

-- ----------------------------
-- Indexes structure for table repo_insights_records
-- ----------------------------
CREATE INDEX "dater" ON "augur_data"."repo_insights_records" USING btree (
  "ri_date" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table repo_insights_records
-- ----------------------------
ALTER TABLE "augur_data"."repo_insights_records" ADD CONSTRAINT "repo_insights_records_pkey" PRIMARY KEY ("ri_id");

-- ----------------------------
-- Indexes structure for table repo_labor
-- ----------------------------
CREATE INDEX "idx_repo_labor_repo_date" ON "augur_data"."repo_labor" USING btree (
  "repo_id" "pg_catalog"."int8_ops" ASC NULLS LAST,
  "data_collection_date" "pg_catalog"."timestamp_ops" ASC NULLS LAST
) TABLESPACE "speed";

-- ----------------------------
-- Uniques structure for table repo_labor
-- ----------------------------
ALTER TABLE "augur_data"."repo_labor" ADD CONSTRAINT "rl-unique" UNIQUE ("repo_id", "rl_analysis_date", "file_path", "file_name") USING INDEX TABLESPACE "speed";

-- ----------------------------
-- Primary Key structure for table repo_labor
-- ----------------------------
ALTER TABLE "augur_data"."repo_labor" ADD CONSTRAINT "repo_labor_pkey" PRIMARY KEY ("repo_labor_id") USING INDEX TABLESPACE "speed";

-- ----------------------------
-- Primary Key structure for table repo_meta
-- ----------------------------
ALTER TABLE "augur_data"."repo_meta" ADD CONSTRAINT "repo_meta_pkey" PRIMARY KEY ("rmeta_id", "repo_id");

-- ----------------------------
-- Primary Key structure for table repo_sbom_scans
-- ----------------------------
ALTER TABLE "augur_data"."repo_sbom_scans" ADD CONSTRAINT "repo_sbom_scans_pkey" PRIMARY KEY ("rsb_id");

-- ----------------------------
-- Primary Key structure for table repo_stats
-- ----------------------------
ALTER TABLE "augur_data"."repo_stats" ADD CONSTRAINT "repo_stats_pkey" PRIMARY KEY ("rstat_id", "repo_id");

-- ----------------------------
-- Primary Key structure for table repo_test_coverage
-- ----------------------------
ALTER TABLE "augur_data"."repo_test_coverage" ADD CONSTRAINT "repo_test_coverage_pkey" PRIMARY KEY ("repo_id");

-- ----------------------------
-- Primary Key structure for table repo_topic
-- ----------------------------
ALTER TABLE "augur_data"."repo_topic" ADD CONSTRAINT "repo_topic_pkey" PRIMARY KEY ("repo_topic_id");

-- ----------------------------
-- Indexes structure for table repos_fetch_log
-- ----------------------------
CREATE INDEX "repos_id,status" ON "augur_data"."repos_fetch_log" USING btree (
  "repos_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "status" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "repos_id,statusops" ON "augur_data"."repos_fetch_log" USING btree (
  "repos_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "status" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table settings
-- ----------------------------
ALTER TABLE "augur_data"."settings" ADD CONSTRAINT "settings_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Indexes structure for table topic_model_event
-- ----------------------------
CREATE INDEX "ix_tme_event" ON "augur_data"."topic_model_event" USING btree (
  "event" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);
CREATE INDEX "ix_tme_repo_ts" ON "augur_data"."topic_model_event" USING btree (
  "repo_id" "pg_catalog"."int4_ops" ASC NULLS LAST,
  "ts" "pg_catalog"."timestamptz_ops" ASC NULLS LAST
);

-- ----------------------------
-- Primary Key structure for table topic_model_event
-- ----------------------------
ALTER TABLE "augur_data"."topic_model_event" ADD CONSTRAINT "topic_model_event_pkey" PRIMARY KEY ("event_id");

-- ----------------------------
-- Primary Key structure for table topic_model_meta
-- ----------------------------
ALTER TABLE "augur_data"."topic_model_meta" ADD CONSTRAINT "topic_model_meta_pkey" PRIMARY KEY ("model_id");

-- ----------------------------
-- Primary Key structure for table topic_words
-- ----------------------------
ALTER TABLE "augur_data"."topic_words" ADD CONSTRAINT "topic_words_pkey" PRIMARY KEY ("topic_words_id");

-- ----------------------------
-- Indexes structure for table unknown_cache
-- ----------------------------
CREATE INDEX "type,projects_id" ON "augur_data"."unknown_cache" USING btree (
  "type" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "repo_group_id" "pg_catalog"."int4_ops" ASC NULLS LAST
);

-- ----------------------------
-- Uniques structure for table unresolved_commit_emails
-- ----------------------------
ALTER TABLE "augur_data"."unresolved_commit_emails" ADD CONSTRAINT "unresolved_commit_emails_email_key" UNIQUE ("email");

-- ----------------------------
-- Primary Key structure for table unresolved_commit_emails
-- ----------------------------
ALTER TABLE "augur_data"."unresolved_commit_emails" ADD CONSTRAINT "unresolved_commit_emails_pkey" PRIMARY KEY ("email_unresolved_id");

-- ----------------------------
-- Primary Key structure for table utility_log
-- ----------------------------
ALTER TABLE "augur_data"."utility_log" ADD CONSTRAINT "utility_log_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Foreign Keys structure for table akl;fjlk;a
-- ----------------------------
ALTER TABLE "augur_data"."akl;fjlk;a" ADD CONSTRAINT "user_repo_user_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table commit_comment_ref
-- ----------------------------
ALTER TABLE "augur_data"."commit_comment_ref" ADD CONSTRAINT "fk_commit_comment_ref_commits_1" FOREIGN KEY ("cmt_id") REFERENCES "augur_data"."commits" ("cmt_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."commit_comment_ref" ADD CONSTRAINT "fk_commit_comment_ref_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table commit_messages
-- ----------------------------
ALTER TABLE "augur_data"."commit_messages" ADD CONSTRAINT "commit_messages_repo_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table commit_parents
-- ----------------------------
ALTER TABLE "augur_data"."commit_parents" ADD CONSTRAINT "fk_commit_parents_commits_1" FOREIGN KEY ("cmt_id") REFERENCES "augur_data"."commits" ("cmt_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_data"."commit_parents" ADD CONSTRAINT "fk_commit_parents_commits_2" FOREIGN KEY ("parent_id") REFERENCES "augur_data"."commits" ("cmt_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table commits
-- ----------------------------
ALTER TABLE "augur_data"."commits" ADD CONSTRAINT "cmt_ght_author_cntrb_id_fk" FOREIGN KEY ("cmt_ght_author_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_data"."commits" ADD CONSTRAINT "fk_commits_repo_2" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table contributor_repo
-- ----------------------------
ALTER TABLE "augur_data"."contributor_repo" ADD CONSTRAINT "contributor_repo_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table contributors_aliases
-- ----------------------------
ALTER TABLE "augur_data"."contributors_aliases" ADD CONSTRAINT "contributors_aliases_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table discourse_insights
-- ----------------------------
ALTER TABLE "augur_data"."discourse_insights" ADD CONSTRAINT "fk_discourse_insights_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table historical_repo_urls
-- ----------------------------
ALTER TABLE "augur_data"."historical_repo_urls" ADD CONSTRAINT "historical_repo_urls_repo_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table issue_assignees
-- ----------------------------
ALTER TABLE "augur_data"."issue_assignees" ADD CONSTRAINT "fk_issue_assignee_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_assignees" ADD CONSTRAINT "fk_issue_assignees_issues_1" FOREIGN KEY ("issue_id") REFERENCES "augur_data"."issues" ("issue_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_assignees" ADD CONSTRAINT "issue_assignees_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table issue_events
-- ----------------------------
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "fk_issue_event_platform_ide" FOREIGN KEY ("platform_id") REFERENCES "augur_data"."platform" ("pltfrm_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "fk_issue_events_issues_1" FOREIGN KEY ("issue_id") REFERENCES "augur_data"."issues" ("issue_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "fk_issue_events_repo" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_events" ADD CONSTRAINT "issue_events_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table issue_labels
-- ----------------------------
ALTER TABLE "augur_data"."issue_labels" ADD CONSTRAINT "fk_issue_labels_issues_1" FOREIGN KEY ("issue_id") REFERENCES "augur_data"."issues" ("issue_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issue_labels" ADD CONSTRAINT "fk_issue_labels_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table issue_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."issue_message_ref" ADD CONSTRAINT "fk_issue_message_ref_issues_1" FOREIGN KEY ("issue_id") REFERENCES "augur_data"."issues" ("issue_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."issue_message_ref" ADD CONSTRAINT "fk_issue_message_ref_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."issue_message_ref" ADD CONSTRAINT "fk_repo_id_fk1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Foreign Keys structure for table issues
-- ----------------------------
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "fk_issues_repo" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "issues_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_data"."issues" ADD CONSTRAINT "issues_reporter_id_fkey" FOREIGN KEY ("reporter_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table libraries
-- ----------------------------
ALTER TABLE "augur_data"."libraries" ADD CONSTRAINT "fk_libraries_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table library_dependencies
-- ----------------------------
ALTER TABLE "augur_data"."library_dependencies" ADD CONSTRAINT "fk_library_dependencies_libraries_1" FOREIGN KEY ("library_id") REFERENCES "augur_data"."libraries" ("library_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table library_version
-- ----------------------------
ALTER TABLE "augur_data"."library_version" ADD CONSTRAINT "fk_library_version_libraries_1" FOREIGN KEY ("library_id") REFERENCES "augur_data"."libraries" ("library_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table lstm_anomaly_results
-- ----------------------------
ALTER TABLE "augur_data"."lstm_anomaly_results" ADD CONSTRAINT "fk_lstm_anomaly_results_lstm_anomaly_models_1" FOREIGN KEY ("model_id") REFERENCES "augur_data"."lstm_anomaly_models" ("model_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
ALTER TABLE "augur_data"."lstm_anomaly_results" ADD CONSTRAINT "fk_lstm_anomaly_results_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table message
-- ----------------------------
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "fk_message_platform_1" FOREIGN KEY ("pltfrm_id") REFERENCES "augur_data"."platform" ("pltfrm_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "fk_message_repo_groups_list_serve_1" FOREIGN KEY ("rgls_id") REFERENCES "augur_data"."repo_groups_list_serve" ("rgls_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "fk_message_repoid" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."message" ADD CONSTRAINT "message_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table message_analysis
-- ----------------------------
ALTER TABLE "augur_data"."message_analysis" ADD CONSTRAINT "fk_message_analysis_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table message_analysis_summary
-- ----------------------------
ALTER TABLE "augur_data"."message_analysis_summary" ADD CONSTRAINT "fk_message_analysis_summary_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table message_sentiment
-- ----------------------------
ALTER TABLE "augur_data"."message_sentiment" ADD CONSTRAINT "fk_message_sentiment_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table message_sentiment_summary
-- ----------------------------
ALTER TABLE "augur_data"."message_sentiment_summary" ADD CONSTRAINT "fk_message_sentiment_summary_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_analysis
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_analysis" ADD CONSTRAINT "fk_pull_request_analysis_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table pull_request_assignees
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_assignees" ADD CONSTRAINT "fk_pull_request_assignees_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_assignees" ADD CONSTRAINT "fk_pull_request_assignees_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_assignees" ADD CONSTRAINT "pull_request_assignees_contrib_id_fkey" FOREIGN KEY ("contrib_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_commits
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_commits" ADD CONSTRAINT "fk_pull_request_commits_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_commits" ADD CONSTRAINT "fk_pull_request_commits_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_commits" ADD CONSTRAINT "pull_request_commits_pr_cmt_author_cntrb_id_fkey" FOREIGN KEY ("pr_cmt_author_cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_events
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "fk_pull_request_events_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "fkpr_platform" FOREIGN KEY ("platform_id") REFERENCES "augur_data"."platform" ("pltfrm_id") ON DELETE RESTRICT ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "fkprevent_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_events" ADD CONSTRAINT "pull_request_events_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_files
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_files" ADD CONSTRAINT "fk_pull_request_commits_pull_requests_1_copy_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_files" ADD CONSTRAINT "fk_pull_request_files_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Foreign Keys structure for table pull_request_labels
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_labels" ADD CONSTRAINT "fk_pull_request_labels_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_labels" ADD CONSTRAINT "fk_pull_request_labels_repo" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table pull_request_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_message_ref" ADD CONSTRAINT "fk_pr_repo" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_message_ref" ADD CONSTRAINT "fk_pull_request_message_ref_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_message_ref" ADD CONSTRAINT "fk_pull_request_message_ref_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Foreign Keys structure for table pull_request_meta
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_meta" ADD CONSTRAINT "fk_pull_request_meta_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_meta" ADD CONSTRAINT "fk_pull_request_repo_meta_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_meta" ADD CONSTRAINT "pull_request_meta_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_repo
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_repo" ADD CONSTRAINT "fk_pull_request_repo_pull_request_meta_1" FOREIGN KEY ("pr_repo_meta_id") REFERENCES "augur_data"."pull_request_meta" ("pr_repo_meta_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_repo" ADD CONSTRAINT "pull_request_repo_pr_cntrb_id_fkey" FOREIGN KEY ("pr_cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_review_message_ref
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_review_message_ref" ADD CONSTRAINT "fk_pull_request_review_message_ref_message_1" FOREIGN KEY ("msg_id") REFERENCES "augur_data"."message" ("msg_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_review_message_ref" ADD CONSTRAINT "fk_pull_request_review_message_ref_pull_request_reviews_1" FOREIGN KEY ("pr_review_id") REFERENCES "augur_data"."pull_request_reviews" ("pr_review_id") ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_review_message_ref" ADD CONSTRAINT "fk_review_repo" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Foreign Keys structure for table pull_request_reviewers
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviewers" ADD CONSTRAINT "fk_pull_request_reviewers_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_reviewers" ADD CONSTRAINT "pull_request_reviewers_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_reviews
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "fk-review-platform" FOREIGN KEY ("platform_id") REFERENCES "augur_data"."platform" ("pltfrm_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "fk_pull_request_reviews_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "fk_repo_review" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_request_reviews" ADD CONSTRAINT "pull_request_reviews_cntrb_id_fkey" FOREIGN KEY ("cntrb_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table pull_request_teams
-- ----------------------------
ALTER TABLE "augur_data"."pull_request_teams" ADD CONSTRAINT "fk_pull_request_teams_pull_requests_1" FOREIGN KEY ("pull_request_id") REFERENCES "augur_data"."pull_requests" ("pull_request_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table pull_requests
-- ----------------------------
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "fk_pull_requests_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "augur_data"."pull_requests" ADD CONSTRAINT "pull_requests_pr_augur_contributor_id_fkey" FOREIGN KEY ("pr_augur_contributor_id") REFERENCES "augur_data"."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table releases
-- ----------------------------
ALTER TABLE "augur_data"."releases" ADD CONSTRAINT "fk_releases_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo
-- ----------------------------
ALTER TABLE "augur_data"."repo" ADD CONSTRAINT "fk_repo_repo_groups_1" FOREIGN KEY ("repo_group_id") REFERENCES "augur_data"."repo_groups" ("repo_group_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
COMMENT ON CONSTRAINT "fk_repo_repo_groups_1" ON "augur_data"."repo" IS 'Repo_groups cardinality set to one and only one because, although in theory there could be more than one repo group for a repo, this might create dependencies in hosted situation that we do not want to live with. ';

-- ----------------------------
-- Foreign Keys structure for table repo_badging
-- ----------------------------
ALTER TABLE "augur_data"."repo_badging" ADD CONSTRAINT "fk_repo_badging_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_clones_data
-- ----------------------------
ALTER TABLE "augur_data"."repo_clones_data" ADD CONSTRAINT "repo_clones_data_repo_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- ----------------------------
-- Foreign Keys structure for table repo_cluster_messages
-- ----------------------------
ALTER TABLE "augur_data"."repo_cluster_messages" ADD CONSTRAINT "fk_repo_cluster_messages_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_dependencies
-- ----------------------------
ALTER TABLE "augur_data"."repo_dependencies" ADD CONSTRAINT "repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_deps_libyear
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_libyear" ADD CONSTRAINT "repo_id_copy_2" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_deps_scorecard
-- ----------------------------
ALTER TABLE "augur_data"."repo_deps_scorecard" ADD CONSTRAINT "repo_id_copy_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_group_insights
-- ----------------------------
ALTER TABLE "augur_data"."repo_group_insights" ADD CONSTRAINT "fk_repo_group_insights_repo_groups_1" FOREIGN KEY ("repo_group_id") REFERENCES "augur_data"."repo_groups" ("repo_group_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_groups_list_serve
-- ----------------------------
ALTER TABLE "augur_data"."repo_groups_list_serve" ADD CONSTRAINT "fk_repo_groups_list_serve_repo_groups_1" FOREIGN KEY ("repo_group_id") REFERENCES "augur_data"."repo_groups" ("repo_group_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_info
-- ----------------------------
ALTER TABLE "augur_data"."repo_info" ADD CONSTRAINT "fk_repo_info_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_insights
-- ----------------------------
ALTER TABLE "augur_data"."repo_insights" ADD CONSTRAINT "fk_repo_insights_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_insights_records
-- ----------------------------
ALTER TABLE "augur_data"."repo_insights_records" ADD CONSTRAINT "repo_id_ref" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE SET NULL ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table repo_labor
-- ----------------------------
ALTER TABLE "augur_data"."repo_labor" ADD CONSTRAINT "fk_repo_labor_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_meta
-- ----------------------------
ALTER TABLE "augur_data"."repo_meta" ADD CONSTRAINT "fk_repo_meta_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_sbom_scans
-- ----------------------------
ALTER TABLE "augur_data"."repo_sbom_scans" ADD CONSTRAINT "repo_linker_sbom" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- ----------------------------
-- Foreign Keys structure for table repo_stats
-- ----------------------------
ALTER TABLE "augur_data"."repo_stats" ADD CONSTRAINT "fk_repo_stats_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_test_coverage
-- ----------------------------
ALTER TABLE "augur_data"."repo_test_coverage" ADD CONSTRAINT "fk_repo_test_coverage_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table repo_topic
-- ----------------------------
ALTER TABLE "augur_data"."repo_topic" ADD CONSTRAINT "fk_repo_topic_repo_1" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table topic_model_event
-- ----------------------------
ALTER TABLE "augur_data"."topic_model_event" ADD CONSTRAINT "fk_tme_model_id" FOREIGN KEY ("model_id") REFERENCES "augur_data"."topic_model_meta" ("model_id") ON DELETE SET NULL ON UPDATE NO ACTION;
ALTER TABLE "augur_data"."topic_model_event" ADD CONSTRAINT "fk_tme_repo_id" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- ----------------------------
-- Foreign Keys structure for table topic_model_meta
-- ----------------------------
ALTER TABLE "augur_data"."topic_model_meta" ADD CONSTRAINT "topic_model_meta_repo_id_fkey" FOREIGN KEY ("repo_id") REFERENCES "augur_data"."repo" ("repo_id") ON DELETE NO ACTION ON UPDATE NO ACTION;
