# aveloxis data-test report

- **Released tag**: 0.22.6 (in `aveloxis_released`)
- **Local version**: working tree (in `aveloxis_new`)
- **Test repo**: https://github.com/augurlabs/augur
- **Schemas**: aveloxis_data, aveloxis_ops
- **Generated**: 2026-05-18T06:00:47Z

## Summary

- **FAIL** (released has more rows = data loss): 0
- **FLAG** (new has more rows = likely new coverage): 0
- **PASS** (equal counts): 116

## PASS — equal row counts

| Table | released | new | delta |
|---|---:|---:|---:|
| `aveloxis_data.chaoss_metric_status` | 0 | 0 | +0 |
| `aveloxis_data.chaoss_user` | 0 | 0 | +0 |
| `aveloxis_data.commit_comment_ref` | 0 | 0 | +0 |
| `aveloxis_data.commit_messages` | 12744 | 12744 | +0 |
| `aveloxis_data.commit_parents` | 3 | 3 | +0 |
| `aveloxis_data.commits` | 42401 | 42401 | +0 |
| `aveloxis_data.contributor_affiliations` | 0 | 0 | +0 |
| `aveloxis_data.contributor_identities` | 678 | 678 | +0 |
| `aveloxis_data.contributor_repo` | 0 | 0 | +0 |
| `aveloxis_data.contributors` | 679 | 679 | +0 |
| `aveloxis_data.contributors_aliases` | 141 | 141 | +0 |
| `aveloxis_data.contributors_old` | 0 | 0 | +0 |
| `aveloxis_data.dei_badging` | 0 | 0 | +0 |
| `aveloxis_data.discourse_insights` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_annual` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_group_annual` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_group_monthly` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_group_weekly` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_monthly` | 0 | 0 | +0 |
| `aveloxis_data.dm_repo_weekly` | 0 | 0 | +0 |
| `aveloxis_data.exclude` | 0 | 0 | +0 |
| `aveloxis_data.historical_repo_urls` | 0 | 0 | +0 |
| `aveloxis_data.issue_assignees` | 665 | 665 | +0 |
| `aveloxis_data.issue_events` | 0 | 0 | +0 |
| `aveloxis_data.issue_labels` | 1362 | 1362 | +0 |
| `aveloxis_data.issue_message_ref` | 0 | 0 | +0 |
| `aveloxis_data.issues` | 989 | 989 | +0 |
| `aveloxis_data.libraries` | 0 | 0 | +0 |
| `aveloxis_data.library_dependencies` | 0 | 0 | +0 |
| `aveloxis_data.library_version` | 0 | 0 | +0 |
| `aveloxis_data.lstm_anomaly_models` | 0 | 0 | +0 |
| `aveloxis_data.lstm_anomaly_results` | 0 | 0 | +0 |
| `aveloxis_data.message_analysis` | 0 | 0 | +0 |
| `aveloxis_data.message_analysis_summary` | 0 | 0 | +0 |
| `aveloxis_data.message_sentiment` | 0 | 0 | +0 |
| `aveloxis_data.message_sentiment_summary` | 0 | 0 | +0 |
| `aveloxis_data.messages` | 4877 | 4877 | +0 |
| `aveloxis_data.network_beyond_augur` | 0 | 0 | +0 |
| `aveloxis_data.network_beyond_augur_dependencies` | 0 | 0 | +0 |
| `aveloxis_data.platforms` | 3 | 3 | +0 |
| `aveloxis_data.pull_request_analysis` | 0 | 0 | +0 |
| `aveloxis_data.pull_request_assignees` | 327 | 327 | +0 |
| `aveloxis_data.pull_request_commits` | 42571 | 42571 | +0 |
| `aveloxis_data.pull_request_events` | 0 | 0 | +0 |
| `aveloxis_data.pull_request_files` | 62540 | 62540 | +0 |
| `aveloxis_data.pull_request_labels` | 1434 | 1434 | +0 |
| `aveloxis_data.pull_request_message_ref` | 0 | 0 | +0 |
| `aveloxis_data.pull_request_meta` | 5256 | 5256 | +0 |
| `aveloxis_data.pull_request_repo` | 5134 | 5134 | +0 |
| `aveloxis_data.pull_request_review_message_ref` | 1058 | 1058 | +0 |
| `aveloxis_data.pull_request_reviewers` | 1784 | 1784 | +0 |
| `aveloxis_data.pull_request_reviews` | 2964 | 2964 | +0 |
| `aveloxis_data.pull_request_teams` | 0 | 0 | +0 |
| `aveloxis_data.pull_requests` | 2628 | 2628 | +0 |
| `aveloxis_data.releases` | 186 | 186 | +0 |
| `aveloxis_data.repo_badging` | 0 | 0 | +0 |
| `aveloxis_data.repo_clones` | 0 | 0 | +0 |
| `aveloxis_data.repo_cluster_messages` | 0 | 0 | +0 |
| `aveloxis_data.repo_dependencies` | 0 | 0 | +0 |
| `aveloxis_data.repo_deps_libyear` | 0 | 0 | +0 |
| `aveloxis_data.repo_deps_libyear_history` | 0 | 0 | +0 |
| `aveloxis_data.repo_deps_scorecard` | 0 | 0 | +0 |
| `aveloxis_data.repo_deps_scorecard_history` | 0 | 0 | +0 |
| `aveloxis_data.repo_deps_vulnerabilities` | 0 | 0 | +0 |
| `aveloxis_data.repo_group_insights` | 0 | 0 | +0 |
| `aveloxis_data.repo_groups` | 2 | 2 | +0 |
| `aveloxis_data.repo_groups_list_serve` | 0 | 0 | +0 |
| `aveloxis_data.repo_info` | 1 | 1 | +0 |
| `aveloxis_data.repo_info_history` | 0 | 0 | +0 |
| `aveloxis_data.repo_insights` | 0 | 0 | +0 |
| `aveloxis_data.repo_insights_records` | 0 | 0 | +0 |
| `aveloxis_data.repo_labor` | 0 | 0 | +0 |
| `aveloxis_data.repo_meta` | 0 | 0 | +0 |
| `aveloxis_data.repo_sbom_scans` | 0 | 0 | +0 |
| `aveloxis_data.repo_stats` | 0 | 0 | +0 |
| `aveloxis_data.repo_test_coverage` | 0 | 0 | +0 |
| `aveloxis_data.repo_topic` | 0 | 0 | +0 |
| `aveloxis_data.repos` | 1 | 1 | +0 |
| `aveloxis_data.repos_fetch_log` | 0 | 0 | +0 |
| `aveloxis_data.review_comments` | 3819 | 3819 | +0 |
| `aveloxis_data.settings` | 0 | 0 | +0 |
| `aveloxis_data.topic_model_event` | 0 | 0 | +0 |
| `aveloxis_data.topic_model_meta` | 0 | 0 | +0 |
| `aveloxis_data.topic_words` | 0 | 0 | +0 |
| `aveloxis_data.unknown_cache` | 0 | 0 | +0 |
| `aveloxis_data.unresolved_commit_emails` | 29 | 29 | +0 |
| `aveloxis_data.utility_log` | 0 | 0 | +0 |
| `aveloxis_data.working_commits` | 0 | 0 | +0 |
| `aveloxis_ops.augur_settings` | 0 | 0 | +0 |
| `aveloxis_ops.client_applications` | 0 | 0 | +0 |
| `aveloxis_ops.collection_queue` | 1 | 1 | +0 |
| `aveloxis_ops.collection_status` | 1 | 1 | +0 |
| `aveloxis_ops.config` | 0 | 0 | +0 |
| `aveloxis_ops.email_confirmations` | 0 | 0 | +0 |
| `aveloxis_ops.foundation_membership` | 0 | 0 | +0 |
| `aveloxis_ops.github_users` | 0 | 0 | +0 |
| `aveloxis_ops.network_weighted_commits` | 0 | 0 | +0 |
| `aveloxis_ops.network_weighted_issues` | 0 | 0 | +0 |
| `aveloxis_ops.network_weighted_pr_reviews` | 0 | 0 | +0 |
| `aveloxis_ops.network_weighted_prs` | 0 | 0 | +0 |
| `aveloxis_ops.refresh_tokens` | 0 | 0 | +0 |
| `aveloxis_ops.repos_fetch_log` | 0 | 0 | +0 |
| `aveloxis_ops.schema_meta` | 1 | 1 | +0 |
| `aveloxis_ops.staging` | 0 | 0 | +0 |
| `aveloxis_ops.subscription_types` | 0 | 0 | +0 |
| `aveloxis_ops.subscriptions` | 0 | 0 | +0 |
| `aveloxis_ops.user_groups` | 0 | 0 | +0 |
| `aveloxis_ops.user_org_requests` | 0 | 0 | +0 |
| `aveloxis_ops.user_repos` | 0 | 0 | +0 |
| `aveloxis_ops.user_session_tokens` | 0 | 0 | +0 |
| `aveloxis_ops.users` | 0 | 0 | +0 |
| `aveloxis_ops.worker_history` | 0 | 0 | +0 |
| `aveloxis_ops.worker_job` | 0 | 0 | +0 |
| `aveloxis_ops.worker_oauth` | 55 | 55 | +0 |
| `aveloxis_ops.worker_settings_facade` | 0 | 0 | +0 |
| `aveloxis_ops.working_commits` | 0 | 0 | +0 |

