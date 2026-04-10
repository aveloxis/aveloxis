
CREATE INDEX "pr_ID_prs_table" ON aveloxis_data."pull_requests" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

CREATE INDEX "pr_id_pr_files" ON aveloxis_data."pull_request_files" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

CREATE INDEX "pr_id_pr_reviews" ON aveloxis_data."pull_request_reviews" USING btree (
  "pull_request_id" "pg_catalog"."int8_ops" ASC NULLS LAST
);

ALTER TABLE aveloxis_data."commits" DROP CONSTRAINT "cmt_ght_author_cntrb_id_fk";

ALTER TABLE aveloxis_data."commits" ADD CONSTRAINT "cmt_ght_author_cntrb_id_fk" FOREIGN KEY ("cmt_ght_author_id") REFERENCES aveloxis_data."contributors" ("cntrb_id") ON DELETE NO ACTION ON UPDATE NO ACTION ;
