SELECT c.cmt_ght_author_id,
       c.cmt_author_platform_username,
       c1.cntrb_id AS id_via_uuid,
       c2.cntrb_id AS id_via_login,
       count(*)    AS commit_row_count
FROM aveloxis_data.commits c
JOIN aveloxis_data.contributors c1 ON c1.cntrb_id = c.cmt_ght_author_id
JOIN aveloxis_data.contributors c2 ON c2.cntrb_login = c.cmt_author_platform_username
WHERE c.cmt_ght_author_id IS NOT NULL
  AND c.cmt_author_platform_username IS NOT NULL
  AND c1.cntrb_id != c2.cntrb_id
  AND c1.cntrb_login = c.cmt_author_platform_username
GROUP BY c.cmt_ght_author_id,
         c.cmt_author_platform_username,
         c1.cntrb_id,
         c2.cntrb_id
ORDER BY commit_row_count DESC;