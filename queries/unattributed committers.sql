SELECT count(*) FROM aveloxis_data.commits AS c where c.cmt_ght_author_id is NULL; --result: 158,874,930

SELECT count(*) FROM aveloxis_data.commits AS c where c.cmt_author_platform_username is NULL; --result: 335,713,317

SELECT count(*) FROM aveloxis_data.commits AS c where c.cmt_author_platform_username is NULL and c.cmt_ght_author_id is NULL; --result: 101,993,377