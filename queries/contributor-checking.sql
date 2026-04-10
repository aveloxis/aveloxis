select * from contributors where cntrb_email is null and cntrb_canonical is not null order by data_collection_date desc; 

select count(*) from contributors where cntrb_canonical is null; 

select count(*) from contributors where cntrb_email is null; 

select count(*) from commits where cmt_committer_email is null 
union 
select count(*) from commits where cmt_committer_email is NOT null; 
