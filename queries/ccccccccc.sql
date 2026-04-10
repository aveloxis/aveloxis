select contributors.cntrb_id, cntrb_email,  substring(alias_email from '@(.*)$') as domain, alias_email from contributors, contributors_aliases 
where contributors.cntrb_id=contributors_aliases.cntrb_id 
order by contributors.cntrb_id; 

select  substring(alias_email from '@(.*)$') as domain, count(*) as counter from contributors, contributors_aliases 
where contributors.cntrb_id=contributors_aliases.cntrb_id 
group by domain 
order by counter desc
limit 2000; 


