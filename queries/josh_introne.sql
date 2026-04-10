SELECT COUNT(*)
FROM messages
WHERE LOWER(msg_text) LIKE '%gpt%'
  AND msg_timestamp::DATE > TO_DATE('11/30/2022', 'MM/DD/YYYY'); -- 39,466

select count(*) from messages where lower(msg_text) like ' %gpt%' 
  AND msg_timestamp::DATE > TO_DATE('11/30/2022', 'MM/DD/YYYY'); --87
	
 select count(*) from messages where lower(msg_text) like '% gpt %' 
  AND msg_timestamp::DATE > TO_DATE('11/30/2022', 'MM/DD/YYYY'); --2,598
	
 select count(*) from messages where lower(msg_text) like '%gpt %' 
  AND msg_timestamp::DATE > TO_DATE('11/30/2022', 'MM/DD/YYYY'); -- 7,897