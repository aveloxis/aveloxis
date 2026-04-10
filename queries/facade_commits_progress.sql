SELECT
  cs.repo_id,
  cs.facade_status,
  cs.commit_sum,
  COUNT(DISTINCT c.cmt_commit_hash) AS collected_commits
FROM aveloxis_ops.collection_status cs
LEFT JOIN aveloxis_data.commits c
  ON c.repo_id = cs.repo_id
WHERE cs.facade_status = 'Collecting'
GROUP BY
  cs.repo_id,
  cs.facade_status,
  cs.commit_sum
ORDER BY cs.commit_sum;

/**
repo_id	facade_status	commit_sum	collected_commits
188263	Collecting	0	38
151390	Collecting	524	80
187921	Collecting	1123	575
133710	Collecting	9811	0
165501	Collecting	25706	19485
152357	Collecting	65250	61429
162986	Collecting	77188	77188
150836	Collecting	144425	135932
151415	Collecting	307687	307687
301030	Collecting	1342127	1328145

repo_id	facade_status	commit_sum	collected_commits
151390	Collecting	524	80
165475	Collecting	4805	3579
165507	Collecting	5090	1492
133710	Collecting	9811	0
165509	Collecting	10956	2488
165487	Collecting	25167	17017
165501	Collecting	25706	2745
152357	Collecting	65250	36502
162986	Collecting	77188	69311
150836	Collecting	144425	135932
151415	Collecting	307687	301967
301030	Collecting	1342127	1308439

repo_id	facade_status	commit_sum	collected_commits
300215	Collecting	52660	49041
301395	Collecting	53055	53055
299325	Collecting	53333	34186
300974	Collecting	53872	11397
299747	Collecting	54431	54431
299471	Collecting	55758	33797
301123	Collecting	56651	56651
298041	Collecting	57329	57329
301304	Collecting	61419	35495
300345	Collecting	61722	61722
298702	Collecting	61762	27538
300921	Collecting	62194	62194
299157	Collecting	62264	62264
298904	Collecting	62270	62270
300886	Collecting	62682	6454

repo_id	facade_status	commit_sum	collected_commits
300215	Collecting	52660	5260
301395	Collecting	53055	6352
299325	Collecting	53333	2925
300974	Collecting	53872	7793
299747	Collecting	54431	8543
299471	Collecting	55758	2642
301123	Collecting	56651	6106
298041	Collecting	57329	5983
301304	Collecting	61419	414
300345	Collecting	61722	6516
298702	Collecting	61762	3452
300921	Collecting	62194	7576
299157	Collecting	62264	5970
298904	Collecting	62270	6911
299525	Collecting	62303	8147

repo_id	facade_status	commit_sum	collected_commits
300215	Collecting	52648	10588
301395	Collecting	52952	10536
299325	Collecting	53297	10617
300974	Collecting	53872	10559
299747	Collecting	54416	10405
299471	Collecting	55513	10747
301123	Collecting	56493	10593
298041	Collecting	57329	10450
301304	Collecting	61313	10620
300345	Collecting	61511	10482
298702	Collecting	61762	10545
298904	Collecting	62107	15113
299157	Collecting	62128	10590
300921	Collecting	62194	14978
300886	Collecting	62327	15055
**/ 