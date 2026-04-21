        
- internal/db/prepared_stmt_retry.go — isStalePreparedStatement classifier, sendBatchWithRetry wrapper, appendKeepaliveParams helper, and the keepaliveParams constant    

(all three defenses in one file so the retry policy is easy to find and change together).                                                                                 

- internal/db/prepared_stmt_retry_test.go — 6 tests covering the classifier, the wrapper's source contract, and the staging-flush wiring.                                 
                                                                                                                                                                        
Modified:                                                                                                                                                                 
- internal/db/postgres.go — NewPostgresStore calls appendKeepaliveParams(connString) before pgxpool.ParseConfig; DefaultQueryExecMode flipped back to                     
QueryExecModeCacheStatement; comment carries the full v0.18.10→v0.18.14 incident trail with reversion triggers.                                                           
- internal/db/staging.go — StagingWriter.Flush now calls sendBatchWithRetry instead of pool.SendBatch.                                                                    
- internal/db/pool_test.go — TestPoolUsesCacheStatement (flipped from TestPoolUsesDescribeCache), plus 3 new keepalive tests (TestPoolAppendsKeepaliveParams source       
contract, TestAppendKeepaliveParams_URLForm, _Idempotent, _KeywordForm runtime behavior).                                                                                 
- docs/guide/troubleshooting.md — new section on SQLSTATE 26000, workers=40 recommendation for Mac deployments, reversion steps.                                          
- internal/db/version.go — bumped to 0.18.14.                                                                                                                             
                                                                                                                                                                        
What to watch after deploying:                                                                                                                                            
                                                                                                                                                                        
1. Grep your log for prepared statement cache miss on SendBatch — that's the retry firing. A few per hour is expected and benign. A few per minute means keepalives aren't
tight enough (tune per the doc) or workers need to drop. 
2. Staging throughput should visibly improve — CacheStatement eliminates the per-query Parse+plan you were paying on every INSERT.                                        
3. Consider dropping workers from 80 to 40 in aveloxis.json for the reason documented: each worker now does more DB throughput, and your Mac's network stack was the      
self-inflicted bottleneck. 