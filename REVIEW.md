# Claude Code Ultrareview Configuration: Aveloxis

This file overrides and directs the slash command agents during `/ultrareview`. Because Aveloxis is a high-velocity, fleet-scale open source software health collection engine, reviews must heavily penalize inefficient database operations, pipeline blocks, and structural inconsistencies.

---

## 1. Performance Focus (Go Ingestion Pipeline)

Aveloxis must maintain raw throughput dominance over older collection engines. 

*   **Goroutine Leaks:** Audit all worker pools, schedulers, and multi-platform platform loops to ensure channels close properly.
*   **Database Connection Saturation:** flag any database operations that do not leverage pooled transactions efficiently. 
*   **API Rate Limit Guardrails:** Verify token consumption logic across GitHub and GitLab modules to ensure parallel fetching does not aggressively exhaust tokens without backoff handling.
*   **Memory Allocations:** Strictly scrutinize high-frequency loops handling large JSON payloads (e.g., commit matrices or SBOM generation). Prefer buffer reuse and allocation-free serialization patterns wherever possible.

---

## 2. Correctness Focus (CHAOSS Metric Calculations)

Garbage-in data invalidates community health metrics. Review agents must verify data capture integrity.

*   **API Payload Mapping:** Ensure that cross-platform mappings handle structural nuances identically (e.g., reconciling GitLab's data types with GitHub's REST/GraphQL structures).
*   **Edge-Case Workflows:** Alert on missing error handlings during network timeouts, webhook dropouts, or partially restricted repository scopes.
*   **Augur Parity:** Flag any modification to metric ingestion methods that deviates from expected schema types or misinterprets standard CHAOSS definitions.

---

## 3. Architectural Modularity

Aveloxis must stay cleanly decoupled as it transitions from a companion pipeline to a total collection solution.

*   **Schema Isolation:** Enforce a strict boundary between `aveloxis_data`, `aveloxis_ops`, and `aveloxis_scan`. No ingestion package should have cross-cutting raw access that touches external native schemas.
*   **Platform Extensibility:** Ensure the base provider interface remains platform-agnostic. Adding an alternative forge/VCS host tomorrow should require zero modifications to core analytics or operations packages.
*   **Dependency Injection:** Highlight hardcoded configurations, tight component couplings, or shared states that complicate testing routines or containerized runner deployments.
*  **Go Language Standards** Follow go language programming standards. 
*  **Coupling and Cohesion** Always consider coupling and cohesion when determining what to incrementally refactor. 

---

## 4. Data Consistency (PostgreSQL Schema Integrity)

Since data from diverse sources converges in 142 tables, consistency constraints are paramount.

*   **Transaction Controls:** Enforce atomic transactions across multi-table metric commits. If a vulnerability scan or SBOM block fails mid-write, partial commits must rollback cleanly.
*   **Unique Constraints & Idempotency:** Validate that all data collection tasks utilize robust idempotent operations (e.g., `ON CONFLICT DO UPDATE`), preventing duplicate logs if a job is rescheduled or retried.
*   **Timezone & Timestamp Alignment:** Enforce strict UTC conversion routines at the interface boundary before pushing timeline metrics to PostgreSQL.
*   **Schema integrity** Make sure that the schema enforces foreign keys and uniqueness as a stable backstop against logic failures in code. 
