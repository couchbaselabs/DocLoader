# Agent Registry & Documentation

## The CBRestLoader
> **Status:** Active | **Version:** 1.0.0

### Mission
To generate high-performance, thread-safe, and efficient REST-based document loader for Couchbase server environment using Java SDK (v3.x) and Spring Boot.

### Contextual Navigation (Directory Map)
```
graph TD
  RestApplication[src/main/java/RestServer/RestApplication.java] -->|Entry Point| RESTLOADER[The CBRestLoader]
  TaskRequest[src/main/java/RestServer/TaskRequest.java] -->|Business Logic| RESTLOADER
  RESTLOADER-->|Utilizes| Couchbase[src/main/java/couchbase]
  Couchbase-->|Utilizes| Utils[src/main/java/utils]
  Utils-->|Utilized by| Couchbase
```

### Logic & Constraints
* **Step-Zero:** Always scan `./src/main/java/couchbase` and `./src/main/java/RestServer` to understand existing SDK and REST patterns before proposing new code.
* **REST API Focus:** Modifications target Spring Boot REST endpoints (RestHandlers) and TaskRequest business logic for HTTP-based document loading.
* **SDK Precision:** Default to the latest Couchbase SDK (v3.x) unless specified otherwise.
* **N1QL Mastery:** Must prioritize Indexing strategies and GSI (Global Secondary Index) awareness when writing queries.
* **Hard Constraints:**
  - Never suggest client-side joining if a N1QL JOIN is more efficient.
  - Always include error handling for DocumentNotFound and CasMismatch.
* **Tone:** Technical, efficiency-focused, and precise.

### Work flow of loading
sequenceDiagram
    participant C as Client (REST)
    participant TM as TaskManager (Thread Pool)
    participant PL as SDKClientPool
    participant WL as WorkLoadGenerate (src/main/java/...)

    Note over C, PL: Initialization Phase
    C->>TM: /init_task_manager(N)
    C->>PL: /reset_sdk_client_pool
    C->>PL: /create_clients

    Note over C, WL: Execution Phase
    C->>C: /doc_load (Generate Request)
    C-->>C: Returns task_id
    C->>TM: /submit_task(task_id)

    TM->>PL: get_client_for_bucket()
    PL-->>TM: Returns SDKClient

    TM->>WL: run() logic
    WL->>WL: Perform Database Load

    WL->>PL: release_client()

    C->>TM: /get_task_result

### Performance Optimization Guidelines
* **Multi-Collection Strategy**: Prefer bucket-level clients with dynamic collection switching over per-collection client instances. Workers should call `selectCollection()` dynamically per operation instead of creating dedicated clients per collection.
* **Connection Scaling**: KV connections should scale based on: `num_workers × target_collections / connection_reuse_factor`. Default of 5 connections per SDKClient may be insufficient for high-concurrency multi-collection workloads.
* **Thread Pool Sizing**: Set `num_workers` based on concurrent task throughput needs, not total collections. Example: 60 workers efficiently handle 5000 collections with proper batching, rather than allocating 20 workers per collection.
* **Batch Processing**: For large-scale multi-collection loading, use batch processing to load collections in chunks (e.g., 60-100 collections per batch) to avoid client pool exhaustion.
* **Client Pool Optimization**: SDKClientPool should cache clients at bucket level and support dynamic scope/collection switching, not create separate client instances per (scope+collection) combination.

### Architecture Anti-Patterns
* **Per-Collection Client Instances**: Creating one SDKClient per collection causes connection exhaustion, memory bloat, and synchronization bottlenecks. With 5000 collections, this creates 5000 × 5 = 25,000 KV connections.
* **Sequential Task Queueing**: Loading 5000 collections with 60 workers creates sequential bottlenecks when each collection gets a separate task. Tasks should consolidate multiple collections into a single workload.
* **Fixed Thread Allocation**: Assuming all collections need dedicated workers. The architecture should support dynamic work distribution where workers cycle through multiple collections.
* **Synchronization Overhead**: Excessive locking in `get_client_for_bucket()` with unique (scope+collection) keys creates contention. Use bucket-level client caching with thread-safe collection switching.
* **Connection Thrashing**: Frequently creating/destroying SDKClient instances impacts performance. Reuse connections across operations with dynamic `selectCollection()` calls.

### Scaling Workflows

**Single Collection (Current Pattern):**
```
Client → TaskManager → WorkLoadGenerate → SDKClientPool → Specific Collection
```
Suitable for: Single collection workloads with static configuration.

**Multi-Collection Optimized (Recommended):**
```
Client → TaskManager → WorkLoadTasks → SDKClientPool (Bucket-Level)
                                   ↓
                            Dynamic Collection Switching per Worker
                                   ↓
                         Worker cycles through multiple collections
```
Suitable for: Large-scale multi-collection loading (hundreds/thousands of collections).

**Batched Multi-Collection:**
```
Client → TaskManager → BatchManager → WorkLoadGenerate (per batch)
                         ↓
                    60 workers load 60 collections concurrently
                         ↓
                    Next batch starts after completion
```
Suitable for: Very large collections (1000+) with controlled resource usage.

### Key Performance Metrics to Monitor
* **Connection Pool Utilization**: Monitor KV connection count vs capacity
* **Client Pool Efficiency**: Track client reuse rate vs new client creation
* **Thread Wait Time**: Measure worker idle time waiting for tasks vs clients
* **Task Queue Depth**: Monitor pending tasks in TaskManager
* **Collection Throughput**: Track collections loaded per time unit
* **Document Success Rate**: Monitor failedMutations and retry patterns
