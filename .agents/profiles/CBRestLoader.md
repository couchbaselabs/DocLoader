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
  CollectionLoadBatcher[src/main/java/RestServer/CollectionLoadBatcher.java] -->|Batch Processing| RESTLOADER
  RESTLOADER-->|Utilizes| Couchbase[src/main/java/couchbase]
  Couchbase-->|Uses| Utils[src/main/java/utils]
  Couchbase/sdk/SDKClientPool -->|Uses| SharedClusterManager[src/main/java/couchbase/sdk/SharedClusterManager.java]
  SharedClusterManager -->|Manages| Cluster Instances
  CollectionLoadBatcher -->|Coordinates| TaskManager
  Utils-->|Utilized by| Couchbase
  Utils-->|Utilized by| RestServer
```

### Logic & Constraints
* **Step-Zero:** Always scan `./src/main/java/couchbase` and `./src/main/java/RestServer` to understand existing SDK and REST patterns before proposing new code.
* **Component Selection:**
  - **Single Collection Workloads**: Use standard `SDKClientPool` → `SDKClient` → `Cluster` pattern
  - **Multi-Collection Workloads (100-1000 collections)**: Use `SharedClusterManager` + dynamic collection switching
  - **Massive Collection Loads (1000+ collections)**: Use `CollectionLoadBatcher` + `SharedClusterManager`
  - **High-Throughput Operations**: Leverage shared ClusterEnvironment with 500+ KV connections
* **REST API Focus:** Modifications target Spring Boot REST endpoints (RestHandlers) and TaskRequest business logic for HTTP-based document loading.
* **SDK Precision:** Default to the latest Couchbase SDK (v3.x) unless specified otherwise.
* **N1QL Mastery:** Must prioritize Indexing strategies and GSI (Global Secondary Index) awareness when writing queries.
* **Hard Constraints:**
  - Never suggest client-side joining if a N1QL JOIN is more efficient.
  - Always include error handling for DocumentNotFound and CasMismatch.
* **Tone:** Technical, efficiency-focused, and precise.

### Core Architecture Components

**SharedClusterManager** (`couchbase/sdk/SharedClusterManager.java`)
- **Purpose**: Singleton pattern managing shared Cluster instances per server connection to avoid connection exhaustion
- **Key Features**:
  - Shared ClusterEnvironment with optimized KV connections (default: 500 for massively parallel loads)
  - Thread-safe reference counting for Cluster instances
  - Automatic environment recreation post-shutdown for long-running workloads
  - Supports both TLS and non-TLS connections
- **Usage Pattern**:
  ```java
  Cluster cluster = SharedClusterManager.getCluster(server);
  // Perform operations
  SharedClusterManager.releaseCluster(server);
  ```
- **Performance Benefits**: Eliminates connection thrashing for multi-collection workloads, reduces memory overhead from per-collection Cluster instances

**CollectionLoadBatcher** (`RestServer/CollectionLoadBatcher.java`)
- **Purpose**: Java-side batch processing for massive collection loads (thousands of collections)
- **Key Features**:
  - Fixed batch size (default: 50) with concurrent processing
  - Thread-safe batch state tracking with progress monitoring
  - Prevents worker starvation and queue overhead
  - Integration with REST API via `submitToBatch()` endpoint
- **Usage Pattern**:
  ```java
  ResponseEntity<Map<String, Object>> result = 
      CollectionLoadBatcher.submitToBatch(requestBody);
  ```
- **Performance Benefits**: Sequential Python calls become batched Java operations, maximizing throughput for massive collection loads

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
* **Shared Cluster Management**: Use `SharedClusterManager` for all multi-collection workloads. It provides:
  - Single Cluster instance per server connection to avoid connection exhaustion
  - Optimized KV connections (default: 500) for massively parallel collection loads
  - Thread-safe reference counting and automatic resource cleanup
  - Environment recreation capability for long-running workloads
* **Connection Scaling**: KV connections should scale based on: `num_workers × target_collections / connection_reuse_factor`. Default of 5 connections per SDKClient may be insufficient for high-concurrency multi-collection workloads. SharedClusterManager defaults to 500 KV connections for large-scale loads.
* **Thread Pool Sizing**: Set `num_workers` based on concurrent task throughput needs, not total collections. Example: 60 workers efficiently handle 5000 collections with proper batching, rather than allocating 20 workers per collection.
* **Batch Processing**: For large-scale multi-collection loading (1000+ collections), use `CollectionLoadBatcher` to:
  - Process collections in batches (default: 50 per batch)
  - Prevent worker starvation and reduce queue overhead
  - Monitor batch progress and completion status
  - Automatically start next batch after current completion
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

**Multi-Collection Optimized (SharedClusterManager):**
```
Client → TaskManager → WorkLoadTasks → SDKClientPool → SharedClusterManager
                                                 ↓
                                            Single Cluster per Server
                                                 ↓
                            Dynamic Collection Switching per Worker
                                                 ↓
                                         Worker cycles through collections
```
Suitable for: Large-scale multi-collection loading (hundreds/thousands) with optimized connection management.

**Batched Multi-Collection (CollectionLoadBatcher):**
```
Client → CollectionLoadBatcher → (Batch 1: 50 collections)
                               → WorkLoadGenerate per collection
                               → Progress Tracking
                               → (Batch 2: 50 collections) after completion
```
Suitable for: Very large number of collections (1000+) where Python sequential calls would cause worker starvation. Uses SharedClusterManager internally for connection optimization.

### Key Performance Metrics to Monitor
* **SharedClusterManager Metrics**:
  - Cluster reference count and reuse rate
  - KV connection utilization vs capacity (default: 500)
  - Environment shutdown/recreation events
  - Per-server cluster instance count
* **CollectionLoadBatcher Metrics**:
  - Active batch count and batch progress percentage
  - Collections loaded per batch vs batch size (default: 50)
  - Batch completion rate and queue depth
  - Batch processor thread pool utilization
* **Connection Pool Utilization**: Monitor KV connection count vs capacity
* **Client Pool Efficiency**: Track client reuse rate vs new client creation
* **Thread Wait Time**: Measure worker idle time waiting for tasks vs clients
* **Task Queue Depth**: Monitor pending tasks in TaskManager
* **Collection Throughput**: Track collections loaded per time unit
* **Document Success Rate**: Monitor failedMutations and retry patterns

### Hard Constraints Integration
* **SharedClusterManager**: Must use `SharedClusterManager.getCluster(server)` and `releaseCluster(server)` for all multi-collection operations. Never create standalone Cluster instances for large-scale workloads.
* **Environment Lifecycle**: Must follow proper ClusterEnvironment lifecycle - use shared environment with automatic recreation capability, never manually manage environment shutdown/reactivation.
* **Batch Processing Threshold**: For workloads with >100 collections, use `CollectionLoadBatcher.submitToBatch()` instead of direct REST calls to prevent worker starvation.
* **Thread Safety**: SharedClusterManager uses synchronized methods and volatile shutdown flag - ensure thread-safe access patterns when dealing with reference counting and environment state.
* **Error Handling**: Always handle `AuthenticationFailureException` and cluster connection errors with proper logging and retries in both SharedClusterManager and CollectionLoadBatcher.

### Build Verification
```
mvn clean compile package
```
