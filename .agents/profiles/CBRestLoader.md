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
  TaskRequest -->|Submits workers to| TaskManager[src/main/java/utils/taskmanager/TaskManager.java]
  TaskManager -->|Schedules by worker index| RankedThreadPoolExecutor[src/main/java/utils/taskmanager/RankedThreadPoolExecutor.java]
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

**TaskManager / RankedThreadPoolExecutor** (`utils/taskmanager/`)
- **Purpose**: the pool that actually executes loads, and the only real bound on load
  concurrency. `/doc_load` merely *builds* `WorkLoadGenerate` objects; nothing runs until
  `/submit_task` hands them to this pool.
- **Scheduling**: the queue is ordered by `Task.workerIndex` (the worker's position within
  its own load), ties broken by submission order — **not** FIFO. So every load gets its
  1st worker before any load gets its 2nd.
- **Why**: `/doc_load` creates `process_concurrency` workers per collection, so N
  collections submit `N × process_concurrency` tasks against a fixed pool. Under FIFO the
  first few collections consumed every thread and later callers observed
  `completed_ops = 0` for as long as the loads ahead took to finish — indistinguishable
  from a hang, so their stall detectors fired on healthy work.
- **Consequence for callers**: per-load concurrency self-balances to
  `pool_size / active_loads`, capped by the requested `process_concurrency`. 450 loads on
  60 workers → 1 worker each, all progressing; 5 loads → ~12 each; 3 or fewer → the full
  20. Callers do not need to compute a concurrency value.
- **Unranked tasks** (`workerIndex` left at 0, e.g. the standalone CLI loaders) keep pure
  FIFO ordering, so their behavior is unchanged.
- **Skipping drained workers**: workers of one `/doc_load` share a generator, so once it
  is empty the ones still queued have nothing to do — but a caller waits for all of them,
  and on a wide run they are behind the entire run's work. The first worker to see an
  empty generator calls `Task.notifyWorkExhausted()`, and `TaskGroup` cancels the
  siblings that never started. `Task.claimForExecution()` decides who owns a task, so a
  *running* worker is never skipped: `Future.cancel(false)` alone would report the task
  cancelled while its runnable is still writing docs (a FutureTask stays in state `NEW`
  for the whole run), which would tell the caller a load had finished mid-flight.
- **Writing a new Task**: implement `runTask()`, not `run()`. `Task.run()` is final and
  takes the claim before calling `runTask()`, so a subclass cannot accidentally leave
  itself open to being cancelled mid-flight by `skipTask()`.
- **Disjoint-range loads** (SIFT/MSMARCO) must not join a `TaskGroup` — each of their
  workers owns its own doc range, so none is redundant.

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

### Task Lifecycle & Cleanup Flow

`loader_tasks` and `completed_tasks` are the two `ConcurrentHashMap`s in `TaskRequest` that track task state.
Tasks are added to `loader_tasks` on `doc_load`, then transition based on which completion path is taken.

**Path A — `get_task_result()` called before `stop_task()`**
```
doc_load  ──► loader_tasks.put(taskName, task)
               │
submit_task ──► taskManager.submit(task) ──► task runs ──► "Task_X is completed!" (log)
               │
get_task_result()
  ├─► taskManager.getTaskResult(task)     (waits for future)
  ├─► loader_tasks.remove(taskName)
  └─► completed_tasks.put(taskName, task)
               │
stop_task()
  ├─► loader_tasks.get()       → null
  ├─► completed_tasks.contains → true
  └─► status: true  ✓
```

**Path B — `stop_task()` called before `get_task_result()`**
```
doc_load  ──► loader_tasks.put(taskName, task)
               │
submit_task ──► taskManager.submit(task) ──► task runs
               │
stop_task()
  ├─► task.stop_load()                    (sets flag, run loop exits)
  ├─► loader_tasks.remove(taskName)
  └─► completed_tasks.put(taskName, task)
               │
get_task_result()
  ├─► loader_tasks.getOrDefault(taskName, completed_tasks.get(taskName))
  │                                        (finds task in completed_tasks)
  ├─► taskManager.getTaskResult(task)
  └─► failures + status returned  ✓
```

**Reset**
```
reset_task_manager()
  ├─► loader_tasks    = new ConcurrentHashMap()
  └─► completed_tasks = new ConcurrentHashMap()   (prevents stale name collisions across test runs)
```

**Why `completed_tasks` exists:**
Without it, `stop_task` called after `get_task_result` (or vice versa) hits a missing key in `loader_tasks`
and returns `{"status": false, "error": "Task X does not exists"}` — logged as CRITICAL in TAF
even though the task completed successfully.

### Performance Optimization Guidelines
* **Multi-Collection Strategy**: Prefer bucket-level clients with dynamic collection switching over per-collection client instances. Workers should call `selectCollection()` dynamically per operation instead of creating dedicated clients per collection.
* **Shared Cluster Management**: Use `SharedClusterManager` for all multi-collection workloads. It provides:
  - Single Cluster instance per server connection to avoid connection exhaustion
  - Optimized KV connections (default: 500) for massively parallel collection loads
  - Thread-safe reference counting and automatic resource cleanup
  - Environment recreation capability for long-running workloads
* **Connection Scaling**: KV connections should scale based on: `num_workers × target_collections / connection_reuse_factor`. Default of 5 connections per SDKClient may be insufficient for high-concurrency multi-collection workloads. SharedClusterManager defaults to 500 KV connections for large-scale loads.
* **Thread Pool Sizing**: `num_workers` is the hard ceiling on how many workers run at once, across every collection. Size it for throughput, not for collection count — 60 workers serve 5000 collections fine, because the scheduler spreads the first worker of each load across the pool instead of stacking `process_concurrency` workers on the first few collections. Raising `process_concurrency` beyond `num_workers / concurrent_loads` adds queue depth, not throughput.
* **Batch Processing**: For large-scale multi-collection loading (1000+ collections), use `CollectionLoadBatcher` to:
  - Process collections in batches (default: 50 per batch)
  - Monitor batch progress and completion status
  - Automatically start next batch after current completion
  - Note: worker starvation is *not* what this addresses — that is the scheduler's job (see TaskManager above)
* **Client Pool Optimization**: SDKClientPool should cache clients at bucket level and support dynamic scope/collection switching, not create separate client instances per (scope+collection) combination.

### Architecture Anti-Patterns
* **Per-Collection Client Instances**: Creating one SDKClient per collection causes connection exhaustion, memory bloat, and synchronization bottlenecks. With 5000 collections, this creates 5000 × 5 = 25,000 KV connections.
* **FIFO Task Queueing**: Draining one load's entire worker set before starting the next load's first worker. With more loads than threads this leaves most callers at `completed_ops = 0` for the whole time the loads ahead of them run, which reads as a hang. Keep the queue ordered by `Task.workerIndex`, and set `workerIndex` on every worker a request creates.
* **Caller-Side Concurrency Arithmetic**: Computing "how many workers can I ask for" outside the loader. The pool already degrades `process_concurrency` to what it can serve; a caller that pre-divides duplicates that logic against a worker count it cannot see.
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
* **Task Queue Depth**: `/get_task_progress` returns `active_tasks`, `queued_tasks` and `pool_workers` alongside the per-task fields. A task reporting `completed_ops = 0` while `queued_tasks > 0` is waiting for a thread; the same task with `queued_tasks = 0` is genuinely stuck and worth investigating.
* **Collection Throughput**: Track collections loaded per time unit
* **Document Success Rate**: Monitor failedMutations and retry patterns

### Hard Constraints Integration
* **SharedClusterManager**: Must use `SharedClusterManager.getCluster(server)` and `releaseCluster(server)` for all multi-collection operations. Never create standalone Cluster instances for large-scale workloads.
* **Environment Lifecycle**: Must follow proper ClusterEnvironment lifecycle - use shared environment with automatic recreation capability, never manually manage environment shutdown/reactivation.
* **Worker Starvation**: Handled by the scheduler, not by the caller. Any request that creates multiple workers must set `Task.workerIndex` on each of them; without it every worker of that request ranks 0 and reintroduces the FIFO starvation for loads behind it.
* **Thread Safety**: SharedClusterManager uses synchronized methods and volatile shutdown flag - ensure thread-safe access patterns when dealing with reference counting and environment state.
* **Error Handling**: Always handle `AuthenticationFailureException` and cluster connection errors with proper logging and retries in both SharedClusterManager and CollectionLoadBatcher.

### Build Verification
```
mvn clean compile package
```
