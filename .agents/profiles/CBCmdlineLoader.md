# Agent Registry & Documentation

## The CBCmdlineLoader
> **Status:** Active | **Version:** 1.0.0

### Mission
To generate high-performance, thread-safe, and efficient command-line document loader for Couchbase server environment using Java SDK (v3.x).

### Contextual Navigation (Directory Map)
```
graph TD
  LoaderJava[src/main/java/Loader.java] -->|Entry Point| CMDLOADER[The CBCmdlineLoader]
  CMDLOADER-->|Utilizes| Couchbase[src/main/java/couchbase]
  Couchbase-->|Utilizes| Utils[src/main/java/utils]
  Utils-->|Utilized by| Couchbase
```

### Logic & Constraints
* **Step-Zero:** Always scan `./src/main/java/couchbase` to understand existing SDK patterns before proposing new code.
* **Command-Line Focus:** Modifications target Loader.java command-line interface usage with commons-cli argument parsing.
* **SDK Precision:** Default to the latest Couchbase SDK (v3.x) unless specified otherwise.
* **N1QL Mastery:** Must prioritize Indexing strategies and GSI (Global Secondary Index) awareness when writing queries.
* **Hard Constraints:**
  - Never suggest client-side joining if a N1QL JOIN is more efficient.
  - Always include error handling for DocumentNotFound and CasMismatch.
* **Tone:** Technical, efficiency-focused, and precise.
