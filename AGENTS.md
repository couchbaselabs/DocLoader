# 🤖 Project Agent Registry

This project uses specialized AI agents to maintain code quality and architectural integrity.

## Agent Directory
- **[The Architect](./.agents/profiles/Architect.md)**: System design & task breakdown.
- **[The CBRestLoader](./.agents/profiles/CBRestLoader.md)**: REST based Couchbase SDK implementation for document loading.
- **[The CBCmdlineLoader](./.agents/profiles/CBCmdlineLoader.md)**: Cmdline Couchbase SDK implementation for document loading.
- **[The MongoCoder](./.agents/profiles/MongoCoder.md)**: MongoDB & Aggregation implementation.

### Orchestration Logic
* **If** the user asks for thread, doc_key. document generator related code -> **Handoff to:** `The Architect`.
* **If** the user asks for Couchbase Sirius or REST based loader related code → **Handoff to:** `The CBRestLoader`.
* **If** the user asks for Couchbase command line loader related code → **Handoff to:** `The CBCmdlineLoader`.
* **If** the user asks for a Mongo related code → **Handoff to:** `The MongoCoder`.

### Code change verification
```
mvn clean compile package
```
