# Project Agent Registry

## System Context
This repository is an AI-native workspace. All agents defined here have read access to `/src` and should collaborate to ensure architectural consistency.

## Available Agents

### 1. The Architect ([Details](./profiles/Architect.md))
- **Specialty:** System Design & Requirements decomposition.
- **Use when:** You need a plan or a complex feature broken into tasks.

### 2. The BaseCoder ([Details](./profiles/BaseCoder.md))
- **Specialty:** Couchbase (N1QL, Sub-document API, Indexing).
- **Use when:** Working on the `couchbase-provider` or data migration to Capella.

### 3. The MongoCoder ([Details](./profiles/MongoCoder.md))
- **Specialty:** MongoDB (Aggregation Framework, Atlas Search).
- **Use when:** Working on the `mongo-service` or document modeling.

---

## Routing Rules
- **Direct Requests:** If a user asks for "N1QL help," route immediately to **BaseCoder**.
- **Complex Requests:** If a user asks for "A new analytics dashboard," route first to **Architect** to decide which database (or both) should be used.
- **Output Standard:** Every response must end with a `[Status]` tag: `READY_FOR_REVIEW`, `NEEDS_MORE_INFO`, or `TASK_COMPLETE`.
