# DocLoader

## Overview

DocLoader is a comprehensive Java-based document loading and testing tool designed 
for Couchbase, MongoDB, and Elasticsearch databases. It provides flexible workload 
generation capabilities for performance testing, data loading, and validation 
scenarios across multiple NoSQL databases.

## Features

• Multi-database support (Couchbase, MongoDB, Elasticsearch)
• Configurable workload patterns (Create, Read, Update, Delete, Expiry)
• Subdocument operations support
• Transaction testing capabilities
• Customizable document and key generation
• Batch processing with configurable batch sizes
• Performance monitoring and metrics
• Support for various data types and document structures

## Prerequisites

• Java 8 or higher
• Maven 3.6+
• Access to target database (Couchbase/MongoDB/Elasticsearch)
• Network connectivity to database servers

## Build Instructions

1. Clone the repository
2. Navigate to the project directory
3. Build the project:
   ```bash
   mvn clean compile
   ```

## Running the Application

### Couchbase Loader:
```bash
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="Loader" -Dexec.args="-n <ip> -user <username> -pwd <password> -b <bucket-name> -p 11210 -create_s 0 -create_e 10000000 -cr 100 -ops 100000 -docSize 1024 -scope <> -collection <>"
```

### MongoDB Loader:
```bash
mvn compile exec:java -Dexec.mainClass="MongoLoader" -Dexec.args="<connection-string> <database> <collection>"
```

### SIFT Loader:
```bash
mvn compile exec:java -Dexec.mainClass="SIFTLoader" -Dexec.args="<parameters>"
```

## Command Line Options

### Required Options:
- `-n, --node <arg>`              IP Address of the database server
- `-user, --rest_username <arg>`  Username for authentication
- `-pwd, --rest_password <arg>`   Password for authentication
- `-b, --bucket <arg>`            Bucket name (Couchbase)
- `-p, --port <arg>`              Memcached Port

### Optional Options:
- `-cr, --create <arg>`           Percentage of create operations
- `-create_s, --create_s <arg>`   Create operations start range
- `-create_e, --create_e <arg>`   Create operations end range
- `-rd, --read <arg>`             Percentage of read operations
- `-read_s, --read_s <arg>`       Read operations start range
- `-read_e, --read_e <arg>`       Read operations end range
- `-up, --update <arg>`           Percentage of update operations
- `-update_s, --update_s <arg>`   Update operations start range
- `-update_e, --update_e <arg>`   Update operations end range
- `-dl, --delete <arg>`           Percentage of delete operations
- `-delete_s, --delete_s <arg>`   Delete operations start range
- `-delete_e, --delete_e <arg>`   Delete operations end range
- `-ex, --expiry <arg>`           Percentage of expiry operations
- `-expiry_s, --expiry_s <arg>`   Expiry operations start range
- `-expiry_e, --expiry_e <arg>`   Expiry operations end range
- `-docSize, --docSize <arg>`     Size of documents in bytes
- `-keySize, --keySize <arg>`     Size of keys in bytes
- `-keyType, --keyType <arg>`     Key generation type (Random/Sequential/Reverse)
- `-valueType, --valueType <arg>` Value generation type
- `-ops, --ops <arg>`             Operations per second
- `-w, --workers <arg>`           Number of worker threads
- `-batchSize, --batchSize <arg>` Batch size for operations
- `-scope <arg>`                  Scope name (Couchbase)
- `-collection <arg>`             Collection name (Couchbase)
- `-durability <arg>`             Durability level
- `-validate, --validate <arg>`   Validate data during reads
- `-loadType, --loadType <arg>`   Load type (Hot/Cold)
- `-gtm, --gtm <arg>`             Go for max document operations
- `-transaction_patterns <arg>`   Transaction load patterns

## Project Structure

```
src/main/java/
├── Loader.java                    # Main Couchbase loader
├── MongoLoader.java              # MongoDB loader
├── SIFTLoader.java               # SIFT-specific loader
├── couchbase/                    # Couchbase-specific utilities
├── mongo/                        # MongoDB-specific utilities
├── elasticsearch/                # Elasticsearch-specific utilities
├── RestServer/                   # REST server components
└── utils/
    ├── docgen/                   # Document generation utilities
    │   └── DocumentGenerator.java # Document generation logic
    ├── key/                      # Key generation utilities
    └── val/                      # Value generation utilities
```

## Key Components

• **DocumentGenerator**: Handles document creation and management
• **Key Generators**: RandomKey, SimpleKey, CircularKey, ReverseKey
• **Value Generators**: Various document templates (Hotel, Cars, Product, etc.)
• **Workload Settings**: Configuration management for test parameters
• **Database Connectors**: Couchbase, MongoDB, Elasticsearch clients

## Usage Examples

### Basic Couchbase Load Test:
```bash
mvn compile exec:java -Dexec.mainClass="couchbase.test.sdk.Loader" -Dexec.args="-n localhost -user Administrator -pwd password -b test-bucket -p 11210 -create_s 0 -create_e 10000 -cr 100 -ops 1000 -docSize 1024"
```

### Read-Heavy Workload:
```bash
mvn compile exec:java -Dexec.mainClass="couchbase.test.sdk.Loader" -Dexec.args="-n localhost -user Administrator -pwd password -b test-bucket -p 11210 -read_s 0 -read_e 10000 -rd 80 -up 20 -ops 5000"
```

### Mixed Workload with Updates:
```bash
mvn compile exec:java -Dexec.mainClass="couchbase.test.sdk.Loader" -Dexec.args="-n localhost -user Administrator -pwd password -b test-bucket -p 11210 -create_s 0 -create_e 5000 -cr 30 -rd 40 -up 30 -ops 2000"
```

## Troubleshooting

• Ensure database server is running and accessible
• Verify network connectivity and firewall settings
• Check authentication credentials
• Monitor system resources (CPU, memory, network)
• Review logs for detailed error information

## Performance Tips

• Start with smaller batch sizes and gradually increase
• Monitor system resources during load testing
• Use appropriate worker thread counts based on system capacity
• Consider network latency when testing remote databases
• Use appropriate document sizes for your use case

## License

This project is part of the Couchbase testing suite.

## Support

For issues and questions, please refer to the Couchbase documentation
or contact the development team.
