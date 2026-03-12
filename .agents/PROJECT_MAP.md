# Project Map & Architecture
This is a java+maven project.

## /src
The core application logic.
* `/src/main/java/couchbase`: Couchbase SDK wrappers, N1QL query templates, load generation, REST API, and transaction support. [Owner: BaseCoder]
  - `sdk/`: Core SDK integration and query execution
  - `transactions/`: Transaction management utilities
  - `loadgen/`: Document load generation templates
  - `rest/`: REST API endpoints
* `/src/main/java/mongo`: MongoDB SDK integration and load generation utilities. [Owner: MongoCoder]
  - `sdk/`: Core MongoDB client integration
  - `loadgen/`: Document load generation templates
* `/src/main/java/elasticsearch`: Elasticsearch client integration (EsClient.java)
* `/src/main/java/RestServer`: REST server infrastructure (RestApplication.java, TaskRequest.java)
  - `RestApplication.java`: Spring Boot application entry point with REST endpoint handlers (RestHandlers class) that delegate to TaskRequest for Couchbase, MongoDB, and SIFT document loading operations
  - `TaskRequest.java`: Business logic and implementation methods for REST endpoints including task management, document loading (Couchbase/MongoDB/SIFT), client creation, and task lifecycle operations
* `/src/main/java/utils`: Shared utilities and helper classes
  - `common/`: Common utility functions
    - `FileDownload.java`: Handles file downloads from URLs, decompression (GZIP), and file operations for SIFT datasets
  - `docgen/`: Document generation logic and workload management. Used by all document loaders (Like Couchbase, Mongo, Elastic, etc within this project)
    - `DocumentGenerator.java`: Abstract base class for key-value document generation with vbucket targeting, sub-document operations, and workload settings
    - `WorkLoadSettings.java`: Configuration class for workload parameters (key size, doc size, operations distribution)
    - `DocRange.java`: Manages document range specifications and indexing
    - `DocType.java`: Document type definitions and enumeration
    - `DRConstants.java`: Constants for document range operations
    - `WorkLoadBase.java`: Base workload configuration
    - `anySize.java`: Handles arbitrary size specifications
    - `mongo/`: MongoDB-specific document generation utilities
  - `key/`: Key generation strategies and utilities
    - `RandomKey.java`: Generates random alphanumeric keys based on workload settings
    - `SimpleKey.java`: Basic key generation with vbucket distribution
    - `CircularKey.java`: Circular key distribution for load testing
    - `ReverseKey.java`: Reverse order key generation
    - `HotKey.java`, `ColdKey.java`: Temperature-based key generation for cache testing
    - `RandomSizeKey.java`: Keys with random size variations
  - `taskmanager/`: Task orchestration and management
    - `TaskManager.java`: Manages thread pool execution, task submission, cancellation, and result tracking for concurrent operations
    - `Task.java`: Task definition with result tracking and abort capabilities
  - `val/`: Value templates and validation schemas
    - `Cars.java`, `MiniCars.java`: Automotive document templates
    - `Hotel.java`, `HeterogeneousHotel.java`: Hospitality document templates with nested structures
    - `Product.java`: E-commerce product document template
    - `Vector.java`: Large vector data generation (81KB)
    - `SimpleValue.java`, `anySizeValue.java`: Basic value generators
    - `SimpleSubDocValue.java`: Sub-document value templates
    - `RandomlyNestedJson.java`: Random nested JSON structure generator
    - `NimbusM.java`, `NimbusP.java`: Nimbus-specific document types
    - `siftBigANN.java`: SIFT BigANN dataset document representation
    - `ESSiftIndex.json`: Elasticsearch SIFT index configuration
    - `Dictionary.java`: Dictionary-based value generation
* `/src/main/java/Loader.java`: Main Couchbase document loader entry point
* `/src/main/java/MongoLoader.java`: MongoDB document loader entry point
* `/src/main/java/SIFTLoader.java`: SIFT-based document loader
* `/src/main/resources`: Runtime configuration files
  - `log4j.properties`: Log4j logging configuration

## /.agents
The operational brain of the AI workforce.
* `index.md`: The Agent Registry.
* PROJECT_MAP.md: This file.
* `profiles/`: Deep-dive instructions for each agent.

## /pom.xml
Maven project configuration with dependencies and build rules.
- **Project Info**: Java 8 Maven project (com.couchbase.capella:capella:0.0.1-SNAPSHOT)
- **Key Dependencies**:
  - Couchbase SDK (java-client 3.4.10)
  - MongoDB Java Driver (3.12.14)
  - Elasticsearch Java Client (8.11.3)
  - Spring Boot Web Starter (2.6.4) for REST server
  - DJL (Deep Java Library) with PyTorch models and HuggingFace tokenizers (0.25.0)
  - AWS Java SDK Core (1.8.10.2)
  - Apache Commons libraries (codec, lang3, io, cli)
  - Jackson JSON binding (2.12.3), JAXB API (2.3.1)
  - JavaFaker (1.0.2) for test data generation
  - SLF4J with Log4j12 (1.7.30) for logging
- **Build Configuration**:
  - Compiles to Java 8 target
  - Builds standalone JAR with dependencies copied to `magmadocloader/lib/`
  - Main class: Loader
  - Final artifact: `magmadocloader/magmadocloader.jar`

## /target
Dir consists of compiled java class and jar files. Usually nothing to look into this unless something related to output files missing issues
