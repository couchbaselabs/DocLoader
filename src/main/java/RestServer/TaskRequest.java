package RestServer;

import utils.docgen.DRConstants;
import utils.docgen.DocRange;
import utils.docgen.DocumentGenerator;
import utils.docgen.WorkLoadSettings;
import utils.docgen.mongo.MongoDocumentGenerator;
import couchbase.loadgen.WorkLoadGenerate;
import couchbase.sdk.SDKClientPool;
import couchbase.sdk.Server;
import elasticsearch.EsClient;
import mongo.sdk.MongoSDKClient;
import couchbase.sdk.Result;
import utils.common.FileDownload;
import utils.taskmanager.Task;
import utils.taskmanager.TaskManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRequest {
    static TaskManager taskManager;
    static AtomicInteger task_id = new AtomicInteger();
    static SDKClientPool SDKClientPool = new SDKClientPool();
    static ArrayList<Server> known_servers = new ArrayList<Server>();
    static Object lock_obj = new Object();
    static private ConcurrentHashMap<String, WorkLoadGenerate> loader_tasks = new ConcurrentHashMap<String, WorkLoadGenerate>();
    static private ConcurrentHashMap<String, mongo.loadgen.WorkLoadGenerate> mongo_loader_tasks = new ConcurrentHashMap<String, mongo.loadgen.WorkLoadGenerate>();

    // Consumed by init_task_manager()
    @JsonProperty("num_workers")
    private int num_workers;

    // Used by SDKClientPool management
    @JsonProperty("req_clients")
    private int req_clients;

    // Consumed by doc_load()
    // Connection params
    @JsonProperty("server_ip")
    private String serverIP;
    @JsonProperty("server_port")
    private String serverPort;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("bucket_name")
    private String bucketName;
    @JsonProperty("scope_name")
    private String scopeName;
    @JsonProperty("collection_name")
    private String collectionName;
    // Create params
    @JsonProperty("create_percent")
    private int createPercent;
    @JsonProperty("create_start")
    private int createStartIndex;
    @JsonProperty("create_end")
    private int createEndIndex;
    // Delete params
    @JsonProperty("delete_percent")
    private int deletePercent;
    @JsonProperty("delete_start")
    private int deleteStartIndex;
    @JsonProperty("delete_end")
    private int deleteEndIndex;
    // Update params
    @JsonProperty("update_percent")
    private int updatePercent;
    @JsonProperty("update_start")
    private int updateStartIndex;
    @JsonProperty("update_end")
    private int updateEndIndex;
    // Read params
    @JsonProperty("read_percent")
    private int readPercent;
    @JsonProperty("read_start")
    private int readStartIndex;
    @JsonProperty("read_end")
    private int readEndIndex;
    // Touch params
    @JsonProperty("touch_start")
    private int touchStartIndex;
    @JsonProperty("touch_end")
    private int touchEndIndex;
    // Replace params
    @JsonProperty("replace_start")
    private int replaceStartIndex;
    @JsonProperty("replace_end")
    private int replaceEndIndex;
    // Expiry params
    @JsonProperty("expiry_percent")
    private int expiryPercent;
    @JsonProperty("expiry_start")
    private int expiryStartIndex;
    @JsonProperty("expiry_end")
    private int expiryEndIndex;
    // Subdoc params
    @JsonProperty("subdoc_percent")
    private int subdocPercent;
    @JsonProperty("create_path")
    private boolean createPath;
    @JsonProperty("is_subdoc_xattr")
    private boolean isSubdocXattr;
    @JsonProperty("is_subdoc_sys_xattr")
    private boolean isSubdocSysXattr;
    // Subdoc insert params
    @JsonProperty("sd_insert_start")
    private int sdInsertStartIndex;
    @JsonProperty("sd_insert_end")
    private int sdInsertEndIndex;
    // Subdoc upsert params
    @JsonProperty("sd_upsert_start")
    private int sdUpsertStartIndex;
    @JsonProperty("sd_upsert_end")
    private int sdUpsertEndIndex;
    // Subdoc remove params
    @JsonProperty("sd_remove_start")
    private int sdRemoveStartIndex;
    @JsonProperty("sd_remove_end")
    private int sdRemoveEndIndex;
    // Subdoc lookup params
    @JsonProperty("sd_read_start")
    private int sdReadStartIndex;
    @JsonProperty("sd_read_end")
    private int sdReadEndIndex;
    // Document related params
    @JsonProperty("key_prefix")
    private String keyPrefix;
    @JsonProperty("key_size")
    private int keySize;
    @JsonProperty("doc_size")
    private int docSize;
    @JsonProperty("key_type")
    private String keyType;
    @JsonProperty("value_type")
    private String valueType;
    // Load properties
    @JsonProperty("num_vbuckets")
    private int numVBuckets;
    @JsonProperty("target_vbuckets")
    private int[] targetVBuckets;
    @JsonProperty("timeout")
    private int timeout;
    @JsonProperty("timeout_unit")
    private String timeoutUnit;
    @JsonProperty("doc_ttl")
    private int docTTL;
    @JsonProperty("doc_ttl_unit")
    private String docTTLUnit;
    @JsonProperty("durability_level")
    private String durabilityLevel;
    @JsonProperty("ops")
    private int ops;
    @JsonProperty("gtm")
    private boolean gtm;
    @JsonProperty("process_concurrency")
    private int processConcurrency;
    @JsonProperty("iterations")
    private int iterations;
    @JsonProperty("track_failures")
    private boolean trackFailures;
    @JsonProperty("validate_docs")
    private boolean validateDocs;
    @JsonProperty("validate_deleted_docs")
    private boolean validateDeletedDocs;
    @JsonProperty("mutate")
    private int mutate;
    @JsonProperty("load_type")
    private String loadType;
    @JsonProperty("transaction_patterns")
    private String transactionPatterns;
    @JsonProperty("elastic")
    private boolean elastic;
    @JsonProperty("es_server")
    private String esServer;
    @JsonProperty("es_api_key")
    private String esAPIKey;
    @JsonProperty("es_similarity")
    private String esSimilarity;
    @JsonProperty("model")
    private String model;
    @JsonProperty("mock_vector")
    private boolean mockVector;
    @JsonProperty("dim")
    private int dim;
    @JsonProperty("base64")
    private boolean base64;
    @JsonProperty("mutate_field")
    private String mutateField;
    @JsonProperty("mutation_timeout")
    private int mutationTimeout;
    @JsonProperty("base_vectors_file_path")
    private String baseVectorsFilePath;
    @JsonProperty("sift_url")
    private String siftURL;

    // Used by add_new_task(), get_task_result(), stop_task(), cancel_task()
    @JsonProperty("task_id")
    private String taskName;
    /*
     * Following params are yet to be implemented.
     * -loadType,--loadType <arg> Hot/Cold
     * -transaction_patterns <arg> Transaction load pattern
     * -valueType,--valueType <arg>
     */

    // Mongo params
    @JsonProperty("mongo_server_ip")
    private String mongoServerIP;
    @JsonProperty("mongo_server_port")
    private String mongoServerPort;
    @JsonProperty("mongo_username")
    private String mongoUsername;
    @JsonProperty("mongo_password")
    private String mongoPassword;
    @JsonProperty("mongo_bucket_name")
    private String mongoBucketName;
    @JsonProperty("mongo_collection_name")
    private String mongoCollectionName;
    @JsonProperty("mongo_is_atlas")
    private boolean mongoIsAtlas;
    private ArrayList<MongoSDKClient> mongoClients;

    private boolean validate_doc_load_params() {
        if (this.bucketName == null && this.mongoBucketName == null)
            return false;

        if (this.createPercent
                + this.updatePercent
                + this.readPercent
                + this.deletePercent
                + this.expiryPercent
                + this.subdocPercent != 100)
            return false;

        // Set default values if null
        if (this.scopeName == null)
            this.scopeName = "_default";
        if (this.collectionName == null)
            this.collectionName = "_default";
        if ((this.durabilityLevel == null) || (this.durabilityLevel.equals("")))
            this.durabilityLevel = "NONE";
        if (this.keyPrefix == null)
            this.keyPrefix = "test_doc-";
        if (this.keySize == 0)
            this.keySize = 20;
        if (this.docSize == 0)
            this.docSize = 256;
        if (this.keyType == null)
            this.keyType = "SimpleKey";
        if (this.valueType == null)
            this.valueType = "SimpleValue";
        if (this.docTTLUnit == null)
            this.docTTLUnit = "seconds";
        if (this.timeoutUnit == null)
            this.timeoutUnit = "seconds";
        if (this.processConcurrency == 0)
            this.processConcurrency = 1;
        return true;
    }

    // Generate getters and setters for all parameters
    public static TaskRequest fromJson(String json) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, TaskRequest.class);
    }

    private void log_request() {
        System.out.println("Server: " + serverIP + ":" + serverPort);
        System.out.println("Creds: " + username + " / " + password);
        System.out.println("Bucket: " + bucketName + ":" + scopeName + ":" + collectionName);
        System.out.println("Key Type: " + keyType + ", Size: " + keySize + ", Prefix: '" + keyPrefix + "'");
        System.out.println("Doc Type: " + valueType + ", Size: " + docSize);
        System.out.println("---- Percent ----");
        System.out.println("  Create: " + createPercent);
        System.out.println("  Update: " + updatePercent);
        System.out.println("  Delete: " + deletePercent);
        System.out.println("  Read  : " + readPercent);
        System.out.println("  Expiry: " + expiryPercent);
        System.out.println("  SubDoc: " + subdocPercent);
        System.out.println("---- Indexes ----");
        System.out.println("creates: (" + createStartIndex + ", " + createEndIndex + ")");
        System.out.println("updates: (" + updateStartIndex + ", " + updateEndIndex + ")");
        System.out.println("reads  : (" + readStartIndex + ", " + readEndIndex + ")");
        System.out.println("deletes: (" + deleteStartIndex + ", " + deleteEndIndex + ")");
        System.out.println("touch  : (" + touchStartIndex + ", " + touchEndIndex + ")");
        System.out.println("replace: (" + replaceStartIndex + ", " + replaceEndIndex + ")");
        System.out.println("expiry : (" + expiryStartIndex + ", " + expiryEndIndex + ")");
        System.out.println("sd insert: (" + sdInsertStartIndex + ", " + sdInsertEndIndex + ")");
        System.out.println("sd upsert: (" + sdUpsertStartIndex + ", " + sdUpsertEndIndex + ")");
        System.out.println("sd lookup: (" + sdReadStartIndex + ", " + sdReadEndIndex + ")");
        System.out.println("sd remove: (" + sdRemoveStartIndex + ", " + sdRemoveEndIndex + ")");
        System.out.println("sd is_xattr: " + isSubdocXattr);
        System.out.println("sd is_sys_xattr: " + isSubdocSysXattr);
        System.out.println("sd create_path: " + createPath);
        System.out.println("Timeout: " + timeout + ", Unit: " + timeoutUnit);
        System.out.println("doc_ttl: " + docTTL + ", Unit: " + docTTLUnit);
        System.out.println("durability_level: " + durabilityLevel);
        System.out.println("Total vbuckets: " + numVBuckets + ", target_vbuckets: " + targetVBuckets);
        System.out.println("ops: " + ops);
        System.out.println("gtm: " + gtm);
        System.out.println("process_concurrency: " + processConcurrency);
        System.out.println("iterations: " + iterations);
        System.out.println("track_failures: " + trackFailures);
        System.out.println("validate_docs: " + validateDocs);
        System.out.println("validate_deleted_docs: " + validateDeletedDocs);
        System.out.println("mutate: " + mutate);
        System.out.println("load_type: " + loadType);
        System.out.println("transaction_patterns: " + transactionPatterns);
        System.out.println("elastic: " + elastic);
        System.out.println("es_server: " + esServer);
        System.out.println("es_api_key: " + esAPIKey);
        System.out.println("es_similarity: " + esSimilarity);
        System.out.println("model: " + model);
        System.out.println("mock_vector: " + mockVector);
        System.out.println("dim: " + dim);
        System.out.println("base64: " + base64);
        System.out.println("mutate_field: " + mutateField);
        System.out.println("mutation_timeout: " + mutationTimeout);
        System.out.println("base_vectors_file_path: " + baseVectorsFilePath);
        System.out.println("sift_url: " + siftURL);
        System.out.println("mongo_server_ip: " + mongoServerIP);
        System.out.println("mongo_server_port: " + mongoServerPort);
        System.out.println("mongo_username: " + mongoUsername);
        System.out.println("mongo_password: " + mongoPassword);
        System.out.println("mongo_bucket_name: " + mongoBucketName);
        System.out.println("mongo_collection_name: " + mongoCollectionName);
        System.out.println("mongo_is_atlas: " + mongoIsAtlas);
    }

    public ResponseEntity<Map<String, Object>> create_clients() {
        Server master = new Server(this.serverIP, this.serverPort,
                this.username, this.password,
                this.serverPort);
        Map<String, Object> body = new HashMap<>();
        try {
            TaskRequest.SDKClientPool.create_clients(this.bucketName, master,
                    this.req_clients);
            body.put("status", true);
        } catch (Exception e) {
            body.put("error", e.toString());
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
    }

    private void reset_sdk_client_pool() {
        if (TaskRequest.SDKClientPool != null)
            TaskRequest.SDKClientPool.shutdown();
        TaskRequest.SDKClientPool = new SDKClientPool();
    }
    
    private void reset_mongo_sdk_client_pool() {
        if (this.mongoClients != null) {
            for (MongoSDKClient client : this.mongoClients) {
                client.disconnectCluster();
            }
        }
        this.mongoClients = new ArrayList<MongoSDKClient>();
    }

    private void init_taskmanager() {
        TaskRequest.taskManager = new TaskManager(this.num_workers);
        System.out.println("Init TaskManager workers=" + this.num_workers);
        this.reset_sdk_client_pool();
        this.reset_mongo_sdk_client_pool();
    }

    private void abort_all_tasks() {
        if (TaskRequest.loader_tasks == null)
            return;
        for (String task_id : TaskRequest.loader_tasks.keySet()) {
            System.out.println("Aborting task '" + task_id + "'");
            TaskRequest.taskManager.abortTask(TaskRequest.loader_tasks.get(task_id));
        }
    }

    private void shutdown_taskmanager() {
        if (TaskRequest.taskManager == null)
            return;
        this.abort_all_tasks();
        System.out.println("Shutdown task manager");
        TaskRequest.taskManager.shutdown();
        this.reset_sdk_client_pool();
        this.reset_mongo_sdk_client_pool();
    }

    public ResponseEntity<Map<String, Object>> get_doc_keys() {
        Map<String, Object> body = new HashMap<>();
        boolean okay = this.validate_doc_load_params();
        if (!okay) {
            body.put("error", "Param validation failed");
            body.put("keys", null);
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }

        WorkLoadSettings ws = new WorkLoadSettings(
                this.keyPrefix, this.keySize, this.docSize,
                this.createPercent, this.readPercent, this.updatePercent,
                this.deletePercent, this.expiryPercent,
                this.processConcurrency, this.ops, null,
                this.keyType, this.valueType,
                this.validateDocs, this.gtm, this.validateDeletedDocs,
                this.mutate);

        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, this.createStartIndex);
        dr.put(DRConstants.create_e, this.createEndIndex);
        dr.put(DRConstants.read_s, this.createEndIndex);
        dr.put(DRConstants.read_e, this.readEndIndex);
        dr.put(DRConstants.update_s, this.updateStartIndex);
        dr.put(DRConstants.update_e, this.updateEndIndex);
        dr.put(DRConstants.delete_s, this.deleteStartIndex);
        dr.put(DRConstants.delete_e, this.deleteEndIndex);
        dr.put(DRConstants.touch_s, this.touchStartIndex);
        dr.put(DRConstants.touch_e, this.touchEndIndex);
        dr.put(DRConstants.replace_s, this.replaceStartIndex);
        dr.put(DRConstants.replace_e, this.replaceEndIndex);
        dr.put(DRConstants.expiry_s, this.expiryStartIndex);
        dr.put(DRConstants.expiry_e, this.expiryEndIndex);

        DocRange range = new DocRange(dr);
        DocumentGenerator dg = null;
        ws.dr = range;
        try {
            dg = new DocumentGenerator(ws, ws.keyType, ws.valueType, this.iterations,
                                       this.numVBuckets, this.targetVBuckets);
        } catch (ClassNotFoundException e) {
            // e.printStackTrace();
            body.put("error", "Failed to create doc generator");
            body.put("message", e.toString());
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }

        List<String> keys = new ArrayList<String>();
        List<String> docs = dg.nextDeleteBatch();
        while (docs.size() > 0) {
            keys.addAll(docs);
            docs = dg.nextDeleteBatch();
        }
        body.put("keys", keys);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> init_task_manager() {
        Map<String, Object> body = new HashMap<>();
        HttpStatus ret_status;
        if (TaskRequest.taskManager == null) {
            this.init_taskmanager();
            body.put("workers", String.valueOf(num_workers));
            ret_status = HttpStatus.OK;
        } else {
            body.put("workers", "0");
            body.put("error", "task_manager already initiated");
            ret_status = HttpStatus.NOT_ACCEPTABLE;
        }
        return new ResponseEntity<>(body, ret_status);
    }

    public ResponseEntity<Map<String, Object>> shutdown_task_manager() {
        Map<String, Object> body = new HashMap<>();
        this.shutdown_taskmanager();
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> reset_task_manager() {
        Map<String, Object> body = new HashMap<>();
        this.shutdown_taskmanager();
        this.init_taskmanager();
        TaskRequest.loader_tasks = new ConcurrentHashMap<String, WorkLoadGenerate>();
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> submit_task() {
        Map<String, Object> body = new HashMap<>();
        try {
            TaskRequest.taskManager.submit(TaskRequest.loader_tasks.get(this.taskName));
            TimeUnit.MILLISECONDS.sleep(200);
            body.put("status", true);
        } catch (Exception e) {
            body.put("status", false);
            body.put("error", e.toString());
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> submit_task_mongo() {
        Map<String, Object> body = new HashMap<>();
        try {
            TaskRequest.taskManager.submit(TaskRequest.mongo_loader_tasks.get(this.taskName));
            TimeUnit.MILLISECONDS.sleep(200);
            body.put("status", true);
        } catch (Exception e) {
            body.put("status", false);
            body.put("error", e.toString());
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> get_task_result_mongo() {
        Map<String, Object> body = new HashMap<>();
        try {
            mongo.loadgen.WorkLoadGenerate task = TaskRequest.mongo_loader_tasks.get(this.taskName);
        if (task != null) {
            boolean okay = TaskRequest.taskManager.getTaskResult(task);
            body.put("status", okay);
        } else {
            body.put("error", "Task " + this.taskName + " does not exists");
                body.put("status", false);
            }
            return new ResponseEntity<>(body, HttpStatus.OK);
        } catch (Exception e) {
            body.put("error", e.toString());
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> get_task_result() {
        Map<String, Object> body = new HashMap<>();
        WorkLoadGenerate task = TaskRequest.loader_tasks.get(this.taskName);
        if (task != null) {
            Map<String, Object> failures = new HashMap<>();
            boolean okay = TaskRequest.taskManager.getTaskResult(task);
            TaskRequest.loader_tasks.remove(this.taskName);
            for (HashMap.Entry<String, List<Result>> optype : task.failedMutations.entrySet()) {
                optype.getValue().forEach(
                        (failed_result) -> {
                            Map<String, Object> res_obj = new HashMap<String, Object>();
                            if (failed_result.document() == null) {
                                // Happens when op_type is 'delete'
                                res_obj.put("value", null);
                            } else {
                                res_obj.put("value", failed_result.document().toString());
                            }
                            res_obj.put("error", failed_result.err().toString());
                            res_obj.put("status", failed_result.status());
                            failures.put(failed_result.id(), res_obj);
                        });
            }
            body.put("fail", failures);
            body.put("status", okay);
        } else {
            body.put("error", "Task " + this.taskName + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> stop_task() {
        Map<String, Object> body = new HashMap<>();
        WorkLoadGenerate task = TaskRequest.loader_tasks.get(this.taskName);
        if (task != null) {
            task.stop_load();
            body.put("status", true);
        } else {
            body.put("error", "Task " + this.taskName + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> cancel_task() {
        Map<String, Object> body = new HashMap<>();
        Task task = TaskRequest.loader_tasks.get(this.taskName);
        if (task != null) {
            TaskRequest.taskManager.abortTask(task);
            TaskRequest.loader_tasks.remove(this.taskName);
            body.put("status", true);
        } else {
            body.put("error", "Task " + this.taskName + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> doc_load() {
        this.log_request();
        Map<String, Object> body = new HashMap<>();
        boolean okay = this.validate_doc_load_params();
        if (!okay) {
            body.put("error", "Param validation failed");
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }

        WorkLoadSettings ws = new WorkLoadSettings(
            this.keyPrefix, this.keySize, this.docSize,
            this.createPercent, this.readPercent, this.updatePercent,
            this.deletePercent, this.expiryPercent, this.subdocPercent,
            this.processConcurrency, this.ops, this.loadType,
            this.keyType, this.valueType,
            this.validateDocs, this.gtm, this.validateDeletedDocs,
            this.mutate, this.createPath, this.isSubdocXattr, this.isSubdocSysXattr,
            this.elastic, this.model, this.mockVector,
            this.dim, this.base64, this.mutateField,
            this.mutationTimeout);

        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, this.createStartIndex);
        dr.put(DRConstants.create_e, this.createEndIndex);
        dr.put(DRConstants.read_s, this.readStartIndex);
        dr.put(DRConstants.read_e, this.readEndIndex);
        dr.put(DRConstants.update_s, this.updateStartIndex);
        dr.put(DRConstants.update_e, this.updateEndIndex);
        dr.put(DRConstants.delete_s, this.deleteStartIndex);
        dr.put(DRConstants.delete_e, this.deleteEndIndex);
        dr.put(DRConstants.touch_s, this.touchStartIndex);
        dr.put(DRConstants.touch_e, this.touchEndIndex);
        dr.put(DRConstants.replace_s, this.replaceStartIndex);
        dr.put(DRConstants.replace_e, this.replaceEndIndex);
        dr.put(DRConstants.expiry_s, this.expiryStartIndex);
        dr.put(DRConstants.expiry_e, this.expiryEndIndex);
        // Subdoc related indexes
        dr.put(DRConstants.subdoc_insert_s, this.sdInsertStartIndex);
        dr.put(DRConstants.subdoc_insert_e, this.sdInsertEndIndex);
        dr.put(DRConstants.subdoc_upsert_s, this.sdUpsertStartIndex);
        dr.put(DRConstants.subdoc_upsert_e, this.sdUpsertEndIndex);
        dr.put(DRConstants.subdoc_remove_s, this.sdRemoveStartIndex);
        dr.put(DRConstants.subdoc_remove_e, this.sdRemoveEndIndex);
        dr.put(DRConstants.subdoc_read_s, this.sdReadStartIndex);
        dr.put(DRConstants.subdoc_read_e, this.sdReadEndIndex);

        DocRange range = new DocRange(dr);
        DocumentGenerator dg = null;

        ws.dr = range;
        try {
            dg = new DocumentGenerator(ws, ws.keyType, ws.valueType, this.iterations,
                                       this.numVBuckets, this.targetVBuckets);
        } catch (Exception e) {
            // e.printStackTrace();
            body.put("error", "Failed to create doc generator");
            body.put("message", e.toString());
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        EsClient esClient = null;
        if (ws.elastic) {
            if (this.esAPIKey != null) {
                esClient = new EsClient(this.esServer, this.esAPIKey);
                esClient.initializeSDK();
                esClient.deleteESIndex(this.collectionName.replace("_", ""));
                try {
                    esClient.createESIndex(this.collectionName.replace("_", ""),
                            this.esSimilarity, null);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        ArrayList<String> task_names = new ArrayList<String>();
        String task_name = "Task_" + TaskRequest.task_id.incrementAndGet();
        int retry = 0;
        for (int i = 0; i < ws.workers; i++) {
            String th_name = task_name + "_" + i;
            WorkLoadGenerate wlg = new WorkLoadGenerate(th_name, dg, TaskRequest.SDKClientPool, esClient,
                    this.durabilityLevel,
                    this.docTTL, this.docTTLUnit, this.trackFailures,
                    retry, null);
            wlg.set_collection_for_load(this.bucketName, this.scopeName, this.collectionName);
            TaskRequest.loader_tasks.put(th_name, wlg);

            task_names.add(th_name);
        }
        body.put("tasks", task_names);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> doc_load_mongo() {
        this.log_request();
        Map<String, Object> body = new HashMap<>();
        boolean okay = this.validate_doc_load_params();
        if (!okay) {
            body.put("error", "Param validation failed");
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        WorkLoadSettings ws = new WorkLoadSettings(
                this.keyPrefix,
                this.keySize,
                this.docSize,
                this.createPercent,
                this.readPercent,
                this.updatePercent,
                this.deletePercent,
                this.expiryPercent,
                this.processConcurrency,
                this.ops,
                this.loadType,
                this.keyType,
                this.valueType,
                false, false, false,
                this.mutate
                );
        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, this.createStartIndex);
        dr.put(DRConstants.create_e ,this.createEndIndex);
        dr.put(DRConstants.read_s ,this.readStartIndex);
        dr.put(DRConstants.read_e ,this.readEndIndex);
        dr.put(DRConstants.update_s ,this.updateStartIndex);
        dr.put(DRConstants.update_e ,this.updateEndIndex);
        dr.put(DRConstants.delete_s ,this.deleteStartIndex);
        dr.put(DRConstants.delete_e ,this.deleteEndIndex);
        dr.put(DRConstants.expiry_s ,this.expiryStartIndex);
        dr.put(DRConstants.expiry_e ,this.expiryEndIndex);

        DocRange range = new DocRange(dr);
        ws.dr = range;
        MongoDocumentGenerator dg = null;
        try {
            dg = new MongoDocumentGenerator(ws, ws.keyType, ws.valueType);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        ArrayList<String> task_names = new ArrayList<String>();
        this.mongoClients = new ArrayList<MongoSDKClient>();
        Server mongoServer = new Server(this.mongoServerIP, this.mongoServerPort, this.mongoUsername, this.mongoPassword, null);
        for (int i = 0; i < ws.workers; i++) {
            try {
                MongoSDKClient client = new MongoSDKClient(mongoServer,
                this.mongoBucketName,
                this.mongoCollectionName,
                this.mongoIsAtlas);
                client.connectCluster();
                this.mongoClients.add(client);
                String th_name = "Loader" + i;
                mongo.loadgen.WorkLoadGenerate task = new mongo.loadgen.WorkLoadGenerate(th_name, dg, client);
                TaskRequest.mongo_loader_tasks.put(th_name, task);
                task_names.add(th_name);
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        body.put("tasks", task_names);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> loadSIFTDataset() throws IOException {
        this.log_request();
        Map<String, Object> body = new HashMap<>();
        boolean okay = this.validate_doc_load_params();
        if (!okay) {
            body.put("error", "Param validation failed");
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        String siftFileName = Paths.get(this.baseVectorsFilePath, "bigann_base.bvecs").toString();
        // Check if the file exists
        FileDownload.checkDownload(this.baseVectorsFilePath, this.siftURL);

        EsClient esClient = null;
        if (this.elastic && this.esServer != null && this.esAPIKey != null) {
            esClient = new EsClient(this.esServer, this.esAPIKey);
            esClient.initializeSDK();
            esClient.deleteESIndex(this.collectionName.replace("_", ""));
            try {
                esClient.createESIndex(this.collectionName.replace("_", ""),
                        this.esSimilarity, null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int[] steps = new int[] { 0, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000, 100000000, 200000000,
                500000000, 1000000000 };
        int poolSize = this.processConcurrency;
        int start_offset = 0, end_offset = 0;
        if (this.createPercent > 0) {
            start_offset = this.createStartIndex;
            end_offset = this.createEndIndex;
        } else if (this.updatePercent > 0) {
            start_offset = this.updateStartIndex;
            end_offset = this.updateEndIndex;
        }

        ArrayList<String> task_names = new ArrayList<String>();
        int k = 0;
        while (!(steps[k] <= start_offset && start_offset < steps[k + 1]))
            k += 1;
        while (steps[k] < end_offset) {
            int start = Math.max(start_offset, steps[k]);
            int end = Math.min(end_offset, steps[k + 1]);
            int step = (end - start) / poolSize;
            for (int i = 0; i < poolSize; i++) {
                WorkLoadSettings ws = new WorkLoadSettings(this.keyPrefix,
                        this.keySize, this.docSize,
                        this.createPercent, this.readPercent,
                        this.updatePercent, this.deletePercent, this.expiryPercent, this.processConcurrency,
                        this.ops, this.loadType, this.keyType, "siftBigANN",
                        this.validateDocs, this.gtm, this.validateDeletedDocs, this.mutate,
                        this.elastic, this.model, this.mockVector,
                        this.dim, this.base64, this.mutateField,
                        this.mutationTimeout, siftFileName);

                HashMap<String, Number> dr = new HashMap<String, Number>();
                dr.put(DRConstants.create_s, start + step * i);
                dr.put(DRConstants.create_e, start + step * (i + 1));
                dr.put(DRConstants.read_s, this.readStartIndex);
                dr.put(DRConstants.read_e, this.readEndIndex);
                dr.put(DRConstants.update_s, start + step * i);
                dr.put(DRConstants.update_e, start + step * (i + 1));
                dr.put(DRConstants.delete_s, this.deleteStartIndex);
                dr.put(DRConstants.delete_e, this.deleteEndIndex);
                dr.put(DRConstants.touch_s, this.touchStartIndex);
                dr.put(DRConstants.touch_e, this.touchEndIndex);
                dr.put(DRConstants.replace_s, this.replaceStartIndex);
                dr.put(DRConstants.replace_e, this.replaceEndIndex);
                dr.put(DRConstants.expiry_s, this.expiryStartIndex);
                dr.put(DRConstants.expiry_e, this.expiryEndIndex);

                DocRange range = new DocRange(dr);
                DocumentGenerator dg = null;

                ws.dr = range;
                try {
                    dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
                } catch (Exception e) {
                    // e.printStackTrace();
                    body.put("error", "Failed to create doc generator");
                    body.put("message", e.toString());
                    return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
                }

                String task_name = "Task_" + TaskRequest.task_id.incrementAndGet() + k + "_" + ws.dr.create_s + "_"
                        + ws.dr.create_e;
                int retry = 0;
                String th_name = task_name + "_" + i;
                WorkLoadGenerate wlg = new WorkLoadGenerate(th_name, dg, TaskRequest.SDKClientPool, esClient,
                        this.durabilityLevel,
                        this.docTTL, this.docTTLUnit, this.trackFailures, retry, null);
                wlg.set_collection_for_load(this.bucketName, this.scopeName, this.collectionName);
                TaskRequest.loader_tasks.put(th_name, wlg);
                task_names.add(th_name);
            }
            k += 1;
        }

        body.put("tasks", task_names);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> check_sift_file() throws IOException {
        FileDownload.checkDownload(this.baseVectorsFilePath, this.siftURL);
        String siftFileName = Paths.get(this.baseVectorsFilePath, "bigann_base.bvecs").toString();
        Map<String, Object> body = new HashMap<>();
        body.put("file_path", siftFileName);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }
}
