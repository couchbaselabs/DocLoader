package couchbase.rest_server;

import couchbase.test.docgen.DRConstants;
import couchbase.test.docgen.DocRange;
import couchbase.test.docgen.DocumentGenerator;
import couchbase.test.docgen.WorkLoadSettings;
import couchbase.test.loadgen.WorkLoadGenerate;
import couchbase.test.sdk.SDKClient;
import couchbase.test.sdk.SDKClientPool;
import couchbase.test.sdk.Server;
import couchbase.test.taskmanager.TaskManager;
import couchbase.test.taskmanager.Task;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskRequest {
    static TaskManager task_manager;
    static AtomicInteger task_id = new AtomicInteger();
    static SDKClientPool sdk_client_pool = new SDKClientPool();
    static ArrayList known_servers = new ArrayList<Server>();
    static Object lock_obj = new Object();
    static private ConcurrentHashMap<String, WorkLoadGenerate> loader_tasks = new ConcurrentHashMap<String, WorkLoadGenerate>();

    // Consumed by init_task_manager()
    @JsonProperty("num_workers")
    private int num_workers;

    // Consumed by doc_load()
    // Connection params
    @JsonProperty("server_ip")
    private String server_ip;
    @JsonProperty("server_port")
    private String server_port;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("bucket_name")
    private String bucket_name;
    @JsonProperty("scope_name")
    private String scope_name;
    @JsonProperty("collection_name")
    private String collection_name;
    // Create params
    @JsonProperty("create_percent")
    private int create_percent;
    @JsonProperty("create_start")
    private int create_start_index;
    @JsonProperty("create_end")
    private int create_end_index;
    // Delete params
    @JsonProperty("delete_percent")
    private int delete_percent;
    @JsonProperty("delete_start")
    private int delete_start_index;
    @JsonProperty("delete_end")
    private int delete_end_index;
    // Update params
    @JsonProperty("update_percent")
    private int update_percent;
    @JsonProperty("update_start")
    private int update_start_index;
    @JsonProperty("update_end")
    private int update_end_index;
    // Read params
    @JsonProperty("read_percent")
    private int read_percent;
    @JsonProperty("read_start")
    private int read_start_index;
    @JsonProperty("read_end")
    private int read_end_index;
    // Touch params
    @JsonProperty("touch_start")
    private int touch_start_index;
    @JsonProperty("touch_end")
    private int touch_end_index;
    // Replace params
    @JsonProperty("replace_start")
    private int replace_start_index;
    @JsonProperty("replace_end")
    private int replace_end_index;
    // Expiry params
    @JsonProperty("expiry_percent")
    private int expiry_percent;
    @JsonProperty("expiry_start")
    private int expiry_start_index;
    @JsonProperty("expiry_end")
    private int expiry_end_index;
    // Document related params
    @JsonProperty("key_prefix")
    private String key_prefix;
    @JsonProperty("key_size")
    private int key_size;
    @JsonProperty("doc_size")
    private int doc_size;
    @JsonProperty("key_type")
    private String key_type;
    @JsonProperty("value_type")
    private String value_type;
    // Load properties
    @JsonProperty("timeout")
    private int timeout;
    @JsonProperty("timeout_unit")
    private String timeout_unit;
    @JsonProperty("doc_ttl")
    private int doc_ttl;
    @JsonProperty("doc_ttl_unit")
    private String doc_ttl_unit;
    @JsonProperty("durability_level")
    private String durability_level;
    @JsonProperty("ops")
    private int ops;
    @JsonProperty("process_concurrency")
    private int process_concurrency;
    @JsonProperty("iterations")
    private int iterations;

    // Used by get_task_result(), stop_task(), cancel_task()
    @JsonProperty("task_id")
    private String task_name;
    /*
    Following params are yet to be implemented.
     -deleted,--deleted <arg>       To verify deleted docs
     -gtm,--gtm <arg>               Go for max doc ops
     -loadType,--loadType <arg>     Hot/Cold
     -transaction_patterns <arg>    Transaction load pattern
     -validate,--validate <arg>     Validate Data during Reads
     -valueType,--valueType <arg>
    */

    private boolean validate_doc_load_params() {
        if ((this.server_ip == null)
               || (this.username == null)
               || (this.password == null)
               || (this.bucket_name == null)  )
            return false;

        if (this.create_percent
                + this.update_percent
                + this.read_percent
                + this.delete_percent
                + this.expiry_percent != 100)
            return false;

        // Set default values if null
        if (this.scope_name == null)
            this.scope_name = "_default";
        if (this.collection_name == null)
            this.collection_name = "_default";
        if (this.durability_level == null)
            this.durability_level = "NONE";
        if (this.key_prefix == null)
            this.key_prefix = "test_doc-";
        if (this.key_size == 0)
            this.key_size = 20;
        if (this.doc_size == 0)
            this.doc_size = 256;
        if (this.key_type == null)
            this.key_type = "SimpleKey";
        if (this.value_type == null)
            this.value_type = "SimpleValue";
        if (this.doc_ttl_unit == null)
            this.doc_ttl_unit = "seconds";
        if (this.timeout_unit == null)
            this.timeout_unit = "seconds";
        if (this.process_concurrency == 0)
            this.process_concurrency = 1;
        return true;
    }

    // Generate getters and setters for all parameters
    public static TaskRequest fromJson(String json) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, TaskRequest.class);
    }

    public ResponseEntity<Map<String, Object>> init_task_manager() {
        Map<String, Object> body = new HashMap<>();
        HttpStatus ret_status;
        if (this.task_manager == null) {
            this.task_manager = new TaskManager(num_workers);
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
        this.task_manager.shutdown();
        Map<String, Object> body = new HashMap<>();
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> get_task_result() {
        Map<String, Object> body = new HashMap<>();
        WorkLoadGenerate task = this.loader_tasks.get(this.task_name);
        if (task != null) {
            boolean okay = this.task_manager.getTaskResult(task);
            this.loader_tasks.remove(this.task_name);
            body.put("fail", task.failedMutations);
            body.put("status", okay);
        } else {
            body.put("error", "Task " + this.task_name + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> stop_task() {
        Map<String, Object> body = new HashMap<>();
        WorkLoadGenerate task = this.loader_tasks.get(this.task_name);
        if (task != null) {
            task.stop_load();
            body.put("status", true);
        }
        else {
            body.put("error", "Task " + this.task_name + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> cancel_task() {
        Map<String, Object> body = new HashMap<>();
        Task task = this.loader_tasks.get(this.task_name);
        if (task != null) {
            this.task_manager.abortTask(task);
            this.loader_tasks.remove(this.task_name);
            body.put("status", true);
        } else {
            body.put("error", "Task " + this.task_name + " does not exists");
            body.put("status", false);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> doc_load() {
        Map<String, Object> body = new HashMap<>();
        boolean okay = this.validate_doc_load_params();
        if (! okay) {
            body.put("error", "Param validation failed");
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }

        Server master = new Server(this.server_ip, this.server_port,
                                   this.username, this.password,
                                   this.server_port);

        boolean validate = false;
        boolean gtm = false;
        boolean deleted = false;
        int mutate = 0;

        WorkLoadSettings ws = new WorkLoadSettings(
            this.key_prefix, this.key_size, this.doc_size,
            this.create_percent, this.read_percent, this.update_percent,
            this.delete_percent, this.expiry_percent,
            this.process_concurrency, this.ops, null,
            this.key_type, this.value_type,
            validate, gtm, deleted, mutate);

        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, this.create_start_index);
        dr.put(DRConstants.create_e, this.create_end_index);
        dr.put(DRConstants.read_s, this.read_start_index);
        dr.put(DRConstants.read_e, this.read_end_index);
        dr.put(DRConstants.update_s, this.update_start_index);
        dr.put(DRConstants.update_e, this.update_end_index);
        dr.put(DRConstants.delete_s, this.delete_start_index);
        dr.put(DRConstants.delete_e, this.delete_end_index);
        dr.put(DRConstants.touch_s, this.touch_start_index);
        dr.put(DRConstants.touch_e, this.touch_end_index);
        dr.put(DRConstants.replace_s, this.replace_start_index);
        dr.put(DRConstants.replace_e, this.replace_end_index);
        dr.put(DRConstants.expiry_s, this.expiry_start_index);
        dr.put(DRConstants.expiry_e, this.expiry_end_index);

        SDKClient client;
        DocRange range = new DocRange(dr);
        DocumentGenerator dg = null;

        ws.dr = range;
        try {
            dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
        } catch (ClassNotFoundException e) {
            // e.printStackTrace();
            body.put("error", "Failed to create doc generator");
            body.put("message", e.toString());
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        try {
            client = new SDKClient(master, this.bucket_name, this.scope_name, this.collection_name);
            client.initialiseSDK();
        } catch (Exception e) {
            // e.printStackTrace();
            body.put("error", "Failed to initialize SDKClient");
            body.put("message", e.toString());
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        boolean trackFailures = true;
        ArrayList task_names = new ArrayList<String>();
        String task_name = "Task_" + this.task_id.incrementAndGet();
        int retry = 0;
        for (int i = 0; i < ws.workers; i++) {
            try {
                String th_name = task_name + "_" + i;
                this.loader_tasks.put(th_name, new WorkLoadGenerate(
                    th_name, dg, client, null, this.durability_level,
                    this.doc_ttl, this.doc_ttl_unit, trackFailures, retry, null));
                this.task_manager.submit(this.loader_tasks.get(th_name));

                TimeUnit.MILLISECONDS.sleep(500);
                task_names.add(th_name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        body.put("tasks", task_names);
        body.put("status", true);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    public ResponseEntity<Map<String, Object>> subdoc_load() {
        Map<String, Object> body = new HashMap<>();
        body.put("status", true);
        boolean okay = this.validate_doc_load_params();
        if (! okay) {
            body.put("error", "Param validation failed");
            return new ResponseEntity<>(body, HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(body, HttpStatus.OK);
    }
}
