package couchbase.loadgen;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.PropertyAccessor;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;

import couchbase.sdk.DocOps;
import couchbase.sdk.Result;
import couchbase.sdk.SDKClient;
import couchbase.sdk.SDKClientPool;
import couchbase.sdk.SubDocOps;
import elasticsearch.EsClient;
import reactor.util.function.Tuple2;
import utils.docgen.DocumentGenerator;
import utils.taskmanager.Task;

public class WorkLoadGenerate extends Task{
    DocumentGenerator dg;
    public SDKClient sdk;
    public DocOps docops;
    public SubDocOps subDocOps;
    public String durability;
    public HashMap<String, List<Result>> failedMutations = new HashMap<String, List<Result>>();
    public boolean trackFailures = true;
    public int retryTimes = 0;
    public int exp;
    public String exp_unit;
    public String retryStrategy;
    public UpsertOptions upsertOptions;
    public InsertOptions expiryOptions;
    public InsertOptions setOptions;
    public RemoveOptions removeOptions;
    public GetOptions getOptions;
    public MutateInOptions mutateInOptions;
    public LookupInOptions lookupInOptions;
    public EsClient esClient = null;
    private SDKClientPool sdkClientPool;
    static Logger logger = LogManager.getLogger(WorkLoadGenerate.class);
    public boolean stop_load = false;
    public String bucket_name;
    public String scope = "_default";
    public String collection = "_default";

    private void update_subdoc_failed_mutation_result(
            String op_type,
            HashMap<String, List<Result>> failed_mutations,
            List<HashMap<String,Object>> sd_results) {
        if(!trackFailures)
            return;
        List<Result> result_arr;
        if(!failedMutations.containsKey(op_type)) {
            result_arr = new ArrayList<Result>();
            failedMutations.put(op_type, result_arr);
        } else {
            result_arr = failedMutations.get(op_type);
        }

        for(HashMap<String, Object> sd_res : sd_results) {
            if (!(boolean)sd_res.get("status")) {
                result_arr.add(new Result((String)sd_res.get("id"),
                                          sd_res.get("value"),
                                          (Throwable)sd_res.get("error"),
                                          false));
            }
        }
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.subDocOps = new SubDocOps();
        this.sdk = client;
        this.durability = durability;
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.subDocOps = new SubDocOps();
        this.sdk = client;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability,
            int exp, String exp_unit, boolean trackFailures, int retryTimes, String retryStrategy) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.subDocOps = new SubDocOps();
        this.sdk = client;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, EsClient esClient,
            String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes, String retryStrategy) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.subDocOps = new SubDocOps();
        this.sdk = client;
        this.esClient = esClient;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClientPool clientPool, EsClient esClient,
            String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes, String retryStrategy) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.subDocOps = new SubDocOps();
        this.sdkClientPool = clientPool;
        this.esClient = esClient;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
    }

    public void stop_load() {
        this.stop_load = true;
    }

    public void set_collection_for_load(String bucket_name, String scope, String collection) {
        this.bucket_name = bucket_name;
        this.scope = scope;
        this.collection = collection;
    }

    public void actual_run() {
        this.result = true;
        logger.info("Starting " + this.taskName);
        // Set timeout in WorkLoadSettings
        this.dg.ws.setTimeoutDuration(60, "seconds");
        // Set Durability in WorkLoadSettings
        this.dg.ws.setDurabilityLevel(this.durability);
        this.dg.ws.setRetryStrategy(this.retryStrategy);

        // When DurabilityLevel.NONE is explicitly set, the SDK sends durability_level=0
        // in the KV protocol frame, which tells the server "client explicitly wants no
        // durability" and may override the bucket-level durability setting.
        // To let the server enforce bucket-level durability, we must NOT call
        // .durability() when the level is NONE - this omits the durability field
        // from the request entirely, allowing the server to apply its own level.
        boolean useClientDurability = this.dg.ws.durability != null
                && this.dg.ws.durability != DurabilityLevel.NONE;

        UpsertOptions upsertOpts = UpsertOptions.upsertOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        if (useClientDurability) upsertOpts = upsertOpts.durability(this.dg.ws.durability);
        upsertOptions = upsertOpts;

        InsertOptions expiryOpts = InsertOptions.insertOptions()
                .timeout(this.dg.ws.timeout)
                .expiry(this.dg.ws.getDuration(this.exp, this.exp_unit))
                .retryStrategy(this.dg.ws.retryStrategy);
        if (useClientDurability) expiryOpts = expiryOpts.durability(this.dg.ws.durability);
        expiryOptions = expiryOpts;

        InsertOptions setOpts = InsertOptions.insertOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        if (useClientDurability) setOpts = setOpts.durability(this.dg.ws.durability);
        setOptions = setOpts;

        RemoveOptions removeOpts = RemoveOptions.removeOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        if (useClientDurability) removeOpts = removeOpts.durability(this.dg.ws.durability);
        removeOptions = removeOpts;

        getOptions = GetOptions.getOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);

        MutateInOptions mutateOpts = MutateInOptions.mutateInOptions()
                .expiry(this.dg.ws.getDuration(this.exp, this.exp_unit))
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        if (useClientDurability) mutateOpts = mutateOpts.durability(this.dg.ws.durability);
        mutateInOptions = mutateOpts;
        lookupInOptions = LookupInOptions.lookupInOptions();

        if(dg.ws.expiry == 0) {
            // If expiry load is not set and we have exp value set,
            // then apply it for inserts and upserts
            setOptions = setOptions.expiry(this.dg.ws.getDuration(this.exp, this.exp_unit));
            upsertOptions = upsertOptions.expiry(this.dg.ws.getDuration(this.exp, this.exp_unit));
        }

        int ops = 0;
        long createDocsWritten = 0;
        boolean flag = false;
        Instant trackFailureTime_start = Instant.now();
        while(! this.stop_load) {
            Instant trackFailureTime_end = Instant.now();
            Duration timeElapsed = Duration.between(trackFailureTime_start, trackFailureTime_end);
            if(timeElapsed.toMinutes() > 5) {
                for (Entry<String, List<Result>> optype: failedMutations.entrySet())
                    System.out.println("Failed mutations count so far: " + optype.getKey() + " == " + optype.getValue().size());
                trackFailureTime_start = Instant.now();
            }
            Instant start = Instant.now();
            if(dg.ws.creates > 0) {
                // Instant st = Instant.now();
                List<Tuple2<String, Object>> docs = dg.nextInsertBatch();
                // Instant en = Instant.now();
                // System.out.println(this.taskName + " Time Taken to generate " + docs.size() + "docs: " + Duration.between(st, en).toMillis() + "ms");
                if (docs.size()>0) {
                    flag = true;
                    if(this.dg.ws.elastic) {
                        this.esClient.insertDocs(this.collection.replace("_", ""), docs);
                    }
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkInsert(this.sdk.connection, docs, setOptions);
                    createDocsWritten += docs.size();
                    ops += dg.ws.batchSize*dg.ws.creates/100;
                    if(this.trackFailures && result.size()>0){
                        this.result = false;
                        logger.warn("[CREATE_FAIL] task=" + this.taskName
                                + " collection=" + this.collection
                                + " batch_sent=" + docs.size()
                                + " failed=" + result.size()
                                + " first_failed_key=" + result.get(0).id()
                                + " error=" + result.get(0).err().getClass().getSimpleName());
                        try {
                            failedMutations.get("create").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("create", result);
                        }
                    }
                }
            }
            if(dg.ws.updates > 0) {
                List<Tuple2<String, Object>> docs = dg.nextUpdateBatch();
                if (docs.size()>0) {
                    flag = true;
                    if(this.dg.ws.elastic) {
                        this.esClient.insertDocs(this.collection.replace("_", ""), docs);
                    }
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkUpsert(this.sdk.connection, docs, upsertOptions);
                    ops += dg.ws.batchSize*dg.ws.updates/100;
                    if(this.trackFailures && result.size()>0){
                        this.result = false;
                        try {
                            failedMutations.get("update").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("update", result);
                        }
                    }
                }
            }
            if(dg.ws.expiry > 0) {
                List<Tuple2<String, Object>> docs = dg.nextExpiryBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkInsert(this.sdk.connection, docs, expiryOptions);
                    ops += dg.ws.batchSize*dg.ws.expiry/100;
                    if(this.trackFailures && result.size()>0){
                        this.result = false;
                        try {
                            failedMutations.get("expiry").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("expiry", result);
                        }
                    }
                }
            }
            if(dg.ws.deletes > 0) {
                List<String> docs = dg.nextDeleteBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkDelete(this.sdk.connection, docs, removeOptions);
                    if(this.dg.ws.elastic) {
                        this.esClient.deleteDocs(this.collection.replace("_", ""), docs);
                    }
                    ops += dg.ws.batchSize*dg.ws.deletes/100;
                    if(this.trackFailures && result.size()>0){
                        this.result = false;
                        try {
                            failedMutations.get("delete").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("delete", result);
                        }
                    }
                }
            }
            if(dg.ws.reads > 0) {
                List<Tuple2<String, Object>> docs = dg.nextReadBatch();
                if (docs.size()>0) {
                    flag = true;
                    List<Tuple2<String, Object>> res = docops.bulkGets(this.sdk.connection, docs, getOptions);
                    if (this.dg.ws.validate) {
                        Map<Object, Object> trnx_res = res.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        Map<Object, Object> trnx_docs = docs.stream().collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)));
                        ObjectMapper om = new ObjectMapper();
                        om.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
                        for (Object name : trnx_docs.keySet()) {
                            try {
                                String a = om.writeValueAsString(trnx_res.get(name));
                                String b = om.writeValueAsString(trnx_docs.get(name));
                                if(this.dg.ws.expectDeleted) {
                                    if(!a.contains(DocumentNotFoundException.class.getSimpleName())) {
                                        System.out.println("Validation failed for key: " + this.sdk.scope + ":" + this.sdk.collection + ":" + name);
                                        System.out.println("Actual Value - " + a);
                                        System.out.println("Expected Value - " + b);
                                        System.out.println(this.taskName + " is completed!");
                                        return;
                                    }
                                } else if(!a.equals(b) && !a.contains("TimeoutException")){
                                    System.out.println("Validation failed for key: " + this.sdk.scope + ":" + this.sdk.collection + ":" + name);
                                    System.out.println("Actual Value - " + a);
                                    System.out.println("Expected Value - " + b);
                                    System.out.println(this.taskName + " is completed!");
                                    return;
                                }
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    ops += dg.ws.batchSize*dg.ws.reads/100;
                }
            }
            if(dg.ws.subdocs> 0) {
                List<Tuple2<String,List<MutateInSpec>>> docs;

                docs = dg.nextSubDocBatch("insert");
                if (docs.size()>0) {
                    flag = true;
                    List<HashMap<String,Object>> result = subDocOps.bulkSubDocOperation(this.sdk.connection, docs, mutateInOptions);
                    ops += dg.ws.batchSize*dg.ws.subdocs/100;
                    this.update_subdoc_failed_mutation_result("insert", failedMutations, result);
                }

                docs = dg.nextSubDocBatch("upsert");
                if (docs.size()>0) {
                    flag = true;
                    List<HashMap<String,Object>> result = subDocOps.bulkSubDocOperation(this.sdk.connection, docs, mutateInOptions);
                    ops += dg.ws.batchSize*dg.ws.subdocs/100;
                    this.update_subdoc_failed_mutation_result("upsert", failedMutations, result);
                }

                List<Tuple2<String,List<LookupInSpec>>> lookup_docs = dg.nextSubDocLookupBatch();
                if (lookup_docs.size()>0) {
                    flag = true;
                    List<HashMap<String,Object>> result = subDocOps.bulkGetSubDocOperation(this.sdk.connection, lookup_docs, lookupInOptions);
                    ops += dg.ws.batchSize*dg.ws.subdocs/100;
                    this.update_subdoc_failed_mutation_result("lookup", failedMutations, result);
                }

                docs = dg.nextSubDocBatch("remove");
                if (docs.size()>0) {
                    flag = true;
                    List<HashMap<String,Object>> result = subDocOps.bulkSubDocOperation(this.sdk.connection, docs, mutateInOptions);
                    ops += dg.ws.batchSize*dg.ws.subdocs/100;
                    this.update_subdoc_failed_mutation_result("remove", failedMutations, result);
                }
            }
            if(ops == 0)
                break;
            else if(ops < dg.ws.ops/dg.ws.workers && flag) {
                flag = false;
                continue;
            }
            ops = 0;
            Instant end = Instant.now();
            timeElapsed = Duration.between(start, end);
            // Throttle to maintain 1-second pacing
            if(!this.dg.ws.gtm && timeElapsed.toMillis() < 1000) {
                try {
                    long i =  (long) ((1000-timeElapsed.toMillis()));
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        logger.info(this.taskName + " is completed!");
        long retryDocsInserted = 0;
        if (retryTimes > 0 && failedMutations.size() > 0) {
            for (Entry<String, List<Result>> optype: failedMutations.entrySet()) {
                logger.warn("[RETRY_START] task=" + this.taskName
                        + " collection=" + this.collection
                        + " op=" + optype.getKey()
                        + " count=" + optype.getValue().size());
            }
            this.result = true;
            for (Entry<String, List<Result>> optype: failedMutations.entrySet()) {
                for (Result r: optype.getValue()) {
                    logger.warn("[RETRY_ATTEMPT] task=" + this.taskName
                            + " op=" + optype.getKey()
                            + " key=" + r.id()
                            + " original_error=" + r.err().getClass().getSimpleName());
                    switch(optype.getKey()) {
                    case "create":
                        try {
                            docops.insert(r.id(), r.document(), this.sdk.connection, setOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                            retryDocsInserted++;
                            logger.info("[RETRY_SUCCESS] op=create key=" + r.id());
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.error("[RETRY_FAIL] op=create key=" + r.id() + " error=" + e.getClass().getSimpleName());
                            this.result = false;
                        } catch (DocumentExistsException e) {
                            // Doc already in bucket from original attempt — not an extra doc
                            logger.warn("[RETRY_EXISTS] op=create key=" + r.id() + " — doc already exists, original write succeeded");
                        } catch (Exception e) {
                            logger.error("[RETRY_FAIL] op=create key=" + r.id() + " error=" + e.toString());
                            this.result = false;
                        }
                        // WARNING: missing break — falls through to 'update' case below
                        logger.warn("[RETRY_FALLTHROUGH] op=create falling through to update for key=" + r.id()
                                + " — this is a switch bug; create will also be upserted");
                    case "update":
                        try {
                            docops.upsert(r.id(), r.document(), this.sdk.connection, upsertOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                            logger.info("[RETRY_SUCCESS] op=update key=" + r.id());
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.error("[RETRY_FAIL] op=update key=" + r.id() + " error=" + e.getClass().getSimpleName());
                            this.result = false;
                        }  catch (DocumentExistsException e) {
                            logger.error("[RETRY_FAIL] op=update key=" + r.id() + " error=" + e.getClass().getSimpleName());
                            this.result = false;
                        } catch (Exception e) {
                            logger.error("[RETRY_FAIL] op=update key=" + r.id() + " error=" + e.toString());
                            this.result = false;
                        }
                        // WARNING: missing break — falls through to 'delete' case below
                        logger.warn("[RETRY_FALLTHROUGH] op=update falling through to delete for key=" + r.id()
                                + " — this is a switch bug; doc will also be deleted");
                    case "delete":
                        try {
                            docops.delete(r.id(), this.sdk.connection, removeOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                            logger.info("[RETRY_SUCCESS] op=delete key=" + r.id());
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            logger.error("[RETRY_FAIL] op=delete key=" + r.id() + " error=" + e.getClass().getSimpleName());
                            this.result = false;
                        } catch (DocumentNotFoundException e) {
                            logger.error("[RETRY_FAIL] op=delete key=" + r.id() + " error=DocumentNotFoundException");
                            this.result = false;
                        }
                    }
                }
            }
        }
        logger.info("[LOAD_COMPLETE] task=" + this.taskName
                + " bucket=" + this.bucket_name
                + " scope=" + this.scope
                + " collection=" + this.collection
                + " create_docs_written=" + createDocsWritten
                + " retry_docs_inserted=" + retryDocsInserted
                + " total=" + (createDocsWritten + retryDocsInserted)
                + " final_createItr=" + this.dg.ws.dr.createItr.get()
                + " create_s=" + this.dg.ws.dr.create_s
                + " create_e=" + this.dg.ws.dr.create_e
                + " expected=" + (this.dg.ws.dr.create_e - this.dg.ws.dr.create_s));
    }

    @Override
    public void run() {
        if (this.sdkClientPool != null) {
            try {
                // Pool blocks internally until a client is available (up to its configured timeout).
                // No busy-poll needed here — blocking in the pool is cheaper and has no retry cap.
                this.sdk = this.sdkClientPool.get_client_for_bucket(
                        this.bucket_name, this.scope, this.collection);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for SDK client for bucket "
                        + this.bucket_name, e);
                Thread.currentThread().interrupt();
                this.result = false;
                return;
            } catch (Exception e) {
                logger.error("Error acquiring SDK client for bucket "
                        + this.bucket_name + ": " + e.getMessage(), e);
            }
            if (this.sdk == null) {
                logger.error("Failed to acquire SDK client for bucket " + this.bucket_name);
                this.result = false;
                return;
            }
        }
        try {
            this.actual_run();
        }
        catch (Exception e) {
            logger.error("Unhandled exception in task " + this.taskName + ": " + e.getMessage(), e);
            this.result = false;
        }
        finally{
            if (this.sdkClientPool != null && this.sdk != null)
                this.sdkClientPool.release_client(this.sdk);
        }
    }
}
