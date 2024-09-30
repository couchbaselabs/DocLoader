package couchbase.test.loadgen;

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
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ServerOutOfMemoryException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import couchbase.test.docgen.DocumentGenerator;
import couchbase.test.sdk.DocOps;
import couchbase.test.sdk.Result;
import couchbase.test.sdk.SDKClient;
import couchbase.test.sdk.SDKClientPool;
import couchbase.test.taskmanager.Task;
import elasticsearch.EsClient;
import reactor.util.function.Tuple2;

public class WorkLoadGenerate extends Task{
    DocumentGenerator dg;
    public SDKClient sdk = null;
    public DocOps docops;
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
    public EsClient esClient = null;
    private SDKClientPool sdkClientPool;
    static Logger logger = LogManager.getLogger(WorkLoadGenerate.class);
    public String bucket_name;
    public String scope = "_default";
    public String collection = "_default";

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
        this.sdk = client;
        this.durability = durability;
    }

    public WorkLoadGenerate(String taskName, DocumentGenerator dg, SDKClient client, String durability, int exp, String exp_unit, boolean trackFailures, int retryTimes) {
        super(taskName);
        this.dg = dg;
        this.docops = new DocOps();
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
        this.sdkClientPool = clientPool;
        this.esClient = esClient;
        this.durability = durability;
        this.trackFailures = trackFailures;
        this.retryTimes = retryTimes;
        this.exp = exp;
        this.exp_unit = exp_unit;
        this.retryStrategy = retryStrategy;
    }

    public void set_collection_for_load(String bucket_name, String scope, String collection) {
        this.bucket_name = bucket_name;
        this.scope = scope;
        this.collection = collection;
    }

    @Override
    public void run() {
        logger.info("Starting " + this.taskName);
        // Set timeout in WorkLoadSettings
        this.dg.ws.setTimeoutDuration(60, "seconds");
        // Set Durability in WorkLoadSettings
        this.dg.ws.setDurabilityLevel(this.durability);
        this.dg.ws.setRetryStrategy(this.retryStrategy);

        upsertOptions = UpsertOptions.upsertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        expiryOptions = InsertOptions.insertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .expiry(this.dg.ws.getDuration(this.exp, this.exp_unit))
                .retryStrategy(this.dg.ws.retryStrategy);
        setOptions = InsertOptions.insertOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        removeOptions = RemoveOptions.removeOptions()
                .timeout(this.dg.ws.timeout)
                .durability(this.dg.ws.durability)
                .retryStrategy(this.dg.ws.retryStrategy);
        getOptions = GetOptions.getOptions()
                .timeout(this.dg.ws.timeout)
                .retryStrategy(this.dg.ws.retryStrategy);
        int ops = 0;
        boolean flag = false;
        Instant trackFailureTime_start = Instant.now();
        while(true) {
            if (this.sdkClientPool != null)
                this.sdk = this.sdkClientPool.get_client_for_bucket(this.bucket_name, this.scope, this.collection);
            Instant trackFailureTime_end = Instant.now();
            Duration timeElapsed = Duration.between(trackFailureTime_start, trackFailureTime_end);
            if(timeElapsed.toMinutes() > 5) {
                for (Entry<String, List<Result>> optype: failedMutations.entrySet())
                    System.out.println("Failed mutations count so far: " + optype.getKey() + " == " + optype.getValue().size());
                trackFailureTime_start = Instant.now();
            }
            Instant start = Instant.now();
            if(dg.ws.creates > 0) {
                Instant st = Instant.now();
                List<Tuple2<String, Object>> docs = dg.nextInsertBatch();
              Instant en = Instant.now();
              System.out.println(this.taskName + " Time Taken to generate " + docs.size() + "docs: " + Duration.between(st, en).toMillis() + "ms");
                if (docs.size()>0) {
                    flag = true;
                    if(this.dg.ws.elastic) {
                        this.esClient.insertDocs(this.esClient.indexName.replace("_", ""), docs);
                    }
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkInsert(this.sdk.connection, docs, setOptions);
                    ops += dg.ws.batchSize*dg.ws.creates/100;
                    if(trackFailures && result.size()>0)
                        try {
                            failedMutations.get("create").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("create", result);
                        }
                }
            }
            if(dg.ws.updates > 0) {
                List<Tuple2<String, Object>> docs = dg.nextUpdateBatch();
                if (docs.size()>0) {
                    flag = true;
                    if(this.dg.ws.elastic) {
                        this.esClient.insertDocs(this.esClient.indexName.replace("_", ""), docs);
                    }
                    List<Result> result = new ArrayList<Result>();
                    if(this.sdk != null)
                        result = docops.bulkUpsert(this.sdk.connection, docs, upsertOptions);
                    ops += dg.ws.batchSize*dg.ws.updates/100;
                    if(trackFailures && result.size()>0)
                        try {
                            failedMutations.get("update").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("update", result);
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
                    if(trackFailures && result.size()>0)
                        try {
                            failedMutations.get("expiry").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("expiry", result);
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
                        this.esClient.deleteDocs(this.esClient.indexName.replace("_", ""), docs);
                    }
                    ops += dg.ws.batchSize*dg.ws.deletes/100;
                    if(trackFailures && result.size()>0)
                        try {
                            failedMutations.get("delete").addAll(result);
                        } catch (Exception e) {
                            failedMutations.put("delete", result);
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
                                        this.sdk.disconnectCluster();
                                        System.out.println(this.taskName + " is completed!");
                                        return;
                                    }
                                } else if(!a.equals(b) && !a.contains("TimeoutException")){
                                    System.out.println("Validation failed for key: " + this.sdk.scope + ":" + this.sdk.collection + ":" + name);
                                    System.out.println("Actual Value - " + a);
                                    System.out.println("Expected Value - " + b);
                                    this.sdk.disconnectCluster();
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
            if(ops == 0)
                break;
            else if(ops < dg.ws.ops/dg.ws.workers && flag) {
                flag = false;
                continue;
            }
            ops = 0;
            Instant end = Instant.now();
            timeElapsed = Duration.between(start, end);
            if(!this.dg.ws.gtm && timeElapsed.toMillis() < 1000)
                try {
                    long i =  (long) ((1000-timeElapsed.toMillis()));
                    TimeUnit.MILLISECONDS.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
        logger.info(this.taskName + " is completed!");
        this.result = true;
        if (retryTimes > 0 && failedMutations.size() > 0)
            for (Entry<String, List<Result>> optype: failedMutations.entrySet()) {
                for (Result r: optype.getValue()) {
                    System.out.println("Loader Retrying: " + r.id() + " -> " + r.err().getClass().getSimpleName());
                    switch(optype.getKey()) {
                    case "create":
                        try {
                            docops.insert(r.id(), r.document(), this.sdk.connection, setOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            System.out.println("Retry Create failed for key: " + r.id());
                            this.result = false;
                        } catch (DocumentExistsException e) {
                            System.out.println("Retry Create failed for key: " + r.id());
                        }
                    case "update":
                        try {
                            docops.upsert(r.id(), r.document(), this.sdk.connection, upsertOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            System.out.println("Retry update failed for key: " + r.id());
                            this.result = false;
                        }  catch (DocumentExistsException e) {
                            System.out.println("Retry update failed for key: " + r.id());
                        }
                    case "delete":
                        try {
                            docops.delete(r.id(), this.sdk.connection, removeOptions);
                            failedMutations.get(optype.getKey()).remove(r);
                        } catch (TimeoutException|ServerOutOfMemoryException e) {
                            System.out.println("Retry delete failed for key: " + r.id());
                            this.result = false;
                        } catch (DocumentNotFoundException e) {
                            System.out.println("Retry delete failed for key: " + r.id());
                        }
                    }
                }
            }
    }
}
