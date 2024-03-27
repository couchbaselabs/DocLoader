package couchbase.test.rest;
import couchbase.test.docgen.DRConstants;
import couchbase.test.docgen.DocRange;
import couchbase.test.docgen.DocumentGenerator;
import couchbase.test.docgen.WorkLoadSettings;
import couchbase.test.loadgen.WorkLoadGenerate;
import couchbase.test.sdk.SDKClient;
import couchbase.test.sdk.Server;
import couchbase.test.taskmanager.TaskManager;
import elasticsearch.EsClient;
import org.springframework.http.ResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


@JsonIgnoreProperties(ignoreUnknown = true)
public class LoadRequest {

    @JsonProperty("node")
    private String node;
    @JsonProperty("user")
    private String user;

    @JsonProperty("password")
    private String password;

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("port")
    private String port;
    private String collection;
    private int createPercent;
    private int createStart;
    private int createEnd;
    private int updatePercent;
    private int updateStart;
    private int updateEnd;
    private int deletePercent;
    private int deleteStart;
    private int deleteEnd;
    private boolean deleted;
    private int expiryPercent;
    private int expiryStart;
    private int expiryEnd;
    private int readPercent;
    private int readStart;
    private int readEnd;
    private int replaceStart;
    private int replaceEnd;
    private int touchStart;
    private int touchEnd;
    private String durability;

    private String keyPrefix;

    private int keySize;
    private String keyType;
    private String loadType;
    private int opsPerSec;
    private int docSize;
    private String scope;
    private String transactionPatterns;
    private int workers;
    private boolean validateData;
    private String valueType;
    private boolean gtm;
    private int mutate;
    private boolean elastic;

    private String model;
    private boolean mockVector;
    private int dim;
    private int retry;
    private boolean esAPI;
    private int maxTTL;
    private String maxTTLUnit;


    // Generate getters and setters for all parameters

    public static LoadRequest fromJson(String json) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, LoadRequest.class);
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getCreatePercent() {
        return createPercent;
    }

    public void setCreatePercent(int createPercent) {
        this.createPercent = createPercent;
    }

    public int getCreateStart() {
        return createStart;
    }

    public void setCreateStart(int createStart) {
        this.createStart = createStart;
    }

    public int getCreateEnd() {
        return createEnd;
    }

    public void setCreateEnd(int createEnd) {
        this.createEnd = createEnd;
    }

    public int getDeletePercent() {
        return deletePercent;
    }

    public void setDeletePercent(int deletePercent) {
        this.deletePercent = deletePercent;
    }

    public int getDeleteStart() {
        return deleteStart;
    }

    public void setDeleteStart(int deleteStart) {
        this.deleteStart = deleteStart;
    }

    public int getDeleteEnd() {
        return deleteEnd;
    }

    public void setDeleteEnd(int deleteEnd) {
        this.deleteEnd = deleteEnd;
    }

    public boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public int getExpiryPercent() {
        return expiryPercent;
    }

    public void setExpiryPercent(int expiryPercent) {
        this.expiryPercent = expiryPercent;
    }

    public int getExpiryStart() {
        return expiryStart;
    }

    public void setExpiryStart(int expiryStart) {
        this.expiryStart = expiryStart;
    }

    public int getExpiryEnd() {
        return expiryEnd;
    }

    public void setExpiryEnd(int expiryEnd) {
        this.expiryEnd = expiryEnd;
    }

    public int getReadPercent() {
        return readPercent;
    }

    public void setReadPercent(int readPercent) {
        this.readPercent = readPercent;
    }

    public int getReadStart() {
        return readStart;
    }

    public void setReadStart(int readStart) {
        this.readStart = readStart;
    }

    public int getReadEnd() {
        return readEnd;
    }

    public void setReadEnd(int readEnd) {
        this.readEnd = readEnd;
    }

    public int getReplaceStart() {
        return replaceStart;
    }

    public void setReplaceStart(int replaceStart) {
        this.replaceStart = replaceStart;
    }

    public int getReplaceEnd() {
        return replaceEnd;
    }

    public void setReplaceEnd(int replaceEnd) {
        this.replaceEnd = replaceEnd;
    }

    public int getTouchStart() {
        return touchStart;
    }

    public void setTouchStart(int touchStart) {
        this.touchStart = touchStart;
    }

    public int getTouchEnd() {
        return touchEnd;
    }

    public void setTouchEnd(int touchEnd) {
        this.touchEnd = touchEnd;
    }

    public String getDurability() {
        return durability;
    }

    public void setDurability(String durability) {
        this.durability = durability;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public String getLoadType() {
        return loadType;
    }

    public void setLoadType(String loadType) {
        this.loadType = loadType;
    }

    public int getOpsPerSec() {
        return opsPerSec;
    }

    public void setOpsPerSec(int opsPerSec) {
        this.opsPerSec = opsPerSec;
    }

    public int getDocSize() {
        return docSize;
    }

    public void setDocSize(int docSize) {
        this.docSize = docSize;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getTransactionPatterns() {
        return transactionPatterns;
    }

    public void setTransactionPatterns(String transactionPatterns) {
        this.transactionPatterns = transactionPatterns;
    }

    public int getWorkers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public boolean isValidateData() {
        return validateData;
    }

    public void setValidateData(boolean validateData) {
        this.validateData = validateData;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }



    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public int getUpdatePercent() {
        return updatePercent;
    }

    public void setUpdatePercent(int updatePercent) {
        this.updatePercent = updatePercent;
    }

    public int getUpdateStart() {
        return updateStart;
    }

    public void setUpdateStart(int updateStart) {
        this.updateStart = updateStart;
    }

    public int getUpdateEnd() {
        return updateEnd;
    }

    public void setUpdateEnd(int updateEnd) {
        this.updateEnd = updateEnd;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getMaxTTLUnit(){
        return this.maxTTLUnit;
    }
    public void setMaxTTLUnit(String maxTTLUnit) {
        this.maxTTLUnit = maxTTLUnit;
    }

    public ResponseEntity<String> processRequest() {
        if(this.validateRequest()) {
            Thread requestThread = new Thread(()->{
                this.work();
            });
            requestThread.start();
            String message = "OK";
            return ResponseEntity.ok(message);
        }
        else {
            return ResponseEntity.badRequest().body("Required parameters are missing");
        }
    }

    boolean validateRequest(){
        if(Objects.equals(this.getUser(),null) ||Objects.equals(this.getUser(), "")){
            return false;
        }
        if(Objects.equals(this.getNode(),null) ||Objects.equals(this.getNode(), "")){
            return false;
        }
        if(Objects.equals(this.getPassword(),null) ||Objects.equals(this.getPassword(), "")){
            return false;
        }
        if(Objects.equals(this.getBucket(),null) ||Objects.equals(this.getBucket(), "")){
            return false;
        }
        if(Objects.equals(this.getPort(),null) ||Objects.equals(this.getPort(), "")){
            return false;
        }

        if(this.getWorkers() == 0){
            this.setWorkers(10);
        }

        if(Objects.equals(this.getKeyPrefix(),null) ||Objects.equals(this.getKeyPrefix(), "")){
            this.setKeyPrefix("test_docs-");
        }

        if(this.getKeySize()==0){
            this.setKeySize(20);
        }

        if(this.getDocSize()==0){
            this.setDocSize(256);
        }

        if(this.getOpsPerSec() == 0){
            this.setOpsPerSec(10000);
        }

        if(Objects.equals(this.getLoadType(),null) ||Objects.equals(this.getLoadType(), "")){
            this.setLoadType(null);
        }

        if(Objects.equals(this.getKeyType(),null) ||Objects.equals(this.getKeyType(), "")){
            this.setKeyType("SimpleKey");
        }

        if(Objects.equals(this.getValueType(),null) ||Objects.equals(this.getValueType(), "")){
            this.setValueType("SimpleValue");
        }

        if(Objects.equals(this.getModel(),null) ||Objects.equals(this.getModel(), "")){
            this.setModel("sentence-transformers/paraphrase-MiniLM-L3-v2");
        }

        if(Objects.equals(this.getScope(),null) ||Objects.equals(this.getScope(), "")){
            this.setScope("_default");
        }

        if(Objects.equals(this.getCollection(),null) ||Objects.equals(this.getCollection(), "")){
            this.setCollection("_default");
        }

        if(Objects.equals(this.getDurability(),null) ||Objects.equals(this.getDurability(), "")){
            this.setDurability("NONE");
        }


        if(Objects.equals(this.getMaxTTLUnit(),null) || Objects.equals(this.getMaxTTLUnit(), "")){
            this.setMaxTTLUnit("seconds");
        }
        return true;
    }

    void work(){
        Server master = new Server(this.getNode(), this.getPort(),this.getUser(),this.getPassword(),this.getPort());

        TaskManager tm = new TaskManager(this.workers);

        WorkLoadSettings ws = new WorkLoadSettings(this.getKeyPrefix(),
                this.getKeySize(),
                this.getDocSize(),
                this.getCreatePercent(), this.getReadPercent(),
                this.getUpdatePercent(), this.getDeletePercent(),
                this.getExpiryPercent(), this.getWorkers(),
                this.getOpsPerSec(), this.getLoadType(),
                this.getKeyType(), this.getValueType(),
                this.validateData,
                this.gtm,
                this.deleted,
                this.mutate,
                this.elastic,
                this.getModel(),
                this.mockVector,
                this.dim);

        HashMap<String, Number> dr = new HashMap<String, Number>();
        dr.put(DRConstants.create_s, this.getCreateStart());
        dr.put(DRConstants.create_e, this.getCreateEnd());
        dr.put(DRConstants.read_s, this.getReadStart());
        dr.put(DRConstants.read_e, this.getReadEnd());
        dr.put(DRConstants.update_s,this.getUpdateStart());
        dr.put(DRConstants.update_e, this.getUpdateEnd());
        dr.put(DRConstants.delete_s, this.getDeleteStart());
        dr.put(DRConstants.delete_e, this.getDeleteEnd());
        dr.put(DRConstants.touch_s, this.getTouchStart());
        dr.put(DRConstants.touch_e, this.getTouchEnd());
        dr.put(DRConstants.replace_s, this.getReplaceStart());
        dr.put(DRConstants.replace_e,this.getReplaceEnd());
        dr.put(DRConstants.expiry_s, this.getExpiryStart());
        dr.put(DRConstants.expiry_e, this.getExpiryEnd());

        DocRange range = new DocRange(dr);
        ws.dr = range;
        DocumentGenerator dg = null;
        try {
            dg = new DocumentGenerator(ws, ws.keyType, ws.valueType);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        System.out.println(this.maxTTLUnit+ " "+ this.durability + " " + this.getCollection());
        SDKClient client = new SDKClient(master, this.getBucket(),this.getScope(),
                this.getCollection());

        try {
            client.initialiseSDK();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < ws.workers; i++) {
            try {
                String th_name = "Loader" + i;
                boolean trackFailures = false;
                if (this.retry > 0)
                    trackFailures = true;
                tm.submit(new WorkLoadGenerate(th_name, dg, client, null, this.getDurability(),
                        this.maxTTL,
                        this.maxTTLUnit, trackFailures,
                        this.retry, null));
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tm.getAllTaskResult();
        tm.shutdown();
        client.disconnectCluster();
    }
}
