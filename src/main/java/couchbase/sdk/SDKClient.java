package couchbase.sdk;

import java.time.Duration;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

public class SDKClient {
    static Logger logger = LogManager.getLogger(SDKClient.class);

    public Server master;
    public String bucket;
    public String scope;
    public String collection;

    private Bucket bucketObj;
    private Cluster cluster;

    // Resolved once, for the scope/collection this client was constructed with.
    // Never reassigned after initialiseSDK(): a client handed out to one caller must
    // never be re-pointed at a different collection by a second caller.
    private volatile Collection defaultCollection;

    public SDKClient(Server master, String bucket, String scope, String collection) {
        super();
        this.master = master;
        this.bucket = bucket;
        this.scope = scope;
        this.collection = collection;
    }

    public SDKClient(Server master, String bucket) {
        super();
        this.master = master;
        this.bucket = bucket;
        this.scope = "_default";
        this.collection = "_default";
    }

    public SDKClient() {
        super();
    }

    public void initialiseSDK() throws Exception {
        logger.info("Connection to the cluster");
        this.connectCluster();
        this.connectBucket(bucket);
        this.defaultCollection = this.bucketObj.scope(this.scope)
                                               .collection(this.collection);
    }

    public void connectCluster(){
        try{
            // Use shared Cluster instance instead of creating new one
            this.cluster = SharedClusterManager.getCluster(this.master);
            logger.info("Cluster connection is successful (using shared instance)");
        }
        catch (AuthenticationFailureException e) {
            logger.info(String.format("cannot login from user: %s/%s",master.rest_username, master.rest_password));
        }
    }

    public void disconnectCluster(){
        // Release reference to shared Cluster instead of disconnecting
        SharedClusterManager.releaseCluster(this.master);
        logger.info("Released shared Cluster instance reference");
    }

    public void shutdownEnv() {
        // No-op - Shared Cluster environment is managed by SharedClusterManager
        logger.debug("shutdownEnv called on shared Cluster - no-op");
    }

    private void connectBucket(String bucket){
        this.bucketObj = this.cluster.bucket(bucket);
    }

    /**
     * Resolve a collection handle for this client's bucket.
     *
     * Deliberately a pure function of its arguments: the caller keeps the handle in a
     * local, so a client shared by many concurrent workers has no per-collection state
     * for one worker to overwrite while another is mid-batch. Cluster, Bucket and
     * Collection are all thread-safe and meant to be shared.
     */
    public Collection collection(String scope, String collection) {
        return this.bucketObj.scope(scope).collection(collection);
    }

    /**
     * The collection this client was constructed for. Used by the CLI loaders, which
     * build one client per run rather than going through SDKClientPool.
     */
    public Collection collection() {
        return this.defaultCollection;
    }
}
