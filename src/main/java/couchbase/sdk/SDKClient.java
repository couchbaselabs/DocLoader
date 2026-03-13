package couchbase.sdk;

import java.time.Duration;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

public class SDKClient {
    static Logger logger = LogManager.getLogger(SDKClient.class);

    public Server master;
    public String bucket;
    public String scope;
    public String collection;

    private Bucket bucketObj;
    private Cluster cluster;

    public Collection connection;

    public static ClusterEnvironment env1 = ClusterEnvironment.builder()
            .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
            .securityConfig(SecurityConfig.enableTls(true)
            .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
            .ioConfig(IoConfig.enableDnsSrv(true))
            .ioConfig(IoConfig.numKvConnections(5))
            .build();

    public static ClusterEnvironment env2 = ClusterEnvironment.builder()
            .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
            .ioConfig(IoConfig.enableDnsSrv(true)).ioConfig(IoConfig.numKvConnections(5))
            .build();

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
        this.selectCollection(scope, collection);
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

    public void selectCollection(String scope, String collection) {
        this.connection = this.bucketObj.scope(scope).collection(collection);
        this.scope = scope;
        this.collection = collection;
    }
}
