package couchbase.sdk;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * SharedClusterManager manages a single Cluster instance per server connection
 * to avoid the performance bottlenecks of creating multiple Cluster instances.
 * Following CBRestLoader.md Multi-Collection Strategy guidelines.
 */
public class SharedClusterManager {
    private static Logger logger = LogManager.getLogger(SharedClusterManager.class);
    
    // Common KV connections setting for massively parallel collection loads
    // Increased from default 5 to 500 to support 5,000 collections loading in parallel
    private static final int DEFAULT_KV_CONNECTIONS = 500;
    
    // Shared ClusterEnvironment with optimized connection settings
    private static ClusterEnvironment sharedEnvironment;
    
    // Store cluster instances per server connection string
    private static ConcurrentHashMap<String, ClusterWrapper> clusterMap = new ConcurrentHashMap<>();
    
    // Initialize the shared environment once (lazy initialization)
    private static void initializeSharedEnvironment() {
        if (sharedEnvironment == null) {
            try {
                sharedEnvironment = ClusterEnvironment.builder()
                        .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
                        .securityConfig(SecurityConfig.enableTls(true)
                                .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
                        .ioConfig(IoConfig.enableDnsSrv(true))
                        .ioConfig(IoConfig.numKvConnections(DEFAULT_KV_CONNECTIONS))
                        .build();
                logger.info("Shared Cluster Environment initialized with " + DEFAULT_KV_CONNECTIONS + " KV connections for massively parallel collection loads");
            } catch (Exception e) {
                logger.error("Failed to initialize shared Cluster Environment", e);
            }
        }
    }
    
    /**
     * Get or create a shared Cluster instance for the given server connection
     */
    public static synchronized Cluster getCluster(Server server) throws AuthenticationFailureException {
        String clusterKey = getClusterKey(server);

        ClusterWrapper wrapper = clusterMap.get(clusterKey);
        if (wrapper == null) {
            logger.info("Creating new shared Cluster instance for server: " + server.ip);
            Cluster cluster = createCluster(server);
            wrapper = new ClusterWrapper(cluster);
            clusterMap.put(clusterKey, wrapper);
        } else {
            wrapper.incrementRefCount();
            logger.debug("Reusing existing Cluster instance for server: " + server.ip +
                         " (ref count: " + wrapper.getRefCount() + ")");
        }

        return wrapper.cluster;
    }
    
    /**
     * Release reference to the shared Cluster instance
     */
    public static synchronized void releaseCluster(Server server) {
        String clusterKey = getClusterKey(server);
        ClusterWrapper wrapper = clusterMap.get(clusterKey);
        
        if (wrapper != null) {
            int refCount = wrapper.decrementRefCount();
            logger.debug("Released Cluster instance for server: " + server.ip + 
                        " (ref count: " + refCount + ")");
            
            if (refCount == 0) {
                logger.info("No more references, disconnecting Cluster for server: " + server.ip);
                wrapper.cluster.disconnect();
                clusterMap.remove(clusterKey);
            }
        }
    }
    
    /**
     * Shutdown all cluster instances and the shared environment
     */
    public static synchronized void shutdownAll() {
        logger.info("Shutting down all shared Cluster instances");
        for (ClusterWrapper wrapper : clusterMap.values()) {
            if (wrapper.cluster != null) {
                wrapper.cluster.disconnect();
            }
        }
        clusterMap.clear();

        if (sharedEnvironment != null) {
            sharedEnvironment.shutdown();
            logger.info("Shared Cluster Environment shutdown complete");
        }
    }
    
    private static Cluster createCluster(Server server) throws AuthenticationFailureException {
        ClusterOptions clusterOptions;
        try {
            if (server.memcached_port.equals("11207")) {
                clusterOptions = ClusterOptions.clusterOptions(server.rest_username, server.rest_password)
                        .environment(sharedEnvironment);
            } else {
                clusterOptions = ClusterOptions.clusterOptions(server.rest_username, server.rest_password)
                        .environment(createNonTLSEnvironment());
            }
            
            Cluster cluster = Cluster.connect(server.ip, clusterOptions);
            logger.info("Cluster connection successful: " + server.ip);
            return cluster;
        } catch (AuthenticationFailureException e) {
            logger.error("Authentication failed for server: " + server.ip + 
                        " with user: " + server.rest_username);
            throw e;
        } catch (Exception e) {
            logger.error("Failed to connect Cluster to server: " + server.ip, e);
            throw new RuntimeException("Cluster connection failed", e);
        }
    }
    
    private static ClusterEnvironment createNonTLSEnvironment() {
        return ClusterEnvironment.builder()
                .timeoutConfig(TimeoutConfig.builder().kvTimeout(Duration.ofSeconds(10)))
                .ioConfig(IoConfig.enableDnsSrv(true))
                .ioConfig(IoConfig.numKvConnections(DEFAULT_KV_CONNECTIONS))
                .build();
    }
    
    private static String getClusterKey(Server server) {
        return server.ip + ":" + server.memcached_port;
    }
    
    /**
     * Wrapper class to track reference count for shared Cluster instances
     */
    private static class ClusterWrapper {
        Cluster cluster;
        AtomicInteger refCount;
        
        ClusterWrapper(Cluster cluster) {
            this.cluster = cluster;
            this.refCount = new AtomicInteger(1);
        }
        
        void incrementRefCount() {
            refCount.incrementAndGet();
        }
        
        int decrementRefCount() {
            return refCount.decrementAndGet();
        }
        
        int getRefCount() {
            return refCount.get();
        }
    }
}
