package couchbase.sdk;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class SDKClientPool {
    static Logger logger = LogManager.getLogger(SDKClientPool.class);
    
    // Thread-safe client collection cache
    private ConcurrentHashMap<String, ClientInfo> clientCache = new ConcurrentHashMap<>();
    
    // Block up to this long waiting for an idle client before giving up
    private static final int CLIENT_WAIT_TIMEOUT_MINUTES = 30;

    // Thread-safe client pools by bucket
    private ConcurrentHashMap<String, LinkedBlockingQueue<SDKClient>> idleClients = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, LinkedBlockingQueue<SDKClient>> busyClients = new ConcurrentHashMap<>();

    public SDKClientPool() {
        super();
    }

    public void shutdown() {
        logger.debug("Closing clients from SDKClientPool and shutting down shared Cluster instances");
        
        // Process all buckets
        for (String bucketName : idleClients.keySet()) {
            LinkedBlockingQueue<SDKClient> idle = idleClients.get(bucketName);
            LinkedBlockingQueue<SDKClient> busy = busyClients.get(bucketName);
            
            if (idle != null) {
                for (SDKClient client : idle) {
                    client.disconnectCluster();
                }
            }
            if (busy != null) {
                for (SDKClient client : busy) {
                    client.disconnectCluster();
                }
            }
        }
        
        // Clear all data structures
        clientCache.clear();
        idleClients.clear();
        busyClients.clear();
        
        // Shutdown shared Cluster manager
        SharedClusterManager.shutdownAll();
    }

    public void force_close_clients_for_bucket(String bucket_name) {
        LinkedBlockingQueue<SDKClient> idle = idleClients.get(bucket_name);
        LinkedBlockingQueue<SDKClient> busy = busyClients.get(bucket_name);
        
        if (idle != null) {
            for (SDKClient client : idle) {
                client.disconnectCluster();
            }
            idleClients.remove(bucket_name);
        }
        
        if (busy != null) {
            for (SDKClient client : busy) {
                client.disconnectCluster();
            }
            busyClients.remove(bucket_name);
        }
    }

    public void create_clients(String bucket_name, Server server, int req_clients) throws Exception {
        // Initialize thread-safe client pools for this bucket if not already present
        idleClients.computeIfAbsent(bucket_name, k -> new LinkedBlockingQueue<>());
        busyClients.computeIfAbsent(bucket_name, k -> new LinkedBlockingQueue<>());
        
        LinkedBlockingQueue<SDKClient> idlePool = idleClients.get(bucket_name);

        for (int i = 0; i < req_clients; i++) {
            SDKClient client = new SDKClient(server, bucket_name);
            client.initialiseSDK();
            idlePool.add(client);
        }
    }

    public SDKClient get_client_for_bucket(String bucket_name, String scope, String collection)
            throws InterruptedException {
        String cache_key = bucket_name + ":" + scope + ":" + collection;

        // Check if client is already cached for this collection
        ClientInfo existing = clientCache.get(cache_key);
        if (existing != null) {
            existing.counter.incrementAndGet();
            return existing.client;
        }

        // Get idle client pool for this bucket
        LinkedBlockingQueue<SDKClient> idlePool = idleClients.get(bucket_name);
        if (idlePool == null) {
            return null;
        }

        // Block until a client becomes available or timeout expires.
        // With 200-300 threads sharing a finite pool, spinning with a fixed retry cap
        // causes spurious failures on long-running loads — blocking here is cheaper and correct.
        SDKClient client = idlePool.poll(CLIENT_WAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        if (client == null) {
            logger.error("Timed out waiting " + CLIENT_WAIT_TIMEOUT_MINUTES
                    + " min for idle SDK client for bucket " + bucket_name);
            return null;
        }

        // Configure client for this collection
        client.selectCollection(scope, collection);

        // Add to busy pool atomically
        busyClients.computeIfAbsent(bucket_name, k -> new LinkedBlockingQueue<>()).add(client);

        // Cache client reference with thread-safe counter
        clientCache.put(cache_key, new ClientInfo(client, new AtomicInteger(1)));

        return client;
    }

    public void release_client(SDKClient client) {
        if (client == null || client.bucket == null) {
            return;
        }
        
        String bucket_key = client.bucket;
        String cache_key = bucket_key + ":" + client.scope + ":" + client.collection;

        // Get cached client info
        ClientInfo info = clientCache.get(cache_key);
        if (info == null) {
            return;
        }
        
        // Decrement counter atomically
        int newCount = info.counter.decrementAndGet();
        
        if (newCount == 0) {
            // Remove from cache atomically
            clientCache.remove(cache_key);
            
            // Remove from busy pool and add to idle pool atomically
            LinkedBlockingQueue<SDKClient> busyPool = busyClients.get(bucket_key);
            LinkedBlockingQueue<SDKClient> idlePool = idleClients.get(bucket_key);
            
            if (busyPool != null) {
                busyPool.remove(client);
            }
            if (idlePool != null) {
                idlePool.add(client);
            }
        }
    }
    
    // Helper class for cached client info with thread-safe counter
    private static class ClientInfo {
        SDKClient client;
        AtomicInteger counter;
        
        ClientInfo(SDKClient client, AtomicInteger counter) {
            this.client = client;
            this.counter = counter;
        }
    }
}
