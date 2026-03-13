package couchbase.sdk;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class SDKClientPool {
    static Logger logger = LogManager.getLogger(SDKClientPool.class);
    
    // Thread-safe client collection cache
    private ConcurrentHashMap<String, ClientInfo> clientCache = new ConcurrentHashMap<>();
    
    // Thread-safe client pools by bucket
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<SDKClient>> idleClients = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<SDKClient>> busyClients = new ConcurrentHashMap<>();

    public SDKClientPool() {
        super();
    }

    public void shutdown() {
        logger.debug("Closing clients from SDKClientPool and shutting down shared Cluster instances");
        
        // Process all buckets
        for (String bucketName : idleClients.keySet()) {
            ConcurrentLinkedQueue<SDKClient> idle = idleClients.get(bucketName);
            ConcurrentLinkedQueue<SDKClient> busy = busyClients.get(bucketName);
            
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
        ConcurrentLinkedQueue<SDKClient> idle = idleClients.get(bucket_name);
        ConcurrentLinkedQueue<SDKClient> busy = busyClients.get(bucket_name);
        
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
        idleClients.computeIfAbsent(bucket_name, k -> new ConcurrentLinkedQueue<>());
        busyClients.computeIfAbsent(bucket_name, k -> new ConcurrentLinkedQueue<>());
        
        ConcurrentLinkedQueue<SDKClient> idlePool = idleClients.get(bucket_name);
        
        for (int i = 0; i < req_clients; i++) {
            SDKClient client = new SDKClient(server, bucket_name);
            client.initialiseSDK();
            idlePool.add(client);
        }
    }

    public SDKClient get_client_for_bucket(String bucket_name, String scope, String collection) {
        String col_name = scope + collection;
        
        // Check if client is already cached for this collection
        ClientInfo existing = clientCache.get(col_name);
        if (existing != null) {
            existing.counter.incrementAndGet();
            return existing.client;
        }
        
        // Get idle client pool for this bucket
        ConcurrentLinkedQueue<SDKClient> idlePool = idleClients.get(bucket_name);
        if (idlePool == null || idlePool.isEmpty()) {
            return null;
        }
        
        // Get client from idle pool atomically
        SDKClient client = idlePool.poll();
        if (client == null) {
            return null;
        }
        
        // Configure client for this collection
        client.selectCollection(scope, collection);
        
        // Add to busy pool atomically
        busyClients.computeIfAbsent(bucket_name, k -> new ConcurrentLinkedQueue<>()).add(client);
        
        // Cache client reference with thread-safe counter
        clientCache.put(col_name, new ClientInfo(client, new AtomicInteger(1)));
        
        return client;
    }

    public void release_client(SDKClient client) {
        if (client == null || client.bucket == null) {
            return;
        }
        
        String bucket_key = client.bucket;
        String col_name = client.scope + client.collection;
        
        // Get cached client info
        ClientInfo info = clientCache.get(col_name);
        if (info == null) {
            return;
        }
        
        // Decrement counter atomically
        int newCount = info.counter.decrementAndGet();
        
        if (newCount == 0) {
            // Remove from cache atomically
            clientCache.remove(col_name);
            
            // Remove from busy pool and add to idle pool atomically
            ConcurrentLinkedQueue<SDKClient> busyPool = busyClients.get(bucket_key);
            ConcurrentLinkedQueue<SDKClient> idlePool = idleClients.get(bucket_key);
            
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
