package couchbase.sdk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class SDKClientPool {
    static Logger logger = LogManager.getLogger(SDKClientPool.class);
    
    public static class BucketPool {
        private final ArrayList<SDKClient> allClients;
        private final BlockingQueue<SDKClient> availableClients;
        private final ReentrantLock lock;
        
        public BucketPool(int size) {
            this.allClients = new ArrayList<>(size);
            this.availableClients = new ArrayBlockingQueue<>(size);
            this.lock = new ReentrantLock();
        }
    }
    
    public ConcurrentHashMap<String, BucketPool> clients;

    public SDKClientPool() {
        super();
        this.clients = new ConcurrentHashMap<String, BucketPool>();
    }

    public void shutdown() {
        logger.debug("Closing clients from SDKClientPool");
        for(Map.Entry<String, BucketPool> entry: this.clients.entrySet()){
            BucketPool pool = entry.getValue();
            for(SDKClient sdk_client: pool.allClients)
                sdk_client.disconnectCluster();
        }
        this.clients.clear();
    }

    public void force_close_clients_for_bucket(String bucket_name) {
        BucketPool pool = this.clients.get(bucket_name);
        if (pool == null)
            return;

        pool.lock.lock();
        try {
            for(SDKClient sdk_client: pool.allClients) {
                sdk_client.disconnectCluster();
            }
            pool.allClients.clear();
            pool.availableClients.clear();
            this.clients.remove(bucket_name);
        } finally {
            pool.lock.unlock();
        }
    }

    public void create_clients(String bucket_name, Server server, int req_clients) throws Exception {
        BucketPool pool = this.clients.computeIfAbsent(bucket_name, k -> new BucketPool(req_clients));
        
        pool.lock.lock();
        try {
            for(int i=0; i<req_clients; i++) {
                SDKClient tem_client = new SDKClient(server, bucket_name);
                tem_client.initialiseSDK();
                pool.allClients.add(tem_client);
                pool.availableClients.offer(tem_client);
            }
        } finally {
            pool.lock.unlock();
        }
    }

    public SDKClient get_client_for_bucket(String bucket_name, String scope, String collection) {
        BucketPool pool = this.clients.get(bucket_name);
        if (pool == null)
            return null;

        try {
            SDKClient client = null;
            while (client == null) {
                client = pool.availableClients.take();
                if (client != null) {
                    client.selectCollection(scope, collection);
                }
            }
            return client;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public void release_client(SDKClient client) {
        BucketPool pool = this.clients.get(client.bucket);
        if (pool == null)
            return;

        pool.availableClients.offer(client);
    }
}
