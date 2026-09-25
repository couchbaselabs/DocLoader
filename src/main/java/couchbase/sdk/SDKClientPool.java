package couchbase.sdk;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * One SDK client per bucket, shared by every worker loading that bucket.
 *
 * There used to be a queue of idle clients per bucket plus a cache keyed by
 * bucket:scope:collection, because a client carried its target collection in a mutable
 * field and therefore could only serve one collection at a time. That design had two
 * defects that no amount of locking around the cache would remove:
 *
 *  - acquire was a check-then-act (cache lookup, then refcount increment). A release
 *    landing in that window dropped the refcount to zero, returned the client to the
 *    idle queue, and let a third thread re-point it at a different collection - while
 *    the first thread was still using it. Its documents then went to the wrong
 *    collection, and the failures were reported against the collection it thought it
 *    was writing to.
 *  - two concurrent misses on the same key each took a client and the second cache
 *    entry overwrote the first, so one client was never returned to the pool. With a
 *    finite pool, repeated leaks eventually wedged every worker in take().
 *
 * SDKClient no longer holds collection state (see SDKClient.collection(scope, coll)),
 * so a client is just a bucket handle. Cluster/Bucket/Collection are thread-safe, so
 * one handle serves any number of concurrent workers on any number of collections.
 * There is nothing left to check, to count, to hand out or to give back.
 */
public class SDKClientPool {
    static Logger logger = LogManager.getLogger(SDKClientPool.class);

    private final ConcurrentHashMap<String, SDKClient> clients = new ConcurrentHashMap<>();

    public SDKClientPool() {
        super();
    }

    public void shutdown() {
        logger.debug("Closing clients from SDKClientPool and shutting down shared Cluster instances");
        for (SDKClient client : clients.values()) {
            client.disconnectCluster();
        }
        clients.clear();
        SharedClusterManager.shutdownAll();
    }

    public void force_close_clients_for_bucket(String bucket_name) {
        SDKClient client = clients.remove(bucket_name);
        if (client != null) {
            client.disconnectCluster();
        }
    }

    /**
     * req_clients is accepted for wire compatibility with existing callers but is no
     * longer meaningful: a single shared handle per bucket serves every worker, so
     * there is no pool to size and no worker ever blocks waiting for a client.
     */
    public void create_clients(String bucket_name, Server server, int req_clients) throws Exception {
        if (clients.containsKey(bucket_name)) {
            return;
        }
        SDKClient client = new SDKClient(server, bucket_name);
        client.initialiseSDK();
        SDKClient existing = clients.putIfAbsent(bucket_name, client);
        if (existing != null) {
            // Lost a concurrent create for the same bucket - drop ours.
            client.disconnectCluster();
        }
    }

    public SDKClient get_client_for_bucket(String bucket_name) {
        return clients.get(bucket_name);
    }
}
