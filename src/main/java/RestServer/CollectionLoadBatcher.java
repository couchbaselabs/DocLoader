package RestServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * CollectionLoadBatcher implements Java-side batching for massive collection loads.
 * When Python calls doc_load() sequentially for many collections, this batches them
 * to prevent worker starvation and queue overhead.
 */
public class CollectionLoadBatcher {
    static Logger logger = LogManager.getLogger(CollectionLoadBatcher.class);
    
    private static final int BATCH_SIZE = 50;  // Process 50 collections concurrently
    private static ExecutorService batchExecutor;
    private static Map<String, BatchState> batchStates = new ConcurrentHashMap<>();
    private static Object batchLock = new Object();
    
    static {
        batchExecutor = Executors.newFixedThreadPool(5);  // 5 concurrent batch processors
        logger.info("CollectionLoadBatcher initialized with batch size: " + BATCH_SIZE);
    }
    
    public static class BatchState {
        String batchId;
        List<String> tasknames = new ArrayList<>();
        int totalCollections;
        int completedCollections;
        long startTime;
        
        public BatchState(String batchId, int totalCollections) {
            this.batchId = batchId;
            this.totalCollections = totalCollections;
            this.completedCollections = 0;
            this.startTime = System.currentTimeMillis();
        }
        
        public synchronized void addTask(String taskname) {
            tasknames.add(taskname);
            completedCollections++;
        }
        
        public synchronized boolean isComplete() {
            return completedCollections >= totalCollections;
        }
        
        public synchronized double getProgress() {
            return (double)completedCollections / totalCollections;
        }
    }
    
    /**
     * Submit a collection load request to the batch processor
     */
    public static ResponseEntity<Map<String, Object>> submitToBatch(Map<String, Object> requestBody) {
        try {
            TaskRequest taskRequest = TaskRequest.fromJson(requestBody.toString());
            
            // Get current batch or create new one
            String batchId = getCurrentBatchId();
            BatchState batchState = batchStates.computeIfAbsent(batchId, k -> 
                new BatchState(batchId, BATCH_SIZE));
            
            // Process the doc_load normally
            ResponseEntity<Map<String, Object>> result = taskRequest.doc_load();
            
            // Add to batch
            batchState.addTask(result.getBody().get("tasks").toString());
            
            // Check if batch is complete and start next batch
            if (batchState.isComplete()) {
                logger.info("Batch " + batchId + " complete (" + batchState.totalCollections + " collections)");
                batchStates.remove(batchId);
                
                // Start processing next batch if there are pending loads
                startNextBatch();
            }
            
            return result;
            
        } catch (Exception e) {
            Map<String, Object> body = new HashMap<>();
            body.put("error", "Batch processing failed: " + e.getMessage());
            body.put("status", false);
            return new ResponseEntity<>(body, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
    private static synchronized String getCurrentBatchId() {
        // Find current batch with capacity
        for (Map.Entry<String, BatchState> entry : batchStates.entrySet()) {
            if (!entry.getValue().isComplete()) {
                return entry.getKey();
            }
        }
        
        // Create new batch ID
        return "batch_" + System.currentTimeMillis();
    }
    
    private static void startNextBatch() {
        // Could implement proactive batch starting if needed
        logger.debug("Ready for next batch of collection loads");
    }
    
    public static void shutdown() {
        if (batchExecutor != null) {
            batchExecutor.shutdownNow();
            logger.info("CollectionLoadBatcher shutdown complete");
        }
    }
    
    public static Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("active_batches", batchStates.size());
        stats.put("total_capacity", BATCH_SIZE);
        
        List<String> batchProgress = new ArrayList<>();
        for (Map.Entry<String, BatchState> entry : batchStates.entrySet()) {
            BatchState state = entry.getValue();
            batchProgress.add(String.format("%s: %.1f%% (%d/%d)", 
                entry.getKey(), 
                state.getProgress() * 100,
                state.completedCollections,
                state.totalCollections));
        }
        stats.put("batch_progress", batchProgress);
        
        return stats;
    }
}
