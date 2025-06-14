package utils.docgen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import reactor.util.function.Tuple2;

public class WorkLoadSettings extends WorkLoadBase {
    public String keyPrefix = "test_docs-";
    public int workers = 10;
    public int ops = 40000;
    public int batchSize = ops/workers;
    public int keySize = 15;
    public int docSize = 256;

    public int creates = 0;
    public int reads = 0;
    public int updates = 0;
    public int deletes = 0;
    public int expiry = 0;
    public int subdocs = 0;
    public int workingSet = 100;

    public String loadType;
    public String keyType = "SimpleKey";
    public String valueType = "SimpleValue";
    public boolean gtm;
    public boolean expectDeleted;
    public boolean validate;
    public int mutated;

    public String mutate_field;
    public int mutation_timeout;

    public DocRange dr;
    public DocumentGenerator doc_gen;

    // Sub-doc specific params
    public boolean create_path;
    public boolean is_subdoc_xattr;
    public boolean is_subdoc_sys_xattr;

    /**** Transaction parameters ****/
    public List<List<?>> transaction_pattern;
    public Boolean commit_transaction;
    public Boolean rollback_transaction;
    public boolean elastic;
    public boolean mongo;
    public String model;
    public boolean mockVector;
    public int dim;
    public boolean base64;
    public String baseVectorsFilePath;

    /**** Constructors ****/
    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u, int d, int e,
            int workers, int ops, String loadType,
            String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted, int mutated,
            boolean elastic, String model, boolean mockVector, int dim, boolean base64,
            String mutate_field, Integer mutation_timeout,
            String baseVectorsFilePath) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.expiry = e;
        this.workers = workers;
        this.ops = ops;
        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
        this.mutated = mutated;
        this.valueType = valueType;
        this.keyType = keyType;
        this.elastic = elastic;
        this.model = model;
        this.mockVector = mockVector;
        this.dim = dim;
        this.mutate_field = mutate_field;
        this.mutation_timeout = mutation_timeout;
        this.base64 = base64;
        this.baseVectorsFilePath = baseVectorsFilePath;
    };

    public WorkLoadSettings(String keyPrefix,
                            int keySize, int docSize, int c, int r, int u, int d, int e,
                            int workers, int ops, String loadType,
                            String keyType, String valueType,
                            boolean validate, boolean gtm, boolean deleted, int mutated) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.expiry = e;
        this.workers = workers;
        this.ops = ops;

        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
        this.mutated = mutated;
        this.valueType = valueType;
        this.keyType = keyType;
    };

    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u, int d, int e,
            int workers, int ops, String loadType,
            String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted, int mutated,
            boolean elastic, String model, boolean mockVector, int dim, boolean base64,
            String mutate_field, Integer mutation_timeout) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.expiry = e;
        this.workers = workers;
        this.ops = ops;
        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
        this.mutated = mutated;
        this.valueType = valueType;
        this.keyType = keyType;
        this.elastic = elastic;
        this.model = model;
        this.mockVector = mockVector;
        this.dim = dim;
        this.mutate_field = mutate_field;
        this.mutation_timeout = mutation_timeout;
        this.base64 = base64;
    };

    public WorkLoadSettings(
            String keyPrefix, int key_size, int doc_size, int mutated,
            Tuple2<Integer, Integer> key_range, int batch_size,
            List<?> transaction_pattern,
            int workers) throws ClassNotFoundException {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = key_size;
        this.docSize = doc_size;
        this.workers = workers;

        this.batchSize = batch_size;

        // Create DocRange object from key_range
        HashMap hm = new HashMap();
        hm.put(DRConstants.create_s, key_range.getT1());
        hm.put(DRConstants.create_e, key_range.getT2());

        this.dr = new DocRange(hm);

        // Populates this.transaction_pattern
            this.create_transaction_load_pattern_per_worker(
                 key_range.getT1(), key_range.getT2(), transaction_pattern);

        // Create DocumentGenerator object
        this.doc_gen = new DocumentGenerator(this, this.keyType, this.valueType);
    }

    /* Constructor to include SubDoc operations */
    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u, int d, int e, int sd,
            int workers, int ops, String loadType,
            String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted, int mutated,
            boolean create_path, boolean is_subdoc_xattr, boolean is_subdoc_sys_xattr) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.expiry = e;
        this.subdocs = sd;
        this.workers = workers;
        this.ops = ops;

        this.create_path = create_path;
        this.is_subdoc_xattr = is_subdoc_xattr;
        this.is_subdoc_sys_xattr = is_subdoc_sys_xattr;

        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
        this.mutated = mutated;
        this.valueType = valueType;
        this.keyType = keyType;
    };

    /* Constructor to include SubDoc operations and elastic parameters */
    public WorkLoadSettings(String keyPrefix,
            int keySize, int docSize, int c, int r, int u, int d, int e, int sd,
            int workers, int ops, String loadType,
            String keyType, String valueType,
            boolean validate, boolean gtm, boolean deleted, int mutated,
            boolean create_path, boolean is_subdoc_xattr, boolean is_subdoc_sys_xattr,
            boolean elastic, String model, boolean mockVector,
            int dim, boolean base64, String mutate_field,
            int mutation_timeout) {
        super();
        this.keyPrefix = keyPrefix;
        this.keySize = keySize;
        this.docSize = docSize;
        this.creates = c;
        this.reads = r;
        this.updates = u;
        this.deletes = d;
        this.expiry = e;
        this.subdocs = sd;
        this.workers = workers;
        this.ops = ops;

        this.create_path = create_path;
        this.is_subdoc_xattr = is_subdoc_xattr;
        this.is_subdoc_sys_xattr = is_subdoc_sys_xattr;

        this.batchSize = this.ops/this.workers;
        this.gtm = gtm;
        this.expectDeleted = deleted;
        this.validate = validate;
        this.mutated = mutated;
        this.valueType = valueType;
        this.keyType = keyType;
        
        this.elastic = elastic;
        this.model = model;
        this.mockVector = mockVector;
        this.dim = dim;
        this.base64 = base64;
        this.mutate_field = mutate_field;
        this.mutation_timeout = mutation_timeout;
    };

    public void setTransactionCommit(Boolean commit_transaction) {
        this.commit_transaction = commit_transaction;
    }

    public void setTransactionRollback(Boolean rollback_transaction) {
        this.rollback_transaction = rollback_transaction;
    }

    private void create_transaction_load_pattern_per_worker(int start, int end, List<?> transaction_pattern) {
        int total_docs_to_be_mutated = (end - start);
        float max_iterations = (float)(total_docs_to_be_mutated/this.batchSize) / this.workers;
        int iterations_to_run = (int)Math.floor(max_iterations);
        int batch_with_extra_loops = ((int)Math.ceil(max_iterations*this.workers-this.workers)) % this.workers;
        List<Object> pattern_1 = (List<Object>)transaction_pattern.get(0);
        List<String> crud_pattern = (List<String>)pattern_1.get(2);

        int pattern_index = 0;
        int t_pattern_len = crud_pattern.size();

        this.transaction_pattern = new ArrayList<List<?>>();
        for(int index=0; index < this.workers; index++) {
            List t_pattern = new ArrayList<Object>();
            List load_pattern = new ArrayList<Object>();

            t_pattern.add(this.batchSize);
            if(index < batch_with_extra_loops)
                t_pattern.add(iterations_to_run+1);
            else
                t_pattern.add(iterations_to_run);
            load_pattern.add(pattern_1.get(0));
            load_pattern.add(pattern_1.get(1));
            load_pattern.add(crud_pattern.get(pattern_index));
            t_pattern.add(load_pattern);
            this.transaction_pattern.add(t_pattern);
            pattern_index++;
            if(pattern_index == t_pattern_len)
                pattern_index = 0;
        }
    }
}
