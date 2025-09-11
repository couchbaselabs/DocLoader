package utils.docgen;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import utils.key.CircularKey;
import utils.key.RandomKey;
import utils.key.RandomSizeKey;
import utils.key.ReverseKey;
import utils.key.SimpleKey;
import utils.val.Cars;
import utils.val.Hotel;
import utils.val.HeterogeneousHotel;
import utils.val.MiniCars;
import utils.val.NimbusM;
import utils.val.NimbusP;
import utils.val.Product;
import utils.val.RandomlyNestedJson;
import utils.val.SimpleValue;
import utils.val.SimpleSubDocValue;
import utils.val.Vector;
import utils.val.anySizeValue;
import utils.val.siftBigANN;

import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInSpec;

abstract class KVGenerator{
    public WorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    protected Object keys;
    protected Object vals;
    private Class<?> keyInstance;
    private Class<?> valInstance;
    protected Method keyMethod;
    protected Method valMethod;
    protected Method subdocLookupMethod;
    protected Method iterationsMethod;
    // Total number of vbuckets to be considered for this load
    protected static int num_vbuckets = 1024;
    // List of target vbuckets to be considered for this load
    protected int[] target_vbuckets;

    // HashMap to hold pre-generated keys for target_vbucket key_gen
    protected Map<Long, String> generated_create_keys = new HashMap<Long, String>();
    protected Map<Long, String> generated_update_keys = new HashMap<Long, String>();
    protected Map<Long, String> generated_read_keys = new HashMap<Long, String>();
    protected Map<Long, String> generated_delete_keys = new HashMap<Long, String>();
    protected Map<Long, String> generated_expiry_keys = new HashMap<Long, String>();

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    public KVGenerator(WorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super();
        this.ws = ws;
        this.setKeyInstance(keyClass);
        this.setValInstance(valClass);

        try {
            this.keys = keyInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.vals = valInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.keyMethod = this.keyInstance.getDeclaredMethod("next", long.class);
            if (this.valInstance.getSimpleName().equals(SimpleSubDocValue.class.getSimpleName())) {
                this.valMethod = this.valInstance.getDeclaredMethod("next", String.class, String.class);
                this.subdocLookupMethod = this.valInstance.getDeclaredMethod("next_lookup", String.class);
            } else {
                this.valMethod = this.valInstance.getDeclaredMethod("next", String.class);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    public KVGenerator(WorkLoadSettings ws, String keyClass, String valClass,
                       int iterations) throws ClassNotFoundException {
        super();
        this.ws = ws;
        this.setKeyInstance(keyClass);
        this.setValInstance(valClass);
        try {
            if (this.keyInstance.getSimpleName().equals(CircularKey.class.getSimpleName())) {
                this.keys = keyInstance.getConstructor(WorkLoadSettings.class, int.class).newInstance(ws, iterations);
                this.iterationsMethod = this.keyInstance.getDeclaredMethod("checkIterations");
            }
            else
                this.keys = keyInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.vals = valInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.keyMethod = this.keyInstance.getDeclaredMethod("next", long.class);
            if (this.valInstance.getSimpleName().equals(SimpleSubDocValue.class.getSimpleName())) {
                this.valMethod = this.valInstance.getDeclaredMethod("next", String.class, String.class);
                this.subdocLookupMethod = this.valInstance.getDeclaredMethod("next_lookup", String.class);
            } else {
                this.valMethod = this.valInstance.getDeclaredMethod("next", String.class);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    private void setKeyInstance(String keyClass) {
        if(keyClass.equals(RandomKey.class.getSimpleName()))
            this.keyInstance = RandomKey.class;
        else if(keyClass.equals(ReverseKey.class.getSimpleName()))
            this.keyInstance = ReverseKey.class;
        else if(keyClass.equals(RandomSizeKey.class.getSimpleName()))
            this.keyInstance = RandomSizeKey.class;
        else if(keyClass.equals(CircularKey.class.getSimpleName()))
            this.keyInstance = CircularKey.class;
        else
            this.keyInstance = SimpleKey.class;
    }

    private void setValInstance(String valClass) {
        if(valClass.equals(anySizeValue.class.getSimpleName()))
            this.valInstance = anySizeValue.class;
        else if (valClass.equals(NimbusP.class.getSimpleName()))
            this.valInstance = NimbusP.class;
        else if (valClass.equals(NimbusM.class.getSimpleName()))
            this.valInstance = NimbusM.class;
        else if (valClass.equals(Hotel.class.getSimpleName()))
            this.valInstance = Hotel.class;
        else if (valClass.equals(HeterogeneousHotel.class.getSimpleName()))
            this.valInstance = HeterogeneousHotel.class;
        else if (valClass.equals(Cars.class.getSimpleName()))
            this.valInstance = Cars.class;
        else if (valClass.equals(MiniCars.class.getSimpleName()))
            this.valInstance = MiniCars.class;
        else if (valClass.equals(Vector.class.getSimpleName()))
            this.valInstance = Vector.class;
        else if (valClass.equals(Product.class.getSimpleName()))
            this.valInstance = Product.class;
        else if (valClass.equals(siftBigANN.class.getSimpleName()))
            this.valInstance = siftBigANN.class;
        else if (valClass.equals(SimpleSubDocValue.class.getSimpleName()))
            this.valInstance = SimpleSubDocValue.class;
        else if (valClass.equals(RandomlyNestedJson.class.getSimpleName()))
            this.valInstance = RandomlyNestedJson.class;
        else
            this.valInstance = SimpleValue.class;
    }

    public void set_vbuckets_config(int num_vbuckets, int[] target_vbuckets) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        this.num_vbuckets = num_vbuckets;
        this.target_vbuckets = target_vbuckets;
        Method set_total_vbs_func = this.keyInstance.getDeclaredMethod("set_total_vbs", int.class);
        set_total_vbs_func.invoke(this.keys, num_vbuckets);
    }

    public void generate_keys_for_target_vbs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method genKeysForTargetVBsFunc = this.keyInstance.getDeclaredMethod(
            "generate_keys_for_target_vbs", Long.class, Long.class, int[].class);

        if (target_vbuckets != null && target_vbuckets.length > 0) {
            generated_create_keys = (Map<Long, String>)genKeysForTargetVBsFunc.invoke(
                this.keys, this.ws.dr.create_s, this.ws.dr.create_e, this.target_vbuckets);
            generated_update_keys = (Map<Long, String>)genKeysForTargetVBsFunc.invoke(
                this.keys, this.ws.dr.update_s, this.ws.dr.update_e, this.target_vbuckets);
            generated_read_keys = (Map<Long, String>)genKeysForTargetVBsFunc.invoke(
                this.keys, this.ws.dr.read_s, this.ws.dr.read_e, this.target_vbuckets);
            generated_delete_keys = (Map<Long, String>)genKeysForTargetVBsFunc.invoke(
                this.keys, this.ws.dr.delete_s, this.ws.dr.delete_e, this.target_vbuckets);
            generated_expiry_keys = (Map<Long, String>)genKeysForTargetVBsFunc.invoke(
                this.keys, this.ws.dr.expiry_s, this.ws.dr.expiry_e, this.target_vbuckets);

            // Because we create '*_e' as 'n' keys,
            // hence update '*_e' var to track the iteration during next()
            this.ws.dr.create_e = this.ws.dr.create_s + this.ws.dr.create_e;
            this.ws.dr.update_e = this.ws.dr.update_s + this.ws.dr.update_e;
            this.ws.dr.read_e = this.ws.dr.read_s + this.ws.dr.read_e;
            this.ws.dr.delete_e = this.ws.dr.delete_s + this.ws.dr.delete_e;
            this.ws.dr.expiry_e = this.ws.dr.expiry_s + this.ws.dr.expiry_e;
        }
    }

    public boolean has_next_create() {
        if (this.ws.dr.createItr.get() < this.ws.dr.create_e)
            return true;
        return false;
    }

    public boolean has_next_read() {
        if (this.ws.dr.readItr.get() < this.ws.dr.read_e)
            return true;
        if (this.keyInstance.getSimpleName().equals(CircularKey.class.getSimpleName())) {
            try {
                if ((boolean)this.iterationsMethod.invoke(this.keys)) {
                    this.resetRead();
                    return true;
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        }
        return false;
    }

    public boolean has_next_update() {
        if (this.ws.dr.updateItr.get() < this.ws.dr.update_e)
            return true;
        if (this.keyInstance.getSimpleName().equals(CircularKey.class.getSimpleName())) {
            try {
                if ((boolean)this.iterationsMethod.invoke(this.keys)) {
                    this.resetUpdate();
                    this.ws.mutated += 1;
                    return true;
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        }
        if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())-startTime<ws.mutation_timeout) {
            this.resetUpdate();
            this.ws.mutated += 1;
            return true;
        }
        return false;
    }

    public boolean has_next_expiry() {
        if (this.ws.dr.expiryItr.get() < this.ws.dr.expiry_e)
            return true;
        if (this.keyInstance.getSimpleName().equals(CircularKey.class.getSimpleName())) {
            try {
                if ((boolean)this.iterationsMethod.invoke(this.keys, null)) {
                    this.resetExpiry();
                    return true;
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        }
        return false;
    }

    public boolean has_next_delete() {
        if (this.ws.dr.deleteItr.get() < this.ws.dr.delete_e)
            return true;
        return false;
    }

    public boolean has_next_subdoc(String op_type) {
        switch(op_type) {
            case "insert":
                if (this.ws.dr.subdocInsertItr.get() < this.ws.dr.subdocInsert_e)
                    return true;
                return false;
            case "upsert":
                if (this.ws.dr.subdocUpsertItr.get() < this.ws.dr.subdocUpsert_e)
                    return true;
                return false;
            case "remove":
                if (this.ws.dr.subdocRemoveItr.get() < this.ws.dr.subdocRemove_e)
                    return true;
                return false;
            case "lookup":
                if (this.ws.dr.subdocReadItr.get() < this.ws.dr.subdocRead_e)
                    return true;
                return false;
            default:
                return false;
        }
    }

    public boolean has_next_subdoc_upsert() {
        if (this.ws.dr.subdocUpsertItr.get() < this.ws.dr.subdocUpsert_e)
            return true;
        return false;
    }

    public boolean has_next_subdoc_remove() {
        if (this.ws.dr.subdocRemoveItr.get() < this.ws.dr.subdocRemove_e)
            return true;
        return false;
    }

    public boolean has_next_subdoc_lookup() {
        if (this.ws.dr.subdocReadItr.get() < this.ws.dr.subdocRead_e)
            return true;
        return false;
    }

    abstract Tuple2<String, Object> next();

    void resetRead() {
        this.ws.dr.readItr =  new AtomicLong(this.ws.dr.read_s);
    }

    void resetExpiry() {
        this.ws.dr.expiryItr.set(this.ws.dr.expiry_s);
    }

    void resetUpdate() {
        this.ws.dr.updateItr.set(this.ws.dr.update_s);
    }
}

public class DocumentGenerator extends KVGenerator{
    public DocumentGenerator(WorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super(ws, keyClass, valClass, 1);
    }

    public DocumentGenerator(WorkLoadSettings ws, String keyClass,
                             String valClass, int iterations) throws ClassNotFoundException {
        super(ws, keyClass, valClass, iterations);
    }

    public DocumentGenerator(WorkLoadSettings ws, String keyClass,
                             String valClass, int iterations, int num_vbuckets,
                             int[] target_vbuckets) throws ClassNotFoundException {
        super(ws, keyClass, valClass, iterations);
        try {
            this.set_vbuckets_config(num_vbuckets, target_vbuckets);
            this.generate_keys_for_target_vbs();
        }
        catch(NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public WorkLoadSettings get_work_load_settings() {
        return this.ws;
    }

    public Tuple2<String, Object> next() {
        long temp = this.ws.dr.createItr.getAndIncrement();
        String k = null;
        Object v = null;
            try {
                if (this.target_vbuckets != null && this.target_vbuckets.length > 0) {
                    k = this.generated_create_keys.get(temp);
                } else {
                    k = (String) this.keyMethod.invoke(this.keys, temp);
                }
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextRead() {
        long temp = this.ws.dr.readItr.getAndIncrement();
        String k = null;
        Object v = null;
            try {
                if (this.target_vbuckets != null && this.target_vbuckets.length > 0) {
                    k = this.generated_read_keys.get(temp);
                } else {
                    k = (String) this.keyMethod.invoke(this.keys, temp);
                }
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextUpdate() {
        long temp = this.ws.dr.updateItr.getAndIncrement();
        String k = null;
        Object v = null;
            try {
                if (this.target_vbuckets != null && this.target_vbuckets.length > 0) {
                    k = this.generated_update_keys.get(temp);
                } else {
                    k = (String) this.keyMethod.invoke(this.keys, temp);
                }
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextExpiry() {
        long temp = this.ws.dr.expiryItr.getAndIncrement();
        String k = null;
        Object v = null;
            try {
                if (this.target_vbuckets != null && this.target_vbuckets.length > 0) {
                    k = this.generated_expiry_keys.get(temp);
                } else {
                    k = (String) this.keyMethod.invoke(this.keys, temp);
                }
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, List<MutateInSpec>>nextSubDoc(String op_type) {
        long temp;
        String key = null;
        List<MutateInSpec> specs = null;

        switch(op_type) {
            case "insert":
                temp = this.ws.dr.subdocInsertItr.getAndIncrement();
                break;
            case "upsert":
                temp = this.ws.dr.subdocUpsertItr.getAndIncrement();
                break;
            case "remove":
                temp = this.ws.dr.subdocRemoveItr.getAndIncrement();
                break;
            case "lookup":
                temp = this.ws.dr.subdocReadItr.getAndIncrement();
                break;
            default:
                return null;
        }
        try {
            key = (String) this.keyMethod.invoke(this.keys, temp);
            specs = (List<MutateInSpec>) this.valMethod.invoke(this.vals, key, op_type);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
            e1.printStackTrace();
        }
        return Tuples.of(key, specs);
    }

    public Tuple2<String, List<LookupInSpec>>nextSubDocLookup() {
        String key = null;
        List<LookupInSpec> specs = null;
        long temp = this.ws.dr.subdocReadItr.getAndIncrement();
        try {
            key = (String) this.keyMethod.invoke(this.keys, temp);
            specs = (List<LookupInSpec>) this.subdocLookupMethod.invoke(this.vals, key);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
            e1.printStackTrace();
        }
        return Tuples.of(key, specs);
    }

    public List<Tuple2<String, Object>> nextInsertBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_create() && count<ws.batchSize*ws.creates/100) {
            docs.add(this.next());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextReadBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_read() && count<ws.batchSize*ws.reads/100) {
            docs.add(this.nextRead());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextUpdateBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_update() && count<ws.batchSize*ws.updates/100) {
            docs.add(this.nextUpdate());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextExpiryBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_expiry() && count<ws.batchSize*ws.expiry/100) {
            docs.add(this.nextExpiry());
            count += 1;
        }
        return docs;
    }

    public List<String> nextDeleteBatch() {
        int count = 0;
        long temp;
        List<String> docs = new ArrayList<String>();
        while (this.has_next_delete() && count<ws.batchSize*ws.deletes/100) {
            try {
                temp = this.ws.dr.deleteItr.getAndIncrement();
                if (this.target_vbuckets != null && this.target_vbuckets.length > 0) {
                    docs.add(this.generated_delete_keys.get(temp));
                } else {
                    docs.add((String) this.keyMethod.invoke(this.keys, temp));
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                e.printStackTrace();
            }
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, List<LookupInSpec>>> nextSubDocLookupBatch() {
        List<Tuple2<String, List<LookupInSpec>>> docs = new ArrayList<>();
        int count = 0;
        long temp;
        while (this.has_next_subdoc("lookup") && count<ws.batchSize*ws.subdocs/100) {
            docs.add(this.nextSubDocLookup());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, List<MutateInSpec>>> nextSubDocBatch(String op_type) {
        List<Tuple2<String, List<MutateInSpec>>> docs = new ArrayList<>();
        int count = 0;
        long temp;
        while (this.has_next_subdoc(op_type) && count<ws.batchSize*ws.subdocs/100) {
            docs.add(this.nextSubDoc(op_type));
            count += 1;
        }
        return docs;
    }
}
