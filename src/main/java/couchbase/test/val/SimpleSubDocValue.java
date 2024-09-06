package couchbase.test.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.LookupInSpec;

import couchbase.test.docgen.WorkLoadSettings;

public class SimpleSubDocValue {
    private final String[] firstNames = {"Adam", "James", "Sharon",
                                         "Jane", "Will", "Phil"};
    private final String[] lastNames = {"Roth", "Cook", "Jones",
                                        "Dave", "Taylor", "Moore"};
    private final String[] cities = {"Chicago", "Austin", "Denver", "Boston",
                                     "NewYork", "Atlanta", "Detroit", "Miami"};
    private final String[] states = {"AL", "CA", "IN", "NV", "NY", "MA", "KY"};
    private final int[] pin_codes = {135, 246, 396, 837, 007, 666, 999, 108};

    public WorkLoadSettings ws;

    public SimpleSubDocValue(WorkLoadSettings ws) {
        super();
        this.ws = ws;
    }

    private List<MutateInSpec> next_sys_xattr_insert_specs(Random random_obj) {
        boolean createPath = true;
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_fname", firstNames[f_name_index]));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_lname", lastNames[l_name_index]));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_city", cities[addr_city_index]));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_state", states[addr_state_index]));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_pincode", pin_codes[addr_pin_index]));
        return mutateInSpecs;
    }

    private List<MutateInSpec> next_insert_specs(Random random_obj) {
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        if(this.ws.is_subdoc_xattr) {
            if(this.ws.create_path) {
                mutateInSpecs.add(MutateInSpec.insert("full_name.first", firstNames[f_name_index]).createPath().xattr());
                mutateInSpecs.add(MutateInSpec.insert("full_name.last", lastNames[l_name_index]).createPath().xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.city", cities[addr_city_index]).createPath().xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.state", states[addr_state_index]).createPath().xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.pincode", pin_codes[addr_pin_index]).createPath().xattr());
            } else {
                mutateInSpecs.add(MutateInSpec.insert("full_name.first", firstNames[f_name_index]).xattr());
                mutateInSpecs.add(MutateInSpec.insert("full_name.last", lastNames[l_name_index]).xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.city", cities[addr_city_index]).xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.state", states[addr_state_index]).xattr());
                mutateInSpecs.add(MutateInSpec.insert("addr.pincode", pin_codes[addr_pin_index]).xattr());
            }
        } else {
            if(this.ws.create_path) {
                mutateInSpecs.add(MutateInSpec.insert("full_name.first", firstNames[f_name_index]).createPath());
                mutateInSpecs.add(MutateInSpec.insert("full_name.last", lastNames[l_name_index]).createPath());
                mutateInSpecs.add(MutateInSpec.insert("addr.city", cities[addr_city_index]).createPath());
                mutateInSpecs.add(MutateInSpec.insert("addr.state", states[addr_state_index]).createPath());
                mutateInSpecs.add(MutateInSpec.insert("addr.pincode", pin_codes[addr_pin_index]).createPath());
            } else {
                mutateInSpecs.add(MutateInSpec.insert("full_name.first", firstNames[f_name_index]));
                mutateInSpecs.add(MutateInSpec.insert("full_name.last", lastNames[l_name_index]));
                mutateInSpecs.add(MutateInSpec.insert("addr.city", cities[addr_city_index]));
                mutateInSpecs.add(MutateInSpec.insert("addr.state", states[addr_state_index]));
                mutateInSpecs.add(MutateInSpec.insert("addr.pincode", pin_codes[addr_pin_index]));
            }
            mutateInSpecs.add(MutateInSpec.increment("mutated", 1).createPath());
        }
        return mutateInSpecs;
    }

    private List<MutateInSpec> next_sys_xattr_upsert_specs(Random random_obj) {
        boolean createPath = true;
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_fname", "updated_sys_xattr_fname"));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_lname", "updated_sys_xattr_lname"));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_city", "updated_sys_xattr_city"));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_state", "updated_sys_xattr_state"));
        mutateInSpecs.add(MutateInSpec.insert("_sys_xattr_pincode", 0));
        return mutateInSpecs;
    }

    private List<MutateInSpec> next_upsert_specs(Random random_obj) {
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        if(this.ws.is_subdoc_xattr) {
            if(this.ws.create_path) {
                mutateInSpecs.add(MutateInSpec.upsert("full_name.first", "updated_xattr_fname").createPath().xattr());
                mutateInSpecs.add(MutateInSpec.upsert("full_name.last", "updated_xattr_lname").createPath().xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.city", "updated_xattr_city").createPath().xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.state", "updated_xattr_state").createPath().xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.pincode", 0).createPath().xattr());
            } else {
                mutateInSpecs.add(MutateInSpec.upsert("full_name.first", "updated_xattr_fname").xattr());
                mutateInSpecs.add(MutateInSpec.upsert("full_name.last", "updated_xattr_lname").xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.city", "updated_xattr_city").xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.state", "updated_xattr_state").xattr());
                mutateInSpecs.add(MutateInSpec.upsert("addr.pincode", 0).xattr());
            }
        } else {
            if(this.ws.create_path) {
                mutateInSpecs.add(MutateInSpec.upsert("full_name.first", "updated_xattr_fname").createPath());
                mutateInSpecs.add(MutateInSpec.upsert("full_name.last", "updated_xattr_lname").createPath());
                mutateInSpecs.add(MutateInSpec.upsert("addr.city", "updated_xattr_city").createPath());
                mutateInSpecs.add(MutateInSpec.upsert("addr.state", "updated_xattr_state").createPath());
                mutateInSpecs.add(MutateInSpec.upsert("addr.pincode", 0).createPath());
            } else {
                mutateInSpecs.add(MutateInSpec.upsert("full_name.first", "updated_xattr_fname"));
                mutateInSpecs.add(MutateInSpec.upsert("full_name.last", "updated_xattr_lname"));
                mutateInSpecs.add(MutateInSpec.upsert("addr.city", "updated_xattr_city"));
                mutateInSpecs.add(MutateInSpec.upsert("addr.state", "updated_xattr_state"));
                mutateInSpecs.add(MutateInSpec.upsert("addr.pincode", 0));
            }
            mutateInSpecs.add(MutateInSpec.increment("mutated", 1).createPath());
        }
        return mutateInSpecs;
    }

    private List<MutateInSpec> next_sys_xattr_remove_specs(Random random_obj) {
        boolean createPath = true;
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        mutateInSpecs.add(MutateInSpec.remove("_sys_xattr_fname"));
        mutateInSpecs.add(MutateInSpec.remove("_sys_xattr_lname"));
        mutateInSpecs.add(MutateInSpec.remove("_sys_xattr_city"));
        mutateInSpecs.add(MutateInSpec.remove("_sys_xattr_state"));
        mutateInSpecs.add(MutateInSpec.remove("_sys_xattr_pincode"));
        return mutateInSpecs;
    }

    private List<MutateInSpec> next_remove_specs(Random random_obj) {
        List<MutateInSpec> mutateInSpecs = new ArrayList<MutateInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        if(this.ws.is_subdoc_xattr) {
            mutateInSpecs.add(MutateInSpec.remove("full_name.first").xattr());
            mutateInSpecs.add(MutateInSpec.remove("full_name.last").xattr());
            mutateInSpecs.add(MutateInSpec.remove("addr.city").xattr());
            mutateInSpecs.add(MutateInSpec.remove("addr.state").xattr());
            mutateInSpecs.add(MutateInSpec.remove("addr.pincode").xattr());
        } else {
            mutateInSpecs.add(MutateInSpec.remove("full_name.first"));
            mutateInSpecs.add(MutateInSpec.remove("full_name.last"));
            mutateInSpecs.add(MutateInSpec.remove("addr.city"));
            mutateInSpecs.add(MutateInSpec.remove("addr.state"));
            mutateInSpecs.add(MutateInSpec.remove("addr.pincode"));
            mutateInSpecs.add(MutateInSpec.increment("mutated", 1).createPath());
        }
        return mutateInSpecs;
    }

    private List<LookupInSpec> next_sys_xattr_read_specs(Random random_obj) {
        boolean createPath = true;
        List<LookupInSpec> lookupInSpecs = new ArrayList<LookupInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        lookupInSpecs.add(LookupInSpec.get("_sys_xattr_fname"));
        lookupInSpecs.add(LookupInSpec.get("_sys_xattr_lname"));
        lookupInSpecs.add(LookupInSpec.get("_sys_xattr_city"));
        lookupInSpecs.add(LookupInSpec.get("_sys_xattr_state"));
        lookupInSpecs.add(LookupInSpec.get("_sys_xattr_pincode"));
        return lookupInSpecs;
    }

    private List<LookupInSpec> next_read_specs(Random random_obj) {
        List<LookupInSpec> lookupInSpecs = new ArrayList<LookupInSpec>();
        int f_name_index = random_obj.nextInt(firstNames.length);
        int l_name_index = random_obj.nextInt(lastNames.length);
        int addr_city_index = random_obj.nextInt(cities.length);
        int addr_state_index = random_obj.nextInt(states.length);
        int addr_pin_index = random_obj.nextInt(pin_codes.length);
        if(this.ws.is_subdoc_xattr) {
            lookupInSpecs.add(LookupInSpec.get("full_name.first").xattr());
            lookupInSpecs.add(LookupInSpec.get("full_name.last").xattr());
            lookupInSpecs.add(LookupInSpec.get("addr.city").xattr());
            lookupInSpecs.add(LookupInSpec.get("addr.state").xattr());
            lookupInSpecs.add(LookupInSpec.get("addr.pincode").xattr());
        } else {
            lookupInSpecs.add(LookupInSpec.get("full_name.first"));
            lookupInSpecs.add(LookupInSpec.get("full_name.last"));
            lookupInSpecs.add(LookupInSpec.get("addr.city"));
            lookupInSpecs.add(LookupInSpec.get("addr.state"));
            lookupInSpecs.add(LookupInSpec.get("addr.pincode"));
        }
        return lookupInSpecs;
    }

    public List<LookupInSpec> next_lookup(String key, String op_type) {
        Random random_obj = new Random();
        random_obj.setSeed(key.hashCode());
        if(this.ws.is_subdoc_sys_xattr)
            return this.next_sys_xattr_read_specs(random_obj);
        return this.next_read_specs(random_obj);
    }

    public List<MutateInSpec> next(String key) {
        // Dummy method to avoid failures in DocGen expecting similar signature
        return null;
    }

    public List<MutateInSpec> next(String key, String op_type) {
        Random random_obj = new Random();
        random_obj.setSeed(key.hashCode());
        switch(op_type) {
            case "insert":
                if(this.ws.is_subdoc_sys_xattr)
                    return this.next_sys_xattr_insert_specs(random_obj);
                else
                    return this.next_insert_specs(random_obj);
            case "upsert":
                if(this.ws.is_subdoc_sys_xattr)
                    return this.next_sys_xattr_upsert_specs(random_obj);
                else
                    return this.next_upsert_specs(random_obj);
            case "remove":
                if(this.ws.is_subdoc_sys_xattr)
                    return this.next_sys_xattr_remove_specs(random_obj);
                else
                    return this.next_remove_specs(random_obj);
        }
        return null;
    }
}
