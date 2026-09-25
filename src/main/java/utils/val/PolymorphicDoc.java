package utils.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.couchbase.client.java.json.JsonObject;

import utils.docgen.WorkLoadSettings;
import utils.val.shapes.ShapeRegistry;
import utils.val.shapes.engine.ShapeWalker;

/**
 * Polymorphic document template: emits a skewed mixture of the document shapes
 * registered in META-INF/services/utils.val.shapes.DocShape.
 *
 * The valueType string selects which types are loaded and, optionally, their mix:
 *   "PolymorphicDoc"                          -> all types, at their declared weights
 *   "PolymorphicDoc:transaction"              -> only transaction documents
 *   "PolymorphicDoc:transaction,profile"      -> those two, declared weights renormalised
 *   "PolymorphicDoc:transaction=50,profile=50" -> those two, in equal measure
 *   "PolymorphicDoc:transaction,profile=35"   -> transaction keeps its declared weight
 *
 * Weights are relative, so "50,50" and "1,1" behave identically.
 *
 * Shapes are discovered at runtime, so adding a document type needs no change here.
 */
public class PolymorphicDoc {

    public static final String NAME = "PolymorphicDoc";
    private static final String TYPE_SEPARATOR = ":";

    public WorkLoadSettings ws;

    private final ShapeRegistry registry;

    public PolymorphicDoc() {
        super();
        this.registry = new ShapeRegistry(null);
    }

    public PolymorphicDoc(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.registry = new ShapeRegistry(typeFilter(ws.valueType));
    }

    /**
     * Parses the optional ":type1,type2=weight" suffix off the valueType string.
     * Returns type name -> weight override, where a null value means "use the declared weight",
     * or null when no suffix is present.
     */
    static Map<String, Integer> typeFilter(String valueType) {
        if (valueType == null || !valueType.contains(TYPE_SEPARATOR)) {
            return null;
        }
        String suffix = valueType.substring(valueType.indexOf(TYPE_SEPARATOR) + 1).trim();
        if (suffix.isEmpty()) {
            return null;
        }
        Map<String, Integer> types = new LinkedHashMap<String, Integer>();
        for (String entry : suffix.split(",")) {
            String token = entry.trim();
            if (token.isEmpty()) {
                continue;
            }
            int eq = token.indexOf('=');
            if (eq < 0) {
                types.put(token, null);
                continue;
            }
            String name = token.substring(0, eq).trim();
            String weight = token.substring(eq + 1).trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("Missing document type before '=' in: " + token);
            }
            int parsed;
            try {
                parsed = Integer.parseInt(weight);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Weight for '" + name + "' must be a whole number, got: " + weight);
            }
            if (parsed < 0) {
                throw new IllegalArgumentException("Weight for '" + name + "' must not be negative: " + parsed);
            }
            if (parsed == 0) {
                throw new IllegalArgumentException(
                        "Weight for '" + name + "' is 0, which would load none of it. Omit the type instead.");
            }
            types.put(name, parsed);
        }
        return types.isEmpty() ? null : types;
    }

    /**
     * Rejects an unusable valueType before the loader gets far enough to fail obscurely.
     * Throws IllegalArgumentException naming the problem and the available types.
     */
    public static void validate(String valueType) {
        new ShapeRegistry(typeFilter(valueType));
    }

    public JsonObject next(String key) {
        long seq = sequenceOf(key);
        Class<?> shapeClass = this.registry.pick(new Random(key.hashCode()));
        JsonObject doc = build(shapeClass, key, this.ws.mutated, seq);

        if (this.ws.mutated > 0 && this.ws.mutate_field != null && !this.ws.mutate_field.isEmpty()) {
            // mutate_field names the fields allowed to change; everything else reverts to the
            // document this key originally produced.
            JsonObject original = build(shapeClass, key, 0, seq);
            Set<String> mutable = new HashSet<String>(Arrays.asList(this.ws.mutate_field.split(",")));
            // Identity-derived fields move together: mutating an email but not the matching name
            // would leave the document describing two different people.
            Set<String> identityFields = ShapeWalker.identityFields(shapeClass);
            if (!Collections.disjoint(mutable, identityFields)) {
                mutable.addAll(identityFields);
            }
            for (String name : new ArrayList<String>(doc.getNames())) {
                if (!mutable.contains(name) && !original.containsKey(name)) {
                    doc.removeKey(name);
                }
            }
            for (String name : original.getNames()) {
                if (!mutable.contains(name)) {
                    doc.put(name, original.get(name));
                }
            }
            // The merge mixes two generations, so the id is rebuilt from the result.
            String id = ShapeWalker.resolveId(shapeClass, doc, seq);
            if (id != null) {
                doc.put(ShapeWalker.ID_FIELD, id);
            }
        }

        doc.put("mutate", this.ws.mutated);
        return doc;
    }

    private JsonObject build(Class<?> shapeClass, String key, int mutated, long seq) {
        // The shape is pinned to the key so a document keeps its type across updates;
        // only field values are regenerated when mutating.
        Random values = new Random(
                mutated > 0 ? (key + Integer.toString(mutated)).hashCode() : key.hashCode());
        return ShapeWalker.walk(shapeClass, new ShapeWalker.Ctx(values), seq);
    }

    /** Trailing digits of the key, so _id patterns carry the document's ordinal. */
    private static long sequenceOf(String key) {
        int end = key.length();
        int start = end;
        while (start > 0 && Character.isDigit(key.charAt(start - 1))) {
            start--;
        }
        if (start == end) {
            return Math.abs((long) key.hashCode()) % 10000000L;
        }
        try {
            return Long.parseLong(key.substring(start, end));
        } catch (NumberFormatException e) {
            return Math.abs((long) key.hashCode()) % 10000000L;
        }
    }
}
