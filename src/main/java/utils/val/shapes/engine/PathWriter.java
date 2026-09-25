package utils.val.shapes.engine;

import com.couchbase.client.java.json.JsonObject;

public final class PathWriter {

    private PathWriter() {
    }

    /** Reads the value at a dotted path, or null if any segment is absent. */
    public static Object get(JsonObject root, String dottedPath) {
        Object cursor = root;
        for (String segment : dottedPath.split("\\.")) {
            if (!(cursor instanceof JsonObject)) {
                return null;
            }
            cursor = ((JsonObject) cursor).get(segment);
        }
        return cursor;
    }

    /**
     * Writes value at a dotted path, creating intermediate objects as needed.
     * Sibling fields sharing a prefix reuse the same intermediate object rather than clobbering it.
     */
    public static void set(JsonObject root, String dottedPath, Object value) {
        String[] segments = dottedPath.split("\\.");
        JsonObject cursor = root;
        for (int i = 0; i < segments.length - 1; i++) {
            Object existing = cursor.get(segments[i]);
            if (existing instanceof JsonObject) {
                cursor = (JsonObject) existing;
            } else {
                JsonObject child = JsonObject.create();
                cursor.put(segments[i], child);
                cursor = child;
            }
        }
        String leaf = segments[segments.length - 1];
        if (value == null) {
            cursor.putNull(leaf);
        } else {
            cursor.put(leaf, value);
        }
    }
}
