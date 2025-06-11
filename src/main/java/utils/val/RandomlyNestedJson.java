package utils.val;

import com.couchbase.client.java.json.JsonObject;

import utils.docgen.WorkLoadSettings;

import com.couchbase.client.java.json.JsonArray;
import java.util.Random;

public class RandomlyNestedJson {

    private static final Random random = new Random();
    private static final int MAX_DEPTH = 5; // Maximum nesting depth
    private static final int MAX_KEYS = 5; // Maximum keys per object
    private static final int MAX_ARRAY_ELEMENTS = 4; // Maximum elements per array

    public WorkLoadSettings ws;

    public RandomlyNestedJson(WorkLoadSettings ws) {
        this.ws = ws;
    }

    public JsonObject next(String key) {
        return createRandomJsonObject(0);
    }

    public static JsonObject createRandomJsonObject(int currentDepth) {
        JsonObject jsonObject = JsonObject.create();

        // Randomly decide if to add nested objects or arrays
        int numKeys = random.nextInt(MAX_KEYS) + 1; // At least one key

        for (int i = 0; i < numKeys; i++) {
            String key = "key_" + i; // Random key name

            if (currentDepth < MAX_DEPTH && random.nextBoolean()) { // Randomly nest deeper
                jsonObject.put(key, createRandomJsonObject(currentDepth + 1));
            } else {
                // Add a random primitive value
                int type = random.nextInt(8); // 0: String, 1: Integer, 2: Boolean, 3: Double, 4: Float, 5: Long, 6: NULL
                switch (type) {
                    case 0:
                        jsonObject.put(key, "value_" + random.nextInt(100));
                        break;
                    case 1:
                        jsonObject.put(key, random.nextInt(1000));
                        break;
                    case 2:
                        jsonObject.put(key, random.nextBoolean());
                        break;
                    case 3:
                        jsonObject.put(key, random.nextDouble());
                        break;
                    case 4:
                        jsonObject.put(key, random.nextFloat());
                        break;
                    case 5:
                        jsonObject.put(key, random.nextLong());
                        break;
                    case 6:
                        jsonObject.put(key, jsonObject.NULL);
                        break;
                    case 7:
                        jsonObject.put(key, createRandomJsonArray(MAX_DEPTH-2));
                        break;
                }
            }
        }
        return jsonObject;
    }

    public static JsonArray createRandomJsonArray(int currentDepth) {
        JsonArray jsonArray = JsonArray.create();
        int numElements = random.nextInt(MAX_ARRAY_ELEMENTS) + 1;

        for (int i = 0; i < numElements; i++) {
            int type = random.nextInt(4);
            if (currentDepth < MAX_DEPTH && random.nextBoolean()) {
            switch (type) {
                case 0:
                    jsonArray.add("array_value_" + random.nextInt(100));
                    break;
                case 1:
                    jsonArray.add(random.nextInt(1000));
                    break;
                case 2:
                    jsonArray.add(random.nextBoolean());
                    break;
                case 3:
                    jsonArray.add(createRandomJsonArray(currentDepth+1));
                    break;
                }
            }
        }
        return jsonArray;
    }
}