package utils.val;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import utils.docgen.WorkLoadSettings;

import com.amazonaws.util.json.JSONException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Random;
import java.util.List;

public class MiniCars {

    private final WorkLoadSettings ws;
    Faker faker;
    private Random random;
    public String[] carManufacturer = {
            "Mercedes-Benz", "BMW", "Audi", "Volkswagen", "McLaren", "Rolls-Royce",
            "Bugatti", "Aston Martin", "Lamborghini", "Maybach"
    };
    public Predictor<String, float[]> predictor = null;
    public String[] carCategories = {"Sedan", "SUV", "Truck", "Coupe", "Convertible", "Hatchback", "Minivan",
            "Van", "Wagon", "Crossover"};

    public String carDescription = "This is a %s car. " +
            "This car has a rating of %d stars";

    public JsonArray hexToRgb(String hexCode) {
        Color color = Color.decode(hexCode);

        JsonArray rgb = JsonArray.create();
        rgb.add(color.getRed());
        rgb.add(color.getGreen());
        rgb.add(color.getBlue());
        return rgb;
    }
    public MiniCars(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        faker = new Faker();
        this.setEmbeddingsModel(ws.model);
    }

    public ArrayList<String> selectRandomItems(String[] array, int numberOfItems) {
        if (numberOfItems > array.length) {
            throw new IllegalArgumentException("Number of items to select cannot be greater than the length of the array");
        }
        // If the random value for numOfItems selected is 0 then return 1 item from the array
        if (numberOfItems == 0) {
            numberOfItems = 1;
        }

        ArrayList<String> selectedItems = new ArrayList<>();
        List<Integer> selectedIndices = new ArrayList<>();

        while (selectedItems.size() < numberOfItems) {
            int randomIndex = this.random.nextInt(array.length);
            if (!selectedIndices.contains(randomIndex)) {
                selectedIndices.add(randomIndex);
                selectedItems.add(array[randomIndex]);
            }
        }

        return selectedItems;
    }

    public JsonArray convertFloatVectorToJSON(float[] vector) throws JSONException {
        JsonArray jsonVal = JsonArray.create();
        for (float value: vector) jsonVal.add( value);
        return jsonVal;
    }

    // Function to reduce the dimensionality to 128
    private static float[] reduceTo128Dimensions(float[] embedding) {
        int targetDimension = 128;
        float[] reducedEmbedding = new float[targetDimension];
        int originalDimension = embedding.length;
        for (int i = 0; i < targetDimension; i++) {
            reducedEmbedding[i] = embedding[i % originalDimension];
        }
        return reducedEmbedding;
    }

    public void setEmbeddingsModel(String DJL_MODEL) {
        String DJL_PATH = "djl://ai.djl.huggingface.pytorch/" + DJL_MODEL;
        Criteria<String, float[]> criteria =
                Criteria.builder()
                        .setTypes(String.class, float[].class)
                        .optModelUrls(DJL_PATH)
                        .optEngine("PyTorch")
                        .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                        .optProgress(new ProgressBar())
                        .build();
        ZooModel<String, float[]> model = null;
        try {
            model = criteria.loadModel();
        } catch (ModelNotFoundException | MalformedModelException | IOException e) {
            e.printStackTrace();
        }
        this.predictor = model.newPredictor();
    }
    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);

        return bytes;
    }
    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
    }

    public JsonObject next(String key) {
        this.random = new Random();
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed(key.hashCode());
        int id = this.random.nextInt();
        int rating = this.random.nextInt(5);
        String manufacturer = carManufacturer[this.random.nextInt(carCategories.length)];
        String description = String.format(carDescription, manufacturer, rating);
        float[] descriptionVector = new float[0];
        if (this.ws.base64) {

        }
        try {
            descriptionVector = this.predictor.predict(description);
        } catch (TranslateException e) {
            e.printStackTrace();
        }
        if (this.ws.base64){
            String descriptionVectorJson = String.valueOf(convertToBase64Bytes(descriptionVector));
            jsonObject.put("descriptionVector", descriptionVectorJson);
        } else {
            JsonArray descriptionVectorJson;
            try {
                descriptionVectorJson = convertFloatVectorToJSON(descriptionVector);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            jsonObject.put("descriptionVector", descriptionVectorJson);
        }
        jsonObject.put("id", id);
        jsonObject.put("manufacturer", manufacturer);
        jsonObject.put("rating", rating);
        jsonObject.put("description", description);

        return jsonObject;
    }

}
