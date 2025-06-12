/*
Primitive type: BOOLEAN, STRING, BIGINT, or DOUBLE
Special type: NULL or MISSING
Composite type: OBJECT, array, or MULTISET
*/

package utils.val;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import utils.docgen.WorkLoadSettings;

public class HeterogeneousHotel {
    Faker faker;
    private Random random;
    private ArrayList<String> addresses = new ArrayList<String>();
    private ArrayList<String> city = new ArrayList<String>();
    private ArrayList<String> country = new ArrayList<String>();
    private List<String> htypes = Arrays.asList("Inn", "Hostel", "Place", "Center", "Hotel", "Motel", "Suites");
    private ArrayList<String> emails = new ArrayList<String>();
    private ArrayList<ArrayList<String>> likes = new ArrayList<ArrayList<String>>();
    // private ArrayList<String> names = new ArrayList<String>();
    private ArrayList<String> url = new ArrayList<String>();
    private ArrayList<ArrayList<JsonObject>> reviews = new ArrayList<ArrayList<JsonObject>>();
    private int mutate;
    private List<String> mutate_field_list = new ArrayList<>();

    public WorkLoadSettings ws;
    private float[] flt_buf;
    private int flt_buf_length;

    private double heterogeneity = 1;
    private int hotelRoundRobinIndex = 0;

    public HeterogeneousHotel() {
        super();
    }

    public HeterogeneousHotel(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        faker = new Faker(random);
        for (int index = 0; index < 4096; index++) {
            addresses.add(faker.address().streetAddress());
            city.add(faker.address().city());
            country.add(faker.address().country());
            String fn = faker.name().firstName();
            String ln = faker.name().lastName();
            // names.add(faker.name().fullName());
            emails.add(fn + '.' + ln + "@heterogeneoushotels.com");
            country.add(faker.address().country());

            ArrayList<String> temp = new ArrayList<String>();
            int numLikes = this.random.nextInt(10);
            for (int n = 0; n <= numLikes; n++) {
                temp.add(faker.name().fullName());
            }
            this.likes.add(temp);
            url.add(faker.internet().url());
            this.setReviewsArray();
        }
        this.flt_buf = new float[1024 * 1024];

        for (int index = 0; index < 1024 * 1024; index++) {
            float x = this.random.nextFloat();
            this.flt_buf[index] = x;
        }
        this.flt_buf_length = this.flt_buf.length;
    }

    public void setReviewsArray() {
        int numReviews = this.random.nextInt(10);
        LocalDateTime now = LocalDateTime.now();
        ArrayList<JsonObject> temp = new ArrayList<JsonObject>();
        for (int n = 0; n <= numReviews; n++) {
            JsonObject review = JsonObject.create();
            review.put("author", faker.name().fullName());
            review.put("date",
                    now.plus(n, ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            JsonObject ratings = JsonObject.create();
            ratings.put("Check in / front desk", this.random.nextInt(5));
            ratings.put("Cleanliness", this.random.nextInt(5));
            ratings.put("Overall", this.random.nextInt(5));
            ratings.put("Rooms", this.random.nextInt(5));
            ratings.put("Value", this.random.nextInt(5));
            review.put("ratings", ratings);
            temp.add(review);
        }
        this.reviews.add(temp);
    }

    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);
        return bytes;
    }

    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
    }

    private float[] get_float_array(int length, Random random_obj) {
        int _slice = random_obj.nextInt(this.flt_buf_length - length);
        // return flt_buf_al.subList(_slice, _slice + length);
        return Arrays.copyOfRange(this.flt_buf, _slice, _slice + length);
    }

    private Object getPrice() {
        double stringProbability = 0.5 * this.heterogeneity;
        Object price = 500 + this.random.nextInt(1500); // between 500 and 1500
        if (this.random.nextDouble() < stringProbability) {
            price = String.valueOf(price);
        }
        return price;
    }

    private Object getName() {
        double dictProbability = 0.5 * this.heterogeneity;
        String fn = faker.name().firstName();
        String ln = faker.name().lastName();
        
        Object name = fn + " " + ln;
        if (this.random.nextDouble() < dictProbability) {
            Map<String, Object> nameMap = new HashMap<>();
            nameMap.put("firstname", fn);
            nameMap.put("lastname", ln);
            name = nameMap;
        }
        return name;
    }

    private Object getEmail() {
        double dictProbability = (1.0 / 3.0) * this.heterogeneity;  // Smoothly scales from 0 to 1/3

        String email = faker.internet().emailAddress();

        double randomValue = this.random.nextDouble();
        if (this.heterogeneity == 0.0 || randomValue < 1 - 2 * dictProbability) {
            // Always string (if heterogeneity == 0) or chosen as string
            return email;
        } else if (randomValue < 1 - dictProbability) {
            // Return null
            return null;
        } else {
            // Indicate *missing* by returning a special marker
            return "MISSING_FIELD";  // Assuming you have a static final Object MISSING_FIELD
        }
    }

    private Object getAvgRating() {
        double dictProbability = (1.0 / 3.0) * this.heterogeneity;  // Smoothly scales from 0 to 1/3
        
        double rating = this.random.nextDouble() * 5.0;  // Example: 0.0 to 5.0
        
        double randomValue = this.random.nextDouble();
        if (this.heterogeneity == 0.0 || randomValue < 1 - 2 * dictProbability) {
            // Always double (if heterogeneity == 0) or chosen as double
            return rating;
        } else if (randomValue < 1 - dictProbability) {
            // Return string version of double
            return String.format("%.2f", rating);
        } else {
            // Return null
            return null;
        }
    }

    private Object getOverallRating() {
        int[] possibleRatings = {1, 2, 3, 4, 5};
        int index = hotelRoundRobinIndex % 5;
        hotelRoundRobinIndex++;
        
        return possibleRatings[index];
    }

    private Object freeParking() {
        double dictProbability = (1.0 / 3.0) * this.heterogeneity;
        boolean boolValue = this.random.nextBoolean();

        double randomValue = this.random.nextDouble();
        if (this.heterogeneity == 0.0 || randomValue < 1 - 2 * dictProbability) {
            return boolValue;
        } else if (randomValue < 1 - dictProbability) {
            return null;
        } else {
            return "MISSING_FIELD";
        }
    }

    private Object publicLikes() {
        int outerSize = 5 + this.random.nextInt(4);  // random(5,8)

        List<Object> outerList = new ArrayList<>();
        for (int i = 0; i < outerSize; i++) {
            if (this.heterogeneity == 1.0 && this.random.nextBoolean()) {
                // Nested list of strings (one level deep)
                int innerSize = 2 + this.random.nextInt(3);  // random(2,4) for variety
                List<String> innerList = new ArrayList<>();
                for (int j = 0; j < innerSize; j++) {
                    innerList.add(faker.lorem().word());
                }
                outerList.add(innerList);
            } else {
                // Simple string
                outerList.add(faker.lorem().word());
            }
        }
        return outerList;
    }


    public JsonObject next(String key) {
        this.random = new Random();
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed((key).hashCode());
        if (this.ws.mutated > 0) {
            this.random.setSeed((key + Integer.toString(this.ws.mutated)).hashCode());
        }
        int index = this.random.nextInt(4096);
        jsonObject.put("free_breakfast", this.random.nextBoolean());
        Object freeParking = freeParking();
        if (freeParking != "MISSING_FIELD") {
            jsonObject.put("free_parking", freeParking);
        }
        jsonObject.put("phone", faker.phoneNumber().phoneNumber());
        jsonObject.put("name", getName());
        jsonObject.put("price", getPrice());
        jsonObject.put("avg_rating", getAvgRating());
        jsonObject.put("overall_rating", getOverallRating());
        jsonObject.put("address", this.addresses.get(index));
        jsonObject.put("city", this.city.get(index));
        jsonObject.put("country", this.country.get(index));
        Object email = getEmail();
        if (email != "MISSING_FIELD") {
            jsonObject.put("email", email);
        }
        jsonObject.put("public_likes", publicLikes());
        jsonObject.put("reviews", this.reviews.get(index));
        jsonObject.put("type", this.htypes.get(index % htypes.size()));
        jsonObject.put("url", this.url.get(index));
        jsonObject.put("mutate", this.mutate);
        if (this.ws.mutated > 0 && !this.ws.mutate_field.isEmpty()) {
            this.random.setSeed((key).hashCode());
            index = this.random.nextInt(4096);
            mutate_field_list = Arrays.asList(this.ws.mutate_field.split(","));
            if (!mutate_field_list.contains("address"))
                jsonObject.put("address", this.addresses.get(index));
            if (!mutate_field_list.contains("city"))
                jsonObject.put("city", this.city.get(index));
            if (!mutate_field_list.contains("country"))
                jsonObject.put("country", this.country.get(index));
            if (!mutate_field_list.contains("email"))
                jsonObject.put("email", getEmail());
            if (!mutate_field_list.contains("name"))
                jsonObject.put("name", getName());
            if (!mutate_field_list.contains("public_likes"))
                jsonObject.put("public_likes", this.likes.get(index));
            if (!mutate_field_list.contains("reviews"))
                jsonObject.put("reviews", this.reviews.get(index));
            if (!mutate_field_list.contains("type"))
                jsonObject.put("type", this.htypes.get(index % htypes.size()));
            if (!mutate_field_list.contains("url"))
                jsonObject.put("url", this.url.get(index));
            if (!mutate_field_list.contains("free_breakfast"))
                jsonObject.put("free_breakfast", this.random.nextBoolean());
            if (!mutate_field_list.contains("free_parking"))
                jsonObject.put("free_parking", this.random.nextBoolean());
            if (!mutate_field_list.contains("phone"))
                jsonObject.put("phone", faker.phoneNumber().phoneNumber());
            if (!mutate_field_list.contains("price"))
                jsonObject.put("price", getPrice());
            if (!mutate_field_list.contains("avg_rating"))
                jsonObject.put("avg_rating", this.random.nextFloat() * 5);
        }
        jsonObject.put("mutate", this.ws.mutated);

        if (this.ws.mockVector) {
            float[] vector = null;
            vector = this.get_float_array(this.ws.dim, this.random);
            if (this.ws.base64)
                jsonObject.put("vector", convertToBase64Bytes(vector));
            else {
                JsonArray floatVector = JsonArray.create();
                for (int i = 0; i < vector.length; i++) {
                    floatVector.add(Float.valueOf(vector[i]));
                }
                jsonObject.put("vector", floatVector);
            }
        }
        return jsonObject;
    }
}
