package utils.val;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Random;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import utils.docgen.WorkLoadSettings;

public class Product {
    Faker faker;
    private Random random;
    
    public WorkLoadSettings ws;
	private ArrayList<ArrayList<JsonObject>> reviews = new ArrayList<ArrayList<JsonObject>>();

    public void setReviewsArray() {
        int numReviews = this.random.nextInt(10);
        LocalDateTime now = LocalDateTime.now();
        ArrayList<JsonObject> temp = new ArrayList<JsonObject>();
        for (int n = 0; n <= numReviews; n++) {
            JsonObject review = JsonObject.create();
            review.put("author", faker.name().fullName());
            review.put("date", now.plus(n, ChronoUnit.WEEKS).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            JsonObject ratings = JsonObject.create();
            ratings.put("rating_value", this.random.nextInt(10));
            ratings.put("performance", this.random.nextInt(10));
            ratings.put("utility", this.random.nextInt(10));
            ratings.put("pricing", this.random.nextInt(10));
            ratings.put("build_quality", this.random.nextInt(10));
            review.put("ratings", ratings);
            temp.add(review);
        }
        this.reviews.add(temp);
    }
    
    public Product(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        faker = new Faker(this.random);
        for (int index=0; index<4096; index++) {
            this.setReviewsArray();
        }
    }

    public  ArrayList<String> buildProductCategory(int length) {
    	ArrayList<String> prodCategory = new ArrayList<String>();
    	for (int n = 0; n <= length; n++) {
    		prodCategory.add(faker.book().genre());
    	}
    	return prodCategory;
    }

    public ArrayList<String> buildProductImageLinks(int length) {
    	ArrayList<String> prodImgLinks = new ArrayList<String>();
    	for (int n = 0; n <= length; n++) {
    		prodImgLinks.add(faker.internet().url());
    	}
    	return prodImgLinks;
    }

    public ArrayList<String> buildProductFeatures(int length) {
    	ArrayList<String> prodFeatures = new ArrayList<String>();
    	for (int n = 0; n <= length; n++) {
    		prodFeatures.add(faker.book().genre());
    	}
    	return prodFeatures;
    }

    public JsonObject buildProductSpecs() {
    	JsonObject prodSpecs = JsonObject.create();
    	
    	prodSpecs.put("make", faker.book().author());
    	prodSpecs.put("model", faker.book().title());

    	return prodSpecs;
    }
    
    public JsonObject next(String key) {
        this.random = new Random();
        JsonObject jsonObject = JsonObject.create();
        this.random.setSeed((key).hashCode());
        if(this.ws.mutated > 0){
            this.random.setSeed((key+Integer.toString(this.ws.mutated)).hashCode());
        }
        int index = this.random.nextInt(4096);
        jsonObject.put("id", "P" + key);
        jsonObject.put("product_name", faker.name().firstName());
        jsonObject.put("product_link", faker.internet().url());
        jsonObject.put("product_features", this.buildProductFeatures(1 + this.random.nextInt(3)));
        jsonObject.put("product_specs", this.buildProductSpecs());
        jsonObject.put("product_image_links", this.buildProductImageLinks(1 + this.random.nextInt(3)));
        jsonObject.put("product_reviews", this.reviews.get(index));
        jsonObject.put("product_category", this.buildProductCategory(1 + this.random.nextInt(3)));
        jsonObject.put("price", 10 + 10000*this.random.nextFloat());
        jsonObject.put("upload_date", faker.date().birthday(1, 65).toString());
        jsonObject.put("avg_rating", 1 + 4*this.random.nextFloat());

        jsonObject.put("num_sold", this.random.nextInt(50000));
        if (this.random.nextBoolean())
            jsonObject.put("num_sold", Integer.toString(this.random.nextInt(50000)));

        jsonObject.put("weight", 0.1 + 1000*this.random.nextDouble());
        if (this.random.nextBoolean())
            jsonObject.put("weight", Double.toString(0.1 + 1000*this.random.nextDouble()));

        jsonObject.put("quantity", 1 + this.random.nextInt(50000));
        if (this.random.nextBoolean())
            jsonObject.put("quantity", Integer.toString(1 + this.random.nextInt(50000)));

        jsonObject.put("seller_name", faker.beer().name());
        jsonObject.put("seller_location", faker.address().cityName() + " , " + faker.address().country());
        jsonObject.put("seller_verified", faker.bool().bool());
        jsonObject.put("template_name", "Product");
        jsonObject.put("mutated", this.ws.mutated);
        return jsonObject;
    }
}
