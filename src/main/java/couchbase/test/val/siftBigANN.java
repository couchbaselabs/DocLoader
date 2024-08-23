package couchbase.test.val;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.javafaker.Faker;

import couchbase.test.docgen.WorkLoadSettings;

public class siftBigANN {
    Faker faker;
    private Random random;
    public WorkLoadSettings ws;
//    t1M = {"vector": None, "size": [5, 6, 7, 8, 9, 10], "color": "green", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 1}
//    t2M = {"vector": None, "size": [6, 7, 8, 9, 10], "color": "green", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 2}
//    t5M = {"vector": None, "size": [7, 8, 9, 10], "color": "red", "brand": "Nike", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 2.5}
//    t10M = {"vector": None, "size": [8, 9, 10], "color": "red", "brand": "Adidas", "country": "USA", "category": "Shoes", "type": "Apparel", "avg_review": 3}
//    t20M = {"vector": None, "size": [9, 10], "color": "red", "brand": "Adidas", "country": "Canada", "category": "Shoes", "type": "Apparel", "avg_review": 3.5}
//    t50M = {"vector": None, "size": [10], "color": "red", "brand": "Adidas", "country": "Canada", "category": "Jeans", "type": "Apparel", "avg_review": 4}
//    t100M = {"vector": None, "color": "red", "brand": "Adidas", "country": "Canada", "category": "Jeans", "type": "Denim", "avg_review": 4.5}
    FileInputStream inputStream = null;
    File fh = null;
	private FileInputStream mutateInputStream = null;
	private long mutateCount;
	private int remainingCount;
    
    public siftBigANN(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        this.random.setSeed(ws.keyPrefix.hashCode());
        
        try {
            this.inputStream = new FileInputStream(this.ws.baseVectorsFilePath);
            if(this.ws.creates > 0) {
            	this.inputStream.skip(ws.dr.create_s * 132);
            	mutateCount = this.ws.dr.create_e - this.ws.dr.create_s;
            }
            else if(this.ws.updates > 0) {
            	this.inputStream.skip(ws.dr.update_s * 132 + this.ws.mutated * 132);
            	mutateCount = this.ws.dr.update_e - this.ws.dr.update_s - this.ws.mutated;
            	if(this.ws.mutated > 0)
            		this.mutateInputStream  = new FileInputStream(this.ws.baseVectorsFilePath);
            }
            remainingCount = this.ws.mutated;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);
        return bytes;
    }

    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
      }

    public Product1 next(String key) throws IOException {
        int id = Integer.parseInt(key.split("-")[1]) - 1;
        float[] vector =  new float[128];
        if(mutateCount > 0) {
            byte[] byteArray = new byte[(int) 4];
            inputStream.read(byteArray);
            int dim = ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if(dim > 128)
                System.out.println(dim);
            byte[] byteVector = new byte[(int) dim];
            inputStream.read(byteVector);
            int i = 0;
            for (byte b : byteVector) {
                vector[i++] = (float)Byte.toUnsignedInt(b);
            }
            mutateCount -= 1;
        } else if(remainingCount > 0) {
            byte[] byteArray = new byte[(int) 4];
            mutateInputStream.read(byteArray);
            int dim = ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if(dim > 128)
                System.out.println(dim);
            byte[] byteVector = new byte[(int) dim];
            mutateInputStream.read(byteVector);
            int i = 0;
            for (byte b : byteVector) {
                vector[i++] = (float)Byte.toUnsignedInt(b);
            }
        }
        if(ws.dr.create_s >= 0 && ws.dr.create_e <= 1000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(5, 6, 7, 8, 9, 10)), "green",
                    "Nike", "USA", "Shoes", "Apparel", 1.0f);
        if(ws.dr.create_s >= 1000000 && ws.dr.create_e <= 2000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(6, 7, 8, 9, 10)), "green",
                    "Nike", "USA", "Shoes", "Apparel", 1.5f);
        if(ws.dr.create_s >= 2000000 && ws.dr.create_e <= 5000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(6, 7, 8, 9, 10)), "red",
                    "Nike", "USA", "Shoes", "Apparel", 2.0f);
        if(ws.dr.create_s >= 5000000 && ws.dr.create_e <= 10000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(7, 8, 9, 10)), "red",
                    "Adidas", "USA", "Shoes", "Apparel", 2.5f);
        if(ws.dr.create_s >= 10000000 && ws.dr.create_e <= 20000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(8, 9, 10)), "red",
                    "Adidas", "Canada", "Shoes", "Apparel", 3.0f);
        if(ws.dr.create_s >= 20000000 && ws.dr.create_e <= 50000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(9, 10)), "red",
                    "Adidas", "Canada", "Jeans", "Apparel", 3.5f);
        if(ws.dr.create_s >= 50000000 && ws.dr.create_e <= 100000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(10)), "red",
                    "Adidas", "Canada", "Jeans", "Denim", 4.0f);
        if(ws.dr.create_s >= 100000000 && ws.dr.create_e <= 200000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList(10)), "red",
                    "Adidas", "Canada", "Jeans", "Denim", 4.5f);
        if(ws.dr.create_s >= 200000000 && ws.dr.create_e <= 500000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList()), "red",
                    "Adidas", "Canada", "Jeans", "Denim", 5.0f);
        if(ws.dr.create_s >= 500000000 && ws.dr.create_e == 1000000000)
            return new Product1(id, vector, new ArrayList<Integer>(Arrays.asList()), "red",
                    "Adidas", "Canada", "Jeans", "Denim", 10.0f);
        return null;
    }
    

    public class Product1 {
        @JsonProperty
        private int id;
        @JsonProperty
        private float[] embedding;
        @JsonProperty
        private ArrayList<Integer> size;
        @JsonProperty
        private String color;
        @JsonProperty
        private String brand;
        @JsonProperty
        private String country;
        @JsonProperty
        private String category;
        @JsonProperty
        private String type;
        @JsonProperty
        private Float review;

        @JsonCreator
        public
        Product1(
                @JsonProperty("id")int id,
                @JsonProperty("embedding") float[] vector,
                @JsonProperty("size") ArrayList<Integer> arrayList,
                @JsonProperty("color") String color,
                @JsonProperty("brand") String brand,
                @JsonProperty("country") String country,
                @JsonProperty("category") String category,
                @JsonProperty("type") String type,
                @JsonProperty("review") float review){
            this.id = id;
            this.embedding = vector;
            this.size = arrayList;
            this.color = color;
            this.brand = brand;
            this.country = country;
            this.category = category;
            this.type = type;
            this.review = review;
        }

        public float[] getEmbedding() {
            return embedding;
        }

        public void setEmbedding(float[] embedding) {
            this.embedding = embedding;
        }

        public ArrayList<Integer> getSize() {
            return size;
        }

        public void setSize(ArrayList<Integer> size) {
            this.size = size;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Float getReview() {
            return review;
        }

        public void setReview(Float review) {
            this.review = review;
        }
    }
}
