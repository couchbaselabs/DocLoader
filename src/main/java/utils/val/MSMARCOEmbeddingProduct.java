package utils.val;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import utils.docgen.WorkLoadSettings;

public class MSMARCOEmbeddingProduct implements Closeable {

    // 3 step ranges for 8,841,823 vectors
    private static final long[] STEPS = new long[] {
        0, 100000, 1000000, 8841823
    };

    public WorkLoadSettings ws;
    private final String sourcePath;

    // Records come from a binary sidecar built once from the .vec source. Reading
    // primitives back costs no text parsing, which is what previously dominated this
    // loader's allocation; see SparseVectorStore for the detail.
    private SparseVectorStore store;

    private long rangeStart;
    private long rangeEnd;
    private int rangeSize;

    private long workerStartRecord;
    private long currentRecord;
    private boolean isMutation;

    public MSMARCOEmbeddingProduct(WorkLoadSettings ws) {
        this.ws = ws;
        this.sourcePath = resolveSourcePath(ws);

        try {
            this.store = SparseVectorStore.open(sourcePath);

            if (ws.creates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.create_s);
                this.workerStartRecord = ws.dr.create_s;
                this.isMutation = false;
                this.currentRecord = ws.dr.create_s;
            } else if (ws.updates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.update_s);
                this.workerStartRecord = ws.dr.update_s;
                this.isMutation = true;
                this.currentRecord = rangeStart + ((workerStartRecord - rangeStart + ws.mutated) % rangeSize);
            } else if (ws.expiry > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.expiry_s);
                this.workerStartRecord = ws.dr.expiry_s;
                this.isMutation = true;
                this.currentRecord = ws.dr.expiry_s;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize MSMARCO embedding source: " + e.getMessage(), e);
        }
    }

    private void initRangeBounds(long docIndex) {
        for (int i = 0; i < STEPS.length - 1; i++) {
            if (docIndex >= STEPS[i] && docIndex < STEPS[i + 1]) {
                this.rangeStart = STEPS[i];
                this.rangeEnd = STEPS[i + 1];
                this.rangeSize = (int) (rangeEnd - rangeStart);
                return;
            }
        }
        throw new IllegalArgumentException("docIndex " + docIndex + " outside STEPS bounds");
    }

    public synchronized Object next(String key) throws IOException {
        int keyNum = Integer.parseInt(key.split("-")[key.split("-").length - 1]);
        int id = keyNum + this.ws.mutated;

        if (isMutation) {
            currentRecord = rangeStart + ((keyNum - rangeStart + ws.mutated) % rangeSize);
        }

        Object sparseEmbedding = store.read(currentRecord);
        currentRecord++;

        // Metadata based on range boundaries
        if (rangeStart >= STEPS[0] && rangeEnd <= STEPS[1])
            return createProduct(id, sparseEmbedding, 5, "Green", "Nike", "USA", "Shoes", "Casual", 1.0f);
        if (rangeStart >= STEPS[1] && rangeEnd <= STEPS[2])
            return createProduct(id, sparseEmbedding, 6, "Green", "Nike", "USA", "Shoes", "Formal", 1.0f);
        if (rangeStart >= STEPS[2] && rangeEnd <= STEPS[3])
            return createProduct(id, sparseEmbedding, 7, "Green", "Nike", "USA", "Jeans", "Formal", 1.0f);
        // if (rangeStart >= STEPS[3] && rangeEnd <= STEPS[4])
        //     return createProduct(id, sparseEmbedding, 8, "Blue", "Adidas", "USA", "Shoes", "Casual", 1.0f);
        // if (rangeStart >= STEPS[4] && rangeEnd <= STEPS[5])
        //     return createProduct(id, sparseEmbedding, 9, "Purple", "Puma", "Canada", "Shoes", "Casual", 1.0f);
        // if (rangeStart >= STEPS[5] && rangeEnd <= STEPS[6])
        //     return createProduct(id, sparseEmbedding, 10, "Pink", "Asics", "Australia", "Jeans", "Casual", 1.0f);
        // if (rangeStart >= STEPS[6] && rangeEnd <= STEPS[7])
        //     return createProduct(id, sparseEmbedding, 11, "Yellow", "Brook", "England", "Shirt", "Formal", 1.0f);
        // if (rangeStart >= STEPS[7] && rangeEnd <= STEPS[8])
        //     return createProduct(id, sparseEmbedding, 12, "Brown", "Hoka", "India", "Shorts", "Sports", 2.0f);
        // if (rangeStart >= STEPS[8] && rangeEnd <= STEPS[9])
        //     return createProduct(id, sparseEmbedding, 13, "Magenta", "New Balance", "Mexico", "Bottoms", "Sneakers", 5.0f);
        // if (rangeStart >= STEPS[9] && rangeEnd <= STEPS[10])
        //     return createProduct(id, sparseEmbedding, 14, "Indigo", "Vans", "France", "Top", "Sandals", 10.0f);

        return null;
    }

    private Object createProduct(int id, Object embedding, int size, String color, String brand,
                                  String country, String category, String type, float review) {
        if (ws.base64) {
            String encodedEmbedding = encodeSparseToBase64(embedding);
            return new Product2(id, encodedEmbedding, size, color, brand, country, category, type, review, ws.mutated);
        }
        return new Product1(id, embedding, size, color, brand, country, category, type, review, ws.mutated);
    }

    private static String encodeSparseToBase64(Object sparseEmbedding) {
        Object[] embedding = (Object[]) sparseEmbedding;
        int[] indices = (int[]) embedding[0];
        float[] values = (float[]) embedding[1];
        int size = indices.length;
        ByteBuffer bb = ByteBuffer.allocate(4 + size * 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(size);
        for (int i = 0; i < size; i++) {
            bb.putInt(indices[i]);
            bb.putFloat(values[i]);
        }
        return Base64.getEncoder().encodeToString(bb.array());
    }
    private static String resolveSourcePath(WorkLoadSettings ws) {
        if (notBlank(ws.embeddingFilePath)) {
            return ws.embeddingFilePath;
        }
        if (notBlank(ws.baseVectorsFilePath)) {
            return ws.baseVectorsFilePath;
        }
        throw new IllegalArgumentException("Embedding source path is missing. Set embeddingFilePath or baseVectorsFilePath in WorkLoadSettings");
    }

    private static boolean notBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    public static long[] getSteps() {
        return STEPS;
    }

    public class Product1 {
        @JsonProperty
        private int id;
        @JsonProperty
        private Object sparse;
        @JsonProperty
        private int size;
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
        private float review;
        @JsonProperty
        private int mutate;
        @JsonCreator
        public Product1(
                @JsonProperty("idx") int id,
                @JsonProperty("sparse") Object sparse,
                @JsonProperty("size") int size,
                @JsonProperty("color") String color,
                @JsonProperty("brand") String brand,
                @JsonProperty("country") String country,
                @JsonProperty("category") String category,
                @JsonProperty("type") String type,
                @JsonProperty("review") float review,
                @JsonProperty("mutate") int mutate) {
            this.id = id;
            this.sparse = sparse;
            this.size = size;
            this.color = color;
            this.brand = brand;
            this.country = country;
            this.category = category;
            this.type = type;
            this.review = review;
            this.mutate = mutate;
        }
    }
    public class Product2 {
        @JsonProperty
        private int id;
        @JsonProperty
        private String sparse;
        @JsonProperty
        private int size;
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
        private float review;
        @JsonProperty
        private int mutate;
        @JsonCreator
        public Product2(
                @JsonProperty("idx") int id,
                @JsonProperty("sparse") String sparse,
                @JsonProperty("size") int size,
                @JsonProperty("color") String color,
                @JsonProperty("brand") String brand,
                @JsonProperty("country") String country,
                @JsonProperty("category") String category,
                @JsonProperty("type") String type,
                @JsonProperty("review") float review,
                @JsonProperty("mutate") int mutate) {
            this.id = id;
            this.sparse = sparse;
            this.size = size;
            this.color = color;
            this.brand = brand;
            this.country = country;
            this.category = category;
            this.type = type;
            this.review = review;
            this.mutate = mutate;
        }
    }
}
