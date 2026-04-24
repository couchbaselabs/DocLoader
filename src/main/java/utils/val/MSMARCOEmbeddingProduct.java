package utils.val;
 
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
 
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
 
import utils.docgen.WorkLoadSettings;
 
public class MSMARCOEmbeddingProduct implements Closeable {
    private static final int STREAM_BUFFER_SIZE = 1024 * 1024;
 
    // 10 step ranges for 8,841,823 vectors (same proportions as SIFT 1B)
    private static final long[] STEPS = new long[] {
        0, 100000,1000000,8841823
    };
 
    public WorkLoadSettings ws;
    private final String sourcePath;
 
    private BufferedReader textReader;
    
    // Range-preserving cyclic mutation support
    private long rangeStart;
    private long rangeEnd;
    private int rangeSize;
    private List<Object> rangeEmbeddingsCache;
    private int cyclicIndex;
 
    public MSMARCOEmbeddingProduct(WorkLoadSettings ws) {
        this.ws = ws;
        this.sourcePath = resolveSourcePath(ws);
 
        try {
            openStream();
            if (ws.creates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.create_s);
                skipRecords(textReader, ws.dr.create_s);
            } else if (ws.updates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.update_s);
                loadRangeEmbeddings();
                this.cyclicIndex = (int) ((ws.dr.update_s - rangeStart + ws.mutated) % rangeSize);
            } else if (ws.expiry > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.expiry_s);
                loadRangeEmbeddings();
                this.cyclicIndex = (int) ((ws.dr.expiry_s - rangeStart) % rangeSize);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize MSMARCO embedding stream: " + e.getMessage(), e);
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
    
    private void loadRangeEmbeddings() throws IOException {
        this.rangeEmbeddingsCache = new ArrayList<>(rangeSize);
        skipRecords(textReader, rangeStart);
        for (int i = 0; i < rangeSize; i++) {
            rangeEmbeddingsCache.add(readNextEmbedding(textReader));
        }
        textReader.close();
        textReader = null;
    }
 
    public synchronized Object next(String key) throws IOException {
        int id = Integer.parseInt(key.split("-")[key.split("-").length - 1]) + this.ws.mutated;
        Object sparseEmbedding;
 
        if (rangeEmbeddingsCache != null) {
            // Cyclic access within range for updates/expiry
            sparseEmbedding = rangeEmbeddingsCache.get(cyclicIndex);
            cyclicIndex = (cyclicIndex + 1) % rangeSize;
        } else {
            // Sequential read for creates
            sparseEmbedding = readNextEmbedding(textReader);
        }
 
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
 
    private Object readNextEmbedding(BufferedReader reader) throws IOException {
        if (reader == null) {
            throw new IOException("Reader is null");
        }
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("No more embedding records available from source: " + sourcePath);
        }
        line = line.trim();
        while (line.isEmpty()) {
            line = reader.readLine();
            if (line == null) {
                throw new IOException("No more embedding records available from source: " + sourcePath);
            }
            line = line.trim();
        }
 
        String[] parts = line.split("\t");
        if (parts.length != 3) {
            throw new IOException("Invalid .vec format: expected 3 tab-separated parts, got " + parts.length);
        }
 
        String indicesStr = parts[1].trim();
        indicesStr = indicesStr.substring(1, indicesStr.length() - 1);
        int[] indices = Arrays.stream(indicesStr.split(","))
                .mapToInt(s -> Integer.parseInt(s.trim()))
                .toArray();
 
        String valuesStr = parts[2].trim();
        valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
        String[] valueParts = valuesStr.split(",");
        if (valueParts.length != indices.length) {
            throw new IOException("Indices and values arrays have different lengths: "
                    + indices.length + " vs " + valueParts.length);
        }
        float[] values = new float[indices.length];
        for (int i = 0; i < valueParts.length; i++) {
            values[i] = Float.parseFloat(valueParts[i].trim());
        }
 
        return createSparseEmbedding(indices, values);
    }
 
    private Object createSparseEmbedding(int[] indices, float[] values) {
        List<Object> result = new ArrayList<>(2);
        List<Integer> indicesList = new ArrayList<>(indices.length);
        for (int idx : indices) {
            indicesList.add(idx);
        }
        List<Float> valuesList = new ArrayList<>(values.length);
        for (float val : values) {
            valuesList.add(val);
        }
        result.add(indicesList);
        result.add(valuesList);
        return result;
    }
 
    @SuppressWarnings("unchecked")
    private static String encodeSparseToBase64(Object sparseEmbedding) {
        List<Object> embedding = (List<Object>) sparseEmbedding;
        List<Integer> indices = (List<Integer>) embedding.get(0);
        List<Float> values = (List<Float>) embedding.get(1);
 
        int size = indices.size();
        ByteBuffer bb = ByteBuffer.allocate(4 + size * 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(size);
        for (int i = 0; i < size; i++) {
            bb.putInt(indices.get(i));
            bb.putFloat(values.get(i));
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
 
    private void openStream() throws IOException {
        textReader = new BufferedReader(
            new InputStreamReader(new FileInputStream(sourcePath), StandardCharsets.UTF_8),
            STREAM_BUFFER_SIZE);
    }
 
    private void skipRecords(BufferedReader reader, long recordsToSkip) throws IOException {
        for (long i = 0; i < recordsToSkip; i++) {
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("Cannot skip " + recordsToSkip + " records, file has fewer lines");
            }
        }
    }
 
    private static boolean notBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }
 
    @Override
    public void close() throws IOException {
        if (textReader != null) {
            textReader.close();
            textReader = null;
        }
        if (rangeEmbeddingsCache != null) {
            rangeEmbeddingsCache.clear();
            rangeEmbeddingsCache = null;
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
 