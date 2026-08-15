package utils.val;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import utils.docgen.WorkLoadSettings;

public class MSMARCOSiftEmbeddingProduct implements Closeable {
    private static final int SIFT_DIM = 128;
    private static final int SIFT_RECORD_BYTES = 4 + SIFT_DIM; // 4 bytes dim + 128 bytes data

    private static final long[] STEPS = new long[] {
            0, 100000, 1000000, 8841823
    };

    public WorkLoadSettings ws;
    private final String sparseSourcePath;
    private final String siftSourcePath;

    // Sparse records come from a binary sidecar built once from the .vec source; see
    // SparseVectorStore. Reading primitives back costs no text parsing, which is what
    // previously dominated this loader's allocation.
    private SparseVectorStore store;

    // SIFT: fixed-size records (4 + 128 = 132 bytes each), addressed positionally as
    // recordIndex * SIFT_RECORD_BYTES so it stays in lockstep with the sparse record.
    private FileChannel siftChannel;
    private final ByteBuffer siftBuffer =
            ByteBuffer.allocate(SIFT_RECORD_BYTES).order(ByteOrder.LITTLE_ENDIAN);

    private long rangeStart;
    private long rangeEnd;
    private int rangeSize;

    private long workerStartRecord;
    private long currentRecord;
    private boolean isMutation;

    public MSMARCOSiftEmbeddingProduct(WorkLoadSettings ws) {
        this.ws = ws;
        this.sparseSourcePath = resolveSparseSourcePath(ws);
        this.siftSourcePath = resolveSiftSourcePath(ws);

        try {
            this.store = SparseVectorStore.open(sparseSourcePath);
            this.siftChannel = FileChannel.open(Paths.get(siftSourcePath), StandardOpenOption.READ);

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
            throw new RuntimeException("Failed to initialize MSMARCO+SIFT streams: " + e.getMessage(), e);
        }
    }

    // Ensures the .idx sidecar exists alongside the .vec file, building it if absent,
    // and returns its path. Synchronized to prevent concurrent workers from racing to
    // build the same index file.
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

        // Both sources are addressed by the same record index, so they cannot drift apart.
        Object sparseEmbedding = store.read(currentRecord);
        float[] siftEmbedding = readSiftEmbedding(currentRecord);
        currentRecord++;

        if (rangeStart >= STEPS[0] && rangeEnd <= STEPS[1])
            return createProduct(id, sparseEmbedding, siftEmbedding, 5, "Green", "Nike", "USA", "Shoes", "Casual", 1.0f);
        if (rangeStart >= STEPS[1] && rangeEnd <= STEPS[2])
            return createProduct(id, sparseEmbedding, siftEmbedding, 6, "Green", "Nike", "USA", "Shoes", "Formal", 1.0f);
        if (rangeStart >= STEPS[2] && rangeEnd <= STEPS[3])
            return createProduct(id, sparseEmbedding, siftEmbedding, 7, "Green", "Nike", "USA", "Jeans", "Formal", 1.0f);

        return null;
    }

    private Object createProduct(int id, Object sparseEmbedding, float[] siftEmbedding, int size, String color,
            String brand, String country, String category, String type, float review) {
        if (ws.base64) {
            return new Product2(id, encodeSparseToBase64(sparseEmbedding), encodeDenseToBase64(siftEmbedding),
                    size, color, brand, country, category, type, review, ws.mutated);
        }
        return new Product1(id, sparseEmbedding, siftEmbedding, size, color, brand, country, category, type, review,
                ws.mutated);
    }

    private float[] readSiftEmbedding(long recordIndex) throws IOException {
        ByteBuffer buf = siftBuffer;
        buf.clear();
        long pos = recordIndex * SIFT_RECORD_BYTES;
        while (buf.hasRemaining()) {
            if (siftChannel.read(buf, pos + buf.position()) < 0)
                throw new IOException("Unexpected EOF reading SIFT record " + recordIndex
                        + " from: " + siftSourcePath);
        }
        buf.flip();
        int dim = buf.getInt();
        if (dim <= 0 || dim > SIFT_DIM) {
            throw new IOException("Invalid SIFT vector dimension " + dim + " from source: " + siftSourcePath);
        }
        float[] vector = new float[SIFT_DIM];
        for (int i = 0; i < dim; i++) {
            vector[i] = (float) Byte.toUnsignedInt(buf.get());
        }
        return vector;
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

    private static String encodeDenseToBase64(float[] vector) {
        byte[] bytes = new byte[Float.BYTES * vector.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(vector);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String resolveSparseSourcePath(WorkLoadSettings ws) {
        if (notBlank(ws.embeddingFilePath)) return ws.embeddingFilePath;
        if (notBlank(ws.baseVectorsFilePath)) return ws.baseVectorsFilePath;
        throw new IllegalArgumentException(
                "Sparse embedding source path is missing. Set embeddingFilePath or baseVectorsFilePath");
    }

    private static String resolveSiftSourcePath(WorkLoadSettings ws) {
        if (notBlank(ws.baseVectorsFilePath)) return ws.baseVectorsFilePath;
        throw new IllegalArgumentException("SIFT source path is missing. Set baseVectorsFilePath");
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
        if (siftChannel != null) {
            siftChannel.close();
            siftChannel = null;
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
        private float[] embedding;
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
                @JsonProperty("embedding") float[] embedding,
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
            this.embedding = embedding;
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
        private String embedding;
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
                @JsonProperty("embedding") String embedding,
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
            this.embedding = embedding;
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
