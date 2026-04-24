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

public class MSMARCOSiftEmbeddingProduct implements Closeable {
    private static final int STREAM_BUFFER_SIZE = 1024 * 1024;
    private static final int SIFT_DIM = 128;

    private static final long[] STEPS = new long[] {
            0, 100000, 1000000, 8841823
    };

    public WorkLoadSettings ws;
    private final String sparseSourcePath;
    private final String siftSourcePath;

    private BufferedReader sparseReader;
    private FileInputStream siftInputStream;

    private long rangeStart;
    private long rangeEnd;
    private int rangeSize;
    private List<Object> rangeSparseCache;
    private List<float[]> rangeSiftCache;
    private int cyclicIndex;

    public MSMARCOSiftEmbeddingProduct(WorkLoadSettings ws) {
        this.ws = ws;
        this.sparseSourcePath = resolveSparseSourcePath(ws);
        this.siftSourcePath = resolveSiftSourcePath(ws);

        try {
            openStreams();
            if (ws.creates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.create_s);
                skipSparseRecords(sparseReader, ws.dr.create_s);
                skipSiftRecords(siftInputStream, ws.dr.create_s);
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
            throw new RuntimeException("Failed to initialize MSMARCO+SIFT streams: " + e.getMessage(), e);
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
        this.rangeSparseCache = new ArrayList<>(rangeSize);
        this.rangeSiftCache = new ArrayList<>(rangeSize);

        skipSparseRecords(sparseReader, rangeStart);
        skipSiftRecords(siftInputStream, rangeStart);

        for (int i = 0; i < rangeSize; i++) {
            rangeSparseCache.add(readNextSparseEmbedding(sparseReader));
            rangeSiftCache.add(readNextSiftEmbedding(siftInputStream));
        }

        sparseReader.close();
        sparseReader = null;
        siftInputStream.close();
        siftInputStream = null;
    }

    public synchronized Object next(String key) throws IOException {
        int id = Integer.parseInt(key.split("-")[key.split("-").length - 1]) + this.ws.mutated;
        Object sparseEmbedding;
        float[] siftEmbedding;

        if (rangeSparseCache != null && rangeSiftCache != null) {
            sparseEmbedding = rangeSparseCache.get(cyclicIndex);
            siftEmbedding = rangeSiftCache.get(cyclicIndex);
            cyclicIndex = (cyclicIndex + 1) % rangeSize;
        } else {
            sparseEmbedding = readNextSparseEmbedding(sparseReader);
            siftEmbedding = readNextSiftEmbedding(siftInputStream);
        }

        if (rangeStart >= STEPS[0] && rangeEnd <= STEPS[1])
            return createProduct(id, sparseEmbedding, siftEmbedding, 5, "Green", "Nike", "USA", "Shoes", "Casual",
                    1.0f);
        if (rangeStart >= STEPS[1] && rangeEnd <= STEPS[2])
            return createProduct(id, sparseEmbedding, siftEmbedding, 6, "Green", "Nike", "USA", "Shoes", "Formal",
                    1.0f);
        if (rangeStart >= STEPS[2] && rangeEnd <= STEPS[3])
            return createProduct(id, sparseEmbedding, siftEmbedding, 7, "Green", "Nike", "USA", "Jeans", "Formal",
                    1.0f);

        return null;
    }

    private Object createProduct(int id, Object sparseEmbedding, float[] siftEmbedding, int size, String color,
            String brand, String country, String category, String type, float review) {
        if (ws.base64) {
            String encodedSparse = encodeSparseToBase64(sparseEmbedding);
            String encodedSift = encodeDenseToBase64(siftEmbedding);
            return new Product2(id, encodedSparse, encodedSift, size, color, brand, country, category, type, review,
                    ws.mutated);
        }
        return new Product1(id, sparseEmbedding, siftEmbedding, size, color, brand, country, category, type, review,
                ws.mutated);
    }

    private Object readNextSparseEmbedding(BufferedReader reader) throws IOException {
        if (reader == null) {
            throw new IOException("Sparse reader is null");
        }
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("No more sparse embedding records available from source: " + sparseSourcePath);
        }
        line = line.trim();
        while (line.isEmpty()) {
            line = reader.readLine();
            if (line == null) {
                throw new IOException("No more sparse embedding records available from source: " + sparseSourcePath);
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

    private float[] readNextSiftEmbedding(FileInputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("SIFT input stream is null");
        }
        byte[] dimBytes = new byte[4];
        readFully(inputStream, dimBytes);
        int dim = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (dim <= 0 || dim > SIFT_DIM) {
            throw new IOException("Invalid SIFT vector dimension " + dim + " from source: " + siftSourcePath);
        }

        byte[] vectorBytes = new byte[dim];
        readFully(inputStream, vectorBytes);
        float[] vector = new float[SIFT_DIM];
        for (int i = 0; i < dim; i++) {
            vector[i] = (float) Byte.toUnsignedInt(vectorBytes[i]);
        }
        return vector;
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

    private static String encodeDenseToBase64(float[] vector) {
        byte[] bytes = new byte[Float.BYTES * vector.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(vector);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String resolveSparseSourcePath(WorkLoadSettings ws) {
        if (notBlank(ws.embeddingFilePath)) {
            return ws.embeddingFilePath;
        }
        if (notBlank(ws.baseVectorsFilePath)) {
            return ws.baseVectorsFilePath;
        }
        throw new IllegalArgumentException(
                "Sparse embedding source path is missing. Set embeddingFilePath or baseVectorsFilePath");
    }

    private static String resolveSiftSourcePath(WorkLoadSettings ws) {
        if (notBlank(ws.baseVectorsFilePath)) {
            return ws.baseVectorsFilePath;
        }
        throw new IllegalArgumentException("SIFT source path is missing. Set baseVectorsFilePath");
    }

    private void openStreams() throws IOException {
        sparseReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(sparseSourcePath), StandardCharsets.UTF_8),
                STREAM_BUFFER_SIZE);
        siftInputStream = new FileInputStream(siftSourcePath);
    }

    private void skipSparseRecords(BufferedReader reader, long recordsToSkip) throws IOException {
        for (long i = 0; i < recordsToSkip; i++) {
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("Cannot skip " + recordsToSkip + " sparse records, file has fewer lines");
            }
        }
    }

    private void skipSiftRecords(FileInputStream inputStream, long recordsToSkip) throws IOException {
        long bytesToSkip = recordsToSkip * (4L + SIFT_DIM);
        while (bytesToSkip > 0) {
            long skipped = inputStream.skip(bytesToSkip);
            if (skipped <= 0) {
                throw new IOException("Cannot skip " + recordsToSkip + " SIFT records, source is shorter");
            }
            bytesToSkip -= skipped;
        }
    }

    private static void readFully(FileInputStream in, byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = in.read(buffer, offset, buffer.length - offset);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading embedding data");
            }
            offset += read;
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (sparseReader != null) {
            sparseReader.close();
            sparseReader = null;
        }
        if (siftInputStream != null) {
            siftInputStream.close();
            siftInputStream = null;
        }
        if (rangeSparseCache != null) {
            rangeSparseCache.clear();
            rangeSparseCache = null;
        }
        if (rangeSiftCache != null) {
            rangeSiftCache.clear();
            rangeSiftCache = null;
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
