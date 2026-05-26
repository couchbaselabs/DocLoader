package utils.val;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import utils.docgen.WorkLoadSettings;

public class MSMARCOEmbeddingProduct implements Closeable {
    private static final int STREAM_BUFFER_SIZE = 1024 * 1024;

    // 3 step ranges for 8,841,823 vectors
    private static final long[] STEPS = new long[] {
        0, 100000, 1000000, 8841823
    };

    public WorkLoadSettings ws;
    private final String sourcePath;

    // lineOffsets[i] = byte offset of line i in the .vec file
    // Loaded from <vecFilePath>.idx on first use; built and saved if absent
    private final long[] lineOffsets;

    private FileChannel fileChannel;
    private BufferedReader lineReader;

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
            this.lineOffsets = loadOrBuildIndex(sourcePath);
            this.fileChannel = FileChannel.open(Paths.get(sourcePath), StandardOpenOption.READ);

            if (ws.creates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.create_s);
                this.workerStartRecord = ws.dr.create_s;
                this.isMutation = false;
                seekToRecord(ws.dr.create_s);
            } else if (ws.updates > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.update_s);
                this.workerStartRecord = ws.dr.update_s;
                this.isMutation = true;
                seekToRecord(rangeStart + ((workerStartRecord - rangeStart + ws.mutated) % rangeSize));
            } else if (ws.expiry > 0 && ws.dr != null) {
                initRangeBounds(ws.dr.expiry_s);
                this.workerStartRecord = ws.dr.expiry_s;
                this.isMutation = true;
                seekToRecord(ws.dr.expiry_s);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize MSMARCO embedding stream: " + e.getMessage(), e);
        }
    }

    // Loads .idx if it exists alongside the .vec file, otherwise builds and saves it first.
    // Synchronized to prevent concurrent workers from racing to build the same index file.
    private static synchronized long[] loadOrBuildIndex(String vecFilePath) throws IOException {
        String idxFilePath = vecFilePath + ".idx";
        if (Files.exists(Paths.get(idxFilePath))) {
            System.out.println("Loading offset index: " + idxFilePath);
            return loadIndex(idxFilePath);
        }
        return buildAndSaveIndex(vecFilePath, idxFilePath);
    }

    private static long[] loadIndex(String idxFilePath) throws IOException {
        try (FileChannel ch = FileChannel.open(Paths.get(idxFilePath), StandardOpenOption.READ)) {
            int numRecords = (int) (ch.size() / 8);
            long[] offsets = new long[numRecords];
            ByteBuffer buf = ByteBuffer.allocate(8 * 8192).order(ByteOrder.LITTLE_ENDIAN);
            int idx = 0;
            while (ch.read(buf) > 0) {
                buf.flip();
                while (buf.remaining() >= 8)
                    offsets[idx++] = buf.getLong();
                buf.compact();
            }
            return offsets;
        }
    }

    // Single pass: scans .vec for newlines, streams offsets directly to .idx, then loads it back.
    // Never holds all offsets in memory during build — only the final long[] on load (~70MB).
    private static long[] buildAndSaveIndex(String vecFilePath, String idxFilePath) throws IOException {
        System.out.println("Building offset index for: " + vecFilePath + " -> " + idxFilePath);
        ByteBuffer readBuf = ByteBuffer.allocateDirect(STREAM_BUFFER_SIZE);
        ByteBuffer writeBuf = ByteBuffer.allocate(8 * 8192).order(ByteOrder.LITTLE_ENDIAN);
        long bytePos = 0;

        try (FileChannel vecCh = FileChannel.open(Paths.get(vecFilePath), StandardOpenOption.READ);
             FileChannel idxCh = FileChannel.open(Paths.get(idxFilePath),
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            // Line 0 always starts at byte 0
            writeBuf.putLong(0L);

            while (vecCh.read(readBuf) > 0) {
                readBuf.flip();
                while (readBuf.hasRemaining()) {
                    byte b = readBuf.get();
                    bytePos++;
                    if (b == '\n') {
                        writeBuf.putLong(bytePos);
                        if (!writeBuf.hasRemaining()) {
                            writeBuf.flip();
                            idxCh.write(writeBuf);
                            writeBuf.clear();
                        }
                    }
                }
                readBuf.clear();
            }
            writeBuf.flip();
            if (writeBuf.hasRemaining())
                idxCh.write(writeBuf);
        }

        System.out.println("Index built: " + idxFilePath);
        return loadIndex(idxFilePath);
    }

    // O(1) seek to any record by direct byte offset lookup.
    // Do NOT close lineReader — closing Channels.newInputStream would close fileChannel.
    private void seekToRecord(long recordIndex) throws IOException {
        fileChannel.position(lineOffsets[(int) recordIndex]);
        lineReader = new BufferedReader(
                new InputStreamReader(Channels.newInputStream(fileChannel), StandardCharsets.UTF_8),
                STREAM_BUFFER_SIZE);
        currentRecord = recordIndex;
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
            long targetRecord = rangeStart + ((keyNum - rangeStart + ws.mutated) % rangeSize);
            if (targetRecord != currentRecord) {
                seekToRecord(targetRecord);
            }
        }

        Object sparseEmbedding = readNextEmbedding();
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

    private Object readNextEmbedding() throws IOException {
        if (lineReader == null) {
            throw new IOException("Reader is null");
        }
        String line = lineReader.readLine();
        if (line == null) {
            throw new IOException("No more embedding records available from source: " + sourcePath);
        }
        line = line.trim();
        while (line.isEmpty()) {
            line = lineReader.readLine();
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
        String[] indexParts = indicesStr.split(",");

        String valuesStr = parts[2].trim();
        valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
        String[] valueParts = valuesStr.split(",");
        if (valueParts.length != indexParts.length) {
            throw new IOException("Indices and values arrays have different lengths: "
                    + indexParts.length + " vs " + valueParts.length);
        }

        List<Integer> indicesList = new ArrayList<>(indexParts.length);
        List<Float> valuesList = new ArrayList<>(indexParts.length);
        for (int i = 0; i < indexParts.length; i++) {
            indicesList.add(Integer.parseInt(indexParts[i].trim()));
            valuesList.add(Float.parseFloat(valueParts[i].trim()));
        }

        List<Object> result = new ArrayList<>(2);
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

    private static boolean notBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public void close() throws IOException {
        // Do not close lineReader — closing Channels.newInputStream wraps would close fileChannel
        lineReader = null;
        if (fileChannel != null) {
            fileChannel.close();
            fileChannel = null;
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
