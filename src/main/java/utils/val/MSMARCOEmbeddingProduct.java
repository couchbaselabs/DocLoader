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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Base64;

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

    // <vecFilePath>.idx holds one little-endian long per line: the byte offset of
    // that line in the .vec file. It is read one entry at a time via a positional
    // read rather than slurped into a long[] — for an 8.8M-line source that array
    // is ~70MB, and every generator instance would hold its own private copy.
    // Path only, not an open channel: see offsetOf().
    private String idxFilePath;

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
            this.idxFilePath = ensureIndex(sourcePath);
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

    // Ensures the .idx sidecar exists alongside the .vec file, building it if absent,
    // and returns its path. Synchronized to prevent concurrent workers from racing to
    // build the same index file.
    private static synchronized String ensureIndex(String vecFilePath) throws IOException {
        String idxFilePath = vecFilePath + ".idx";
        if (!Files.exists(Paths.get(idxFilePath)))
            buildAndSaveIndex(vecFilePath, idxFilePath);
        return idxFilePath;
    }

    // Single pass: scans .vec for newlines, streams offsets directly to .idx.
    // Never holds all offsets in memory.
    private static void buildAndSaveIndex(String vecFilePath, String idxFilePath) throws IOException {
        System.out.println("Building offset index for: " + vecFilePath + " -> " + idxFilePath);
        ByteBuffer readBuf = ByteBuffer.allocateDirect(STREAM_BUFFER_SIZE);
        ByteBuffer writeBuf = ByteBuffer.allocate(8 * 8192).order(ByteOrder.LITTLE_ENDIAN);
        long bytePos = 0;

        // Build into a temp file and rename atomically. ensureIndex()'s exists-check only
        // serialises workers inside one JVM; writing in place lets a second process observe
        // a file that exists but is still being written, and read truncated offsets.
        Path tmp = Paths.get(idxFilePath + ".tmp");
        try {
        try (FileChannel vecCh = FileChannel.open(Paths.get(vecFilePath), StandardOpenOption.READ);
             FileChannel idxCh = FileChannel.open(tmp,
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
            idxCh.force(true);
        }
        Files.move(tmp, Paths.get(idxFilePath),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }

        System.out.println("Index built: " + idxFilePath);
    }

    // Reads the byte offset of a single line from the .idx sidecar.
    //
    // The channel is opened per call rather than held for this object's lifetime. Nothing
    // in the repo calls close() on a generator, and TaskRequest builds
    // process_concurrency of them per request in a long-running server, so a retained
    // channel accumulates for the JVM's lifetime -- and ulimit -n is 1024 on the volume
    // hosts. Opening here keeps this class at the same descriptor count as before this
    // change, where loadIndex() opened and closed the index inside try-with-resources.
    //
    // Not a hot path: seekToRecord() is the only caller, reached once per generator in
    // create mode and once per iteration wrap in mutation mode, since keys arrive in
    // ascending order and currentRecord already tracks the target.
    private long offsetOf(long recordIndex) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel idxChannel = FileChannel.open(Paths.get(idxFilePath), StandardOpenOption.READ)) {
            long records = idxChannel.size() / 8;
            if (recordIndex < 0 || recordIndex >= records)
                throw new IOException("record index " + recordIndex + " out of bounds [0, " + records
                        + ") in " + idxFilePath);
            long pos = recordIndex * 8L;
            while (buf.hasRemaining()) {
                if (idxChannel.read(buf, pos + buf.position()) < 0)
                    throw new IOException("truncated offset index at record " + recordIndex);
            }
        }
        buf.flip();
        return buf.getLong();
    }

    // O(1) seek to any record by direct byte offset lookup.
    // Do NOT close lineReader — closing Channels.newInputStream would close fileChannel.
    private void seekToRecord(long recordIndex) throws IOException {
        fileChannel.position(offsetOf(recordIndex));
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
        while (isWhitespaceOnly(line)) {
            line = lineReader.readLine();
            if (line == null) {
                throw new IOException("No more embedding records available from source: " + sourcePath);
            }
        }
        return parseRecord(line);
    }

    // Parses one "<id>\t[indices]\t[values]" record into { int[], float[] }.
    //
    // The pair is kept as primitive arrays rather than List<Integer>/List<Float>: at
    // ~300 non-zeros the boxed form costs ~12KB per document against ~2.5KB here, and
    // every in-flight batch retains batchSize x workers of them.
    //
    // Returned as Object[] so Jackson still emits [[indices...],[values...]], byte for
    // byte what the nested-List form produced.
    private static Object parseRecord(String line) throws IOException {
        int firstTab = line.indexOf('\t');
        int secondTab = firstTab < 0 ? -1 : line.indexOf('\t', firstTab + 1);
        if (secondTab < 0)
            throw new IOException("Invalid .vec format: expected 3 tab-separated parts");

        int indicesOpen = line.indexOf('[', firstTab + 1);
        int indicesClose = line.lastIndexOf(']', secondTab);
        int valuesOpen = line.indexOf('[', secondTab + 1);
        int valuesClose = line.lastIndexOf(']');
        if (indicesOpen < 0 || indicesClose < indicesOpen || valuesOpen < 0 || valuesClose < valuesOpen)
            throw new IOException("Invalid .vec format: malformed index/value list");

        int count = countElements(line, indicesOpen + 1, indicesClose);
        int[] indices = new int[count];
        float[] values = new float[count];
        int indexCount = parseIndices(line, indicesOpen + 1, indicesClose, indices);
        int valueCount = parseValues(line, valuesOpen + 1, valuesClose, values);
        if (indexCount != valueCount) {
            throw new IOException("Indices and values arrays have different lengths: "
                    + indexCount + " vs " + valueCount);
        }
        // countElements() counts separators while the parsers skip runs of them, so an
        // empty element -- a trailing or doubled comma -- makes count overshoot what is
        // actually parsed. Trim to the parsed length: leaving the surplus slots at their
        // defaults would inject a phantom index 0 / value 0.0 into the document, and the
        // check above cannot catch it because both sides undercount identically. The
        // split(",") this replaced dropped trailing empties, so trimming keeps that
        // behaviour. Never taken on well-formed input.
        if (indexCount != count) {
            indices = Arrays.copyOf(indices, indexCount);
            values = Arrays.copyOf(values, indexCount);
        }
        return new Object[] { indices, values };
    }

    // Counts comma-separated elements in [from, to); 0 if the region is whitespace only.
    private static int countElements(String s, int from, int to) {
        int i = from;
        while (i < to && s.charAt(i) <= ' ')
            i++;
        if (i >= to)
            return 0;
        int count = 1;
        for (; i < to; i++)
            if (s.charAt(i) == ',')
                count++;
        return count;
    }

    // Parses the comma-separated integers in [from, to) into out. Allocation free.
    private static int parseIndices(String s, int from, int to, int[] out) throws IOException {
        int n = 0;
        int i = from;
        while (i < to) {
            while (i < to && isSeparator(s.charAt(i)))
                i++;
            if (i >= to)
                break;
            boolean negative = s.charAt(i) == '-';
            if (negative || s.charAt(i) == '+')
                i++;
            int start = i;
            int value = 0;
            while (i < to) {
                char c = s.charAt(i);
                if (c < '0' || c > '9')
                    break;
                value = value * 10 + (c - '0');
                i++;
            }
            if (i == start)
                throw new IOException("Invalid .vec format: malformed index at offset " + start);
            if (n == out.length)
                throw new IOException("Invalid .vec format: more indices than counted");
            out[n++] = negative ? -value : value;
        }
        return n;
    }

    // Parses the comma-separated floats in [from, to) into out.
    //
    // Float.parseFloat is kept, on a per-token substring, rather than accumulating the
    // decimal by hand: the source prints up to 17 significant digits, past the 2^53
    // mantissa limit under which a hand-rolled accumulator is still provably correctly
    // rounded. parseFloat is what guarantees the emitted JSON is unchanged.
    private static int parseValues(String s, int from, int to, float[] out) throws IOException {
        int n = 0;
        int i = from;
        while (i < to) {
            while (i < to && isSeparator(s.charAt(i)))
                i++;
            if (i >= to)
                break;
            int start = i;
            while (i < to && s.charAt(i) != ',')
                i++;
            int end = i;
            while (end > start && s.charAt(end - 1) <= ' ')
                end--;
            if (end == start)
                throw new IOException("Invalid .vec format: empty value at offset " + start);
            if (n == out.length)
                throw new IOException("Invalid .vec format: more values than indices");
            out[n++] = Float.parseFloat(s.substring(start, end));
        }
        return n;
    }

    private static boolean isSeparator(char c) {
        return c == ',' || c <= ' ';
    }

    private static boolean isWhitespaceOnly(String s) {
        for (int i = 0; i < s.length(); i++)
            if (s.charAt(i) > ' ')
                return false;
        return true;
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
