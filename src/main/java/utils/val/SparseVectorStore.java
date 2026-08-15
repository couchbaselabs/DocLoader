package utils.val;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Random-access reader for the MSMARCO sparse vector source, backed by a binary sidecar
 * built once from the text .vec file.
 *
 * <p>Motivation: the text source prints each value at up to 17 significant digits. That is
 * past the fast path in {@code FloatingDecimal.readJavaFormatString}, so every
 * {@code Float.parseFloat} call drops into the arbitrary-precision correction loop backed
 * by {@code sun.misc.FDBigInteger}. A live heap histogram taken mid-load showed 11.7M
 * FDBigInteger and 3.6M FloatingDecimal$ASCIIToBinaryBuffer instances -- roughly 1.19GB of
 * pure parser scratch -- which is what drove ParallelGC's adaptive sizing to inflate eden
 * to 2.58GB chasing its GCTimeRatio goal. Parsing once into a binary sidecar removes that
 * allocation from every subsequent load entirely.
 *
 * <p>Values are bit-exact: the sidecar stores the {@code float} that
 * {@code Float.parseFloat} already produced, so documents generated from it are identical
 * to those generated from the text.
 *
 * <p>File layout, little-endian throughout:
 *
 * <pre>
 *   0                    header, {@value #HEADER_BYTES} bytes
 *                          long magic       = MAGIC
 *                          int  version     = VERSION
 *                          int  reserved    = 0
 *                          long recordCount
 *                          long tableOffset
 *   {@value #HEADER_BYTES}  record data, records back to back
 *   tableOffset          offset table, (recordCount + 1) longs
 *                          entry[i] = absolute offset of record i
 *                          entry[recordCount] = end of data == tableOffset
 *
 *   record i:
 *     int   nnz
 *     int   indices[nnz]
 *     float values[nnz]
 * </pre>
 *
 * <p>The table is written after the data so the build needs no advance record count, and
 * it is read one entry at a time by positional read -- an 8.8M-record source would need a
 * ~70MB {@code long[]} to hold it, per instance.
 *
 * <p>Not thread safe: {@link #read} reuses internal buffers. Each generator owns its own
 * instance, and the generators serialise their own {@code next()} calls.
 */
public final class SparseVectorStore implements Closeable {

    /** ASCII "MSV_SP01". */
    private static final long MAGIC = 0x4D53565F53503031L;
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 32;

    private static final String BIN_SUFFIX = ".bin";
    private static final String TMP_SUFFIX = ".tmp";
    private static final String TABLE_TMP_SUFFIX = ".table.tmp";

    /** Guards against absurd allocations if a sidecar is truncated or corrupt. */
    private static final int MAX_NNZ = 1 << 20;

    private static final int INITIAL_RECORD_BUFFER = 16 * 1024;

    // Build-time only.
    private static final int TEXT_READ_BUFFER = 1 << 20;
    private static final int DATA_WRITE_BUFFER = 1 << 20;
    private static final int TABLE_WRITE_ENTRIES = 8192;
    private static final long PROGRESS_INTERVAL = 1000000L;

    private static final int[] NO_INDICES = new int[0];
    private static final float[] NO_VALUES = new float[0];

    private final String binPath;
    private final FileChannel channel;
    private final long recordCount;
    private final long tableOffset;

    // Reused across reads, and direct: FileChannel.read() into a heap buffer copies via a
    // temporary direct buffer it has to allocate, which measured ~1.1KB of churn per
    // record. Direct buffers read straight through, so a read now allocates nothing
    // beyond the two arrays it returns.
    //
    // The offset pair buffer holds entries i and i+1, which are adjacent in the table, so
    // one 16-byte read locates a record instead of two 8-byte reads.
    private final ByteBuffer offsetPair = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);
    private ByteBuffer recordView = ByteBuffer.allocateDirect(INITIAL_RECORD_BUFFER)
            .order(ByteOrder.LITTLE_ENDIAN);

    private SparseVectorStore(String binPath, FileChannel channel, long recordCount, long tableOffset) {
        this.binPath = binPath;
        this.channel = channel;
        this.recordCount = recordCount;
        this.tableOffset = tableOffset;
    }

    /**
     * Opens the sidecar for {@code vecFilePath}, building it first if it is missing or
     * unusable. Building is a one-off full pass over the text source.
     */
    public static SparseVectorStore open(String vecFilePath) throws IOException {
        String path = ensureSidecar(vecFilePath);
        FileChannel ch = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
        try {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            readFully(ch, header, 0L, "header of " + path);
            header.flip();

            long magic = header.getLong();
            int version = header.getInt();
            header.getInt(); // reserved
            long count = header.getLong();
            long table = header.getLong();

            if (magic != MAGIC)
                throw new IOException("Not a sparse vector sidecar: " + path);
            if (version != VERSION)
                throw new IOException("Sidecar " + path + " is version " + version
                        + ", this build expects " + VERSION + "; delete it to rebuild");
            if (count < 0 || table < HEADER_BYTES || table + 8L * (count + 1) != ch.size())
                throw new IOException("Sidecar " + path + " has an inconsistent header"
                        + " (records=" + count + ", tableOffset=" + table + ", size=" + ch.size() + ")");

            return new SparseVectorStore(path, ch, count, table);
        } catch (IOException e) {
            ch.close();
            throw e;
        } catch (RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /** Number of records available, i.e. lines in the source. */
    public long recordCount() {
        return recordCount;
    }

    /**
     * Reads record {@code recordIndex} as {@code { int[] indices, float[] values }}.
     *
     * <p>Returned as {@code Object[]} because that is what Jackson renders as
     * {@code [[indices...],[values...]]} -- the shape the generated documents have always
     * had. Only the two primitive arrays are allocated; the read itself is buffer-reusing.
     */
    public Object[] read(long recordIndex) throws IOException {
        if (recordIndex < 0 || recordIndex >= recordCount)
            throw new IOException("record index " + recordIndex + " out of bounds [0, "
                    + recordCount + ") in " + binPath);

        // Entries recordIndex and recordIndex+1 are adjacent, so one read bounds the
        // record. The table always carries a trailing sentinel, so i+1 is in range.
        offsetPair.clear();
        readFully(channel, offsetPair, tableOffset + recordIndex * 8L,
                "offset table entries " + recordIndex + ".." + (recordIndex + 1) + " of " + binPath);
        long from = offsetPair.getLong(0);
        long to = offsetPair.getLong(8);

        long span = to - from;
        if (span < 4 || span > Integer.MAX_VALUE)
            throw new IOException("corrupt record length " + span + " at record " + recordIndex
                    + " in " + binPath);

        int length = (int) span;
        if (length > recordView.capacity()) {
            int grown = recordView.capacity();
            while (grown < length)
                grown <<= 1;
            recordView = ByteBuffer.allocateDirect(grown).order(ByteOrder.LITTLE_ENDIAN);
        }

        recordView.clear();
        recordView.limit(length);
        readFully(channel, recordView, from, "record " + recordIndex + " of " + binPath);

        int nnz = recordView.getInt(0);
        if (nnz < 0 || nnz > MAX_NNZ)
            throw new IOException("record " + recordIndex + " declares nnz=" + nnz + " in " + binPath);
        if (4L + 8L * nnz != length)
            throw new IOException("record " + recordIndex + " declares nnz=" + nnz + " (" + (4L + 8L * nnz)
                    + " bytes) but spans " + length + " bytes in " + binPath);

        int offset = 4;
        int[] indices = new int[nnz];
        for (int i = 0; i < nnz; i++, offset += 4)
            indices[i] = recordView.getInt(offset);

        float[] values = new float[nnz];
        for (int i = 0; i < nnz; i++, offset += 4)
            values[i] = recordView.getFloat(offset);

        return new Object[] { indices, values };
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /* ---------------------------------------------------------------- build */

    /**
     * Returns the sidecar path, building it if absent or unusable. Synchronized so
     * concurrent generators in one JVM build it once; across JVMs the build writes to a
     * temp file and renames atomically, so a racing build is wasteful but not corrupting.
     */
    private static synchronized String ensureSidecar(String vecFilePath) throws IOException {
        String binPath = vecFilePath + BIN_SUFFIX;
        if (isUsable(binPath))
            return binPath;
        build(vecFilePath, binPath);
        return binPath;
    }

    /** Cheap structural check: right magic and version, and the header agrees with the size. */
    private static boolean isUsable(String binPath) {
        Path path = Paths.get(binPath);
        if (!Files.exists(path))
            return false;
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            if (ch.size() < HEADER_BYTES)
                return false;
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            readFully(ch, header, 0L, binPath);
            header.flip();
            if (header.getLong() != MAGIC || header.getInt() != VERSION)
                return false;
            header.getInt(); // reserved
            long count = header.getLong();
            long table = header.getLong();
            return count >= 0 && table >= HEADER_BYTES && table + 8L * (count + 1) == ch.size();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Single pass over the text source: parse each line, append its binary record, and
     * stream the record offsets to a side file. The offset table is appended afterwards,
     * which is why no advance record count is needed.
     */
    private static void build(String vecFilePath, String binPath) throws IOException {
        Path tmp = Paths.get(binPath + TMP_SUFFIX);
        Path tableTmp = Paths.get(binPath + TABLE_TMP_SUFFIX);

        System.out.println("Building sparse vector sidecar: " + vecFilePath + " -> " + binPath);
        System.out.println("  one-off; subsequent loads read it directly and do no text parsing");

        long records = 0;
        long blank = 0;
        long dataPos = HEADER_BYTES;
        long tablePos = 0;

        ByteBuffer dataBuf = ByteBuffer.allocate(DATA_WRITE_BUFFER).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer tableBuf = ByteBuffer.allocate(8 * TABLE_WRITE_ENTRIES).order(ByteOrder.LITTLE_ENDIAN);

        try {
            try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    FileChannel table = FileChannel.open(tableTmp, StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                    BufferedReader in = new BufferedReader(new InputStreamReader(
                            new FileInputStream(vecFilePath), StandardCharsets.UTF_8), TEXT_READ_BUFFER)) {

                String line;
                while ((line = in.readLine()) != null) {
                    int[] indices;
                    float[] values;
                    if (isWhitespaceOnly(line)) {
                        // Keep the record/line mapping 1:1 rather than skipping, so a record
                        // index always addresses the line with the same number.
                        indices = NO_INDICES;
                        values = NO_VALUES;
                        blank++;
                    } else {
                        Object[] parsed = parseTextRecord(line);
                        indices = (int[]) parsed[0];
                        values = (float[]) parsed[1];
                    }

                    int recordBytes = 4 + 8 * indices.length;
                    if (recordBytes > dataBuf.capacity()) {
                        dataPos = flush(out, dataBuf, dataPos);
                        dataBuf = ByteBuffer.allocate(recordBytes).order(ByteOrder.LITTLE_ENDIAN);
                    } else if (recordBytes > dataBuf.remaining()) {
                        dataPos = flush(out, dataBuf, dataPos);
                    }

                    // Offset of this record: everything already flushed, plus what is buffered.
                    tableBuf.putLong(dataPos + dataBuf.position());
                    if (!tableBuf.hasRemaining())
                        tablePos = flush(table, tableBuf, tablePos);

                    dataBuf.putInt(indices.length);
                    for (int i = 0; i < indices.length; i++)
                        dataBuf.putInt(indices[i]);
                    for (int i = 0; i < values.length; i++)
                        dataBuf.putFloat(values[i]);

                    records++;
                    if (records % PROGRESS_INTERVAL == 0)
                        System.out.println("  ... " + records + " records, "
                                + (dataPos + dataBuf.position()) / (1024 * 1024) + " MB");
                }

                dataPos = flush(out, dataBuf, dataPos);
                tableBuf.putLong(dataPos); // sentinel: end of the last record
                tablePos = flush(table, tableBuf, tablePos);

                long expectedTableBytes = 8L * (records + 1);
                if (tablePos != expectedTableBytes)
                    throw new IOException("internal error: offset table is " + tablePos
                            + " bytes, expected " + expectedTableBytes);

                appendTable(out, tableTmp, dataPos, tablePos);

                ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
                header.putLong(MAGIC);
                header.putInt(VERSION);
                header.putInt(0);
                header.putLong(records);
                header.putLong(dataPos);
                flush(out, header, 0L);

                out.force(true);
            }
            Files.move(tmp, Paths.get(binPath),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        } finally {
            Files.deleteIfExists(tableTmp);
        }

        System.out.println("Sidecar built: " + binPath + " (" + records + " records"
                + (blank > 0 ? ", " + blank + " blank" : "") + ")");
    }

    /** Copies the staged offset table onto the end of the sidecar. */
    private static void appendTable(FileChannel out, Path tableTmp, long at, long bytes) throws IOException {
        try (FileChannel table = FileChannel.open(tableTmp, StandardOpenOption.READ)) {
            long remaining = bytes;
            long target = at;
            while (remaining > 0) {
                long moved = out.transferFrom(table, target, remaining);
                if (moved <= 0)
                    throw new IOException("could not append offset table to sidecar");
                target += moved;
                remaining -= moved;
            }
        }
    }

    private static long flush(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        buf.flip();
        long pos = position;
        while (buf.hasRemaining())
            pos += ch.write(buf, pos);
        buf.clear();
        return pos;
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position, String what)
            throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0)
                throw new IOException("unexpected EOF reading " + what);
            pos += n;
        }
    }

    /* ----------------------------------------------------- text parsing (build only) */

    /**
     * Parses one {@code "<id>\t[indices]\t[values]"} line into {@code { int[], float[] }}.
     *
     * <p>Runs once per record for the lifetime of the sidecar. {@code Float.parseFloat} is
     * used deliberately: the source carries up to 17 significant digits, past the point
     * where a hand-rolled decimal accumulator is still provably correctly rounded, and
     * parseFloat is what makes the stored floats bit-identical to the text.
     */
    static Object[] parseTextRecord(String line) throws IOException {
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
        if (indexCount != valueCount)
            throw new IOException("Indices and values arrays have different lengths: "
                    + indexCount + " vs " + valueCount);
        return new Object[] { indices, values };
    }

    /** Counts comma-separated elements in [from, to); 0 if the region is whitespace only. */
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
}
