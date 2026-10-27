package utils.docgen;

/**
 * Stateless, O(1)-memory pseudo-random permutation over the half-open range
 * {@code [0, size)}.
 *
 * <p>It lets mutations be issued in a shuffled key order without:
 * <ul>
 *   <li>materialising a {@code long[]} of the whole range -- which would not
 *       scale to the billions-of-docs loads this tool targets (8 bytes/doc), and</li>
 *   <li>introducing any shared mutable state -- the caller keeps drawing from
 *       its single existing atomic counter and merely runs the drawn value
 *       through {@link #map(long)}, so no new race is created.</li>
 * </ul>
 *
 * <p>Implemented as a small Feistel network combined with cycle-walking. A
 * Feistel network is a bijection on a power-of-two domain regardless of its
 * round function; cycle-walking (re-encrypting any output that falls outside
 * {@code [0, size)}) reduces that to an exact bijection on {@code [0, size)}.
 * "Exact bijection" is the property that matters here: every offset maps to a
 * distinct offset in range, so each key is still visited exactly once per pass.
 *
 * <p>The permutation is fully determined by {@code (size, seed)}, so a re-run
 * against the same range reproduces the same order -- desirable for a loader
 * you may re-point at the same key space.
 */
public final class IndexShuffler {
    private static final int ROUNDS = 4;
    // Domains wider than this risk overflowing a signed long once the two
    // halves are recombined; update ranges never approach it, so we simply
    // disable shuffling (identity map) in that degenerate case.
    private static final int MAX_DOMAIN_BITS = 62;

    private final long size;
    private final long seed;
    private final int halfBits;
    private final long halfMask;
    private final boolean identity;

    public IndexShuffler(long size, long seed) {
        this.size = size;
        this.seed = seed;
        if (size <= 1) {
            this.halfBits = 0;
            this.halfMask = 0;
            this.identity = true;
            return;
        }
        // ceil(log2(size)): smallest number of bits that can index the range.
        int bits = 64 - Long.numberOfLeadingZeros(size - 1);
        // Use balanced halves; an even domain width keeps the Feistel simple.
        int hb = (bits + 1) / 2;
        this.halfBits = hb;
        this.halfMask = (1L << hb) - 1;
        this.identity = (2 * hb) > MAX_DOMAIN_BITS;
    }

    /**
     * Maps offset {@code o} in {@code [0, size)} to a distinct permuted offset
     * in {@code [0, size)}. Returns {@code o} unchanged for degenerate ranges.
     */
    public long map(long o) {
        if (identity) {
            return o;
        }
        long x = encrypt(o);
        // Cycle-walk: the Feistel domain (2^(2*halfBits)) is >= size but at most
        // ~4x size, so re-encrypting until we land back inside [0, size)
        // terminates quickly (expected < 4 iterations) and yields a bijection.
        while (x >= size) {
            x = encrypt(x);
        }
        return x;
    }

    /** Bijection on the Feistel domain [0, 2^(2*halfBits)). */
    private long encrypt(long value) {
        long left = (value >>> halfBits) & halfMask;
        long right = value & halfMask;
        for (int round = 0; round < ROUNDS; round++) {
            long nextLeft = right;
            long nextRight = (left ^ (roundFunction(right, round) & halfMask)) & halfMask;
            left = nextLeft;
            right = nextRight;
        }
        return (left << halfBits) | right;
    }

    /** SplitMix64-style avalanche of the right half, round index and seed. */
    private long roundFunction(long block, int round) {
        long x = block + seed + round * 0x9E3779B97F4A7C15L;
        x ^= (x >>> 30);
        x *= 0xBF58476D1CE4E5B9L;
        x ^= (x >>> 27);
        x *= 0x94D049BB133111EBL;
        x ^= (x >>> 31);
        return x;
    }
}
