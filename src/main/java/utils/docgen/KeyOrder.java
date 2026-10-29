package utils.docgen;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Order in which a pass visits the doc indexes of a range.
 *
 * The generator keeps handing out tickets from its AtomicLong exactly as before
 * (so termination, progress and iteration resets are unchanged) and asks this
 * class which doc a ticket stands for. Sequential order is a null KeyOrder.
 */
public abstract class KeyOrder {
    public static final String SEQUENTIAL = "sequential";
    public static final String SHUFFLE = "shuffle";
    public static final String ZIPFIAN = "zipfian";

    /** Doc offset in [0, n) for ticket offset t in [0, n) during the given pass. */
    public abstract long map(long t, long n, long pass);

    /** null means sequential: callers skip the mapping and use the ticket as the index. */
    public static KeyOrder of(String name, double theta) {
        if (name == null || name.isEmpty() || name.equals(SEQUENTIAL))
            return null;
        if (name.equals(SHUFFLE))
            return new Shuffle();
        if (name.equals(ZIPFIAN))
            return new Zipfian(theta);
        throw new IllegalArgumentException("Unknown key_order '" + name + "', expected "
                + SEQUENTIAL + " | " + SHUFFLE + " | " + ZIPFIAN);
    }

    /**
     * A different pseudo-random permutation of [0, n) every pass, so pages written in
     * the previous pass end up partially dead instead of wholly dead.
     */
    static class Shuffle extends KeyOrder {
        @Override
        public long map(long t, long n, long pass) {
            return permute(t, n, pass);
        }
    }

    /**
     * Skewed draws (rank 1 most popular) using Hörmann-Derflinger rejection-inversion:
     * O(1) setup and memory for any n and any theta > 0. Ranks are spread over the range
     * with a permutation seeded by the pass, so the hot set is not one contiguous block
     * of indexes and moves every pass. Each pass then kills a fresh slice of old pages
     * (theta 0.99, 1M docs: 22.6/40.0/53.6% of create docs overwritten after passes
     * 0-2, vs 22.5/35.6/45.2% with a fixed hot set), so old pages reach magma's
     * fragmentation threshold about one pass sooner. Pass 0 keeps the original hot set.
     */
    static class Zipfian extends KeyOrder {
        private static final long HOT_SET_SEED = 0x5eedL;
        private final double theta;

        Zipfian(double theta) {
            if (!(theta > 0) || Double.isInfinite(theta))
                throw new IllegalArgumentException("zipf_theta must be > 0, got " + theta);
            this.theta = theta;
        }

        @Override
        public long map(long t, long n, long pass) {
            return permute(sample(n) - 1, n, HOT_SET_SEED ^ (pass * 0x9E3779B97F4A7C15L));
        }

        // ponytail: setup constants are recomputed per draw (3 extra pow/log); cache per n
        // if this ever shows up next to the network I/O it feeds.
        long sample(long n) {
            double hIntegralX1 = hIntegral(1.5) - 1;
            double hIntegralN = hIntegral(n + 0.5);
            double s = 2 - hIntegralInverse(hIntegral(2.5) - h(2));
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (true) {
                double u = hIntegralN + rnd.nextDouble() * (hIntegralX1 - hIntegralN);
                double x = hIntegralInverse(u);
                long k = (long) (x + 0.5);
                if (k < 1)
                    k = 1;
                else if (k > n)
                    k = n;
                if (k - x <= s || u >= hIntegral(k + 0.5) - h(k))
                    return k;
            }
        }

        private double h(double x) {
            return Math.exp(-theta * Math.log(x));
        }

        private double hIntegral(double x) {
            double logX = Math.log(x);
            return helper2((1 - theta) * logX) * logX;
        }

        private double hIntegralInverse(double x) {
            double t = x * (1 - theta);
            if (t < -1)
                t = -1;
            return Math.exp(helper1(t) * x);
        }

        private static double helper1(double x) {
            if (Math.abs(x) > 1e-8)
                return Math.log1p(x) / x;
            return 1 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x));
        }

        private static double helper2(double x) {
            if (Math.abs(x) > 1e-8)
                return Math.expm1(x) / x;
            return 1 + x * 0.5 * (1 + x * (1.0 / 3.0) * (1 + 0.25 * x));
        }
    }

    /**
     * Bijection on [0, n) keyed by seed: a 4-round Feistel network over the smallest
     * even-width bit domain >= n, cycle-walking values that land outside [0, n). That
     * domain is < 4n, so a walk takes under 4 steps on average.
     */
    static long permute(long x, long n, long seed) {
        if (n <= 1)
            return 0;
        int bits = 64 - Long.numberOfLeadingZeros(n - 1);
        int half = (bits + 1) / 2;
        long mask = (1L << half) - 1;
        long key = mix(seed);
        do {
            long l = x >>> half;
            long r = x & mask;
            for (int round = 0; round < 4; round++) {
                long f = mix(r ^ key ^ (round * 0x9E3779B97F4A7C15L)) & mask;
                long next = l ^ f;
                l = r;
                r = next;
            }
            x = (l << half) | r;
        } while (x >= n);
        return x;
    }

    // splitmix64 finalizer
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
