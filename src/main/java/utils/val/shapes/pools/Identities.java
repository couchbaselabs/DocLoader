package utils.val.shapes.pools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The one pool shared across document types.
 *
 * Shapes are otherwise self-contained, and that is deliberate: a new document type should
 * need no edits outside its own file. Identity is the single exception, because a person
 * legitimately appears on a transaction, a profile and a media record at once, and query
 * tests that correlate those types need the values to actually line up.
 *
 * The rule for what belongs here: attributes of a real-world entity that appears across
 * document types. Domain vocabulary (store ids, labels, channels) stays local to its shape.
 *
 * Every value is built from fixed seeds and pure loops, so the pool is byte-identical in
 * every JVM. Validation regenerates documents in a separate process and would break otherwise.
 */
public final class Identities {

    /** Wide enough that a 100k-document load averages ~10 records per identity. */
    public static final int SIZE = 10000;

    /**
     * Draw bias. Real customer bases are Pareto-shaped: a few accounts generate most of the
     * records. Skewing the draw puts high- and low-selectivity values in the same dataset,
     * which a uniform draw cannot do.
     */
    private static final double SKEW = 2.0;

    private Identities() {
    }

    public static Identity pick(Random random) {
        double biased = Math.pow(random.nextDouble(), SKEW);
        int index = (int) (biased * POOL.size());
        return POOL.get(Math.min(index, POOL.size() - 1));
    }

    public static int size() {
        return POOL.size();
    }

    /** One person. The canonical form; documents may render it inconsistently. */
    public static final class Identity {
        public final String email;
        public final String firstName;
        public final String lastName;
        public final String phone;

        Identity(String email, String firstName, String lastName, String phone) {
            this.email = email;
            this.firstName = firstName;
            this.lastName = lastName;
            this.phone = phone;
        }

        /**
         * The same address as stored by systems that did not agree on case. Keeping the
         * identity stable while varying its rendering is what makes LOWER() dedup real.
         */
        public String emailAs(Random random) {
            int roll = random.nextInt(10);
            if (roll < 6) {
                return email;
            }
            if (roll < 8) {
                return email.toLowerCase();
            }
            if (roll < 9) {
                return email.toUpperCase();
            }
            int at = email.indexOf('@');
            return email.substring(0, at).toUpperCase() + email.substring(at).toLowerCase();
        }

        /** The same number as written by systems that did not agree on a format. */
        public String phoneAs(Random random) {
            String digits = phone.replaceAll("[^0-9]", "");
            String tail = digits.substring(Math.max(0, digits.length() - 10));
            switch (random.nextInt(6)) {
                case 0:
                    return "+" + digits;
                case 1:
                    return "+" + digits.substring(0, digits.length() - 10) + "-"
                            + tail.substring(0, 3) + "-" + tail.substring(3, 6) + "-" + tail.substring(6);
                case 2:
                    return "(" + tail.substring(0, 3) + ") " + tail.substring(3, 6) + "-" + tail.substring(6);
                case 3:
                    return tail;
                case 4:
                    return "00" + digits;
                default:
                    return "+" + digits.substring(0, digits.length() - 10) + " "
                            + tail.substring(0, 3) + " " + tail.substring(3, 6) + " " + tail.substring(6);
            }
        }

        public String fullName() {
            return firstName + " " + lastName;
        }
    }

    // ── source vocabularies ───────────────────────────────────────────────

    /** Includes accented, non-Latin and punctuated forms: collation and LOWER() differ on these. */
    private static final List<String> FIRST = Arrays.asList(
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
            "Matthew", "Margaret", "Anthony", "Sandra", "Mark", "Ashley", "Donald", "Kimberly",
            "Priya", "Arjun", "Ananya", "Rohit", "Kavya", "Vikram", "Meera", "Sanjay",
            "Wei", "Li", "Xiaoming", "Yan", "Hiroshi", "Yuki", "Haruto", "Sakura",
            "Mohammed", "Fatima", "Omar", "Aisha", "Youssef", "Layla", "Karim", "Noor",
            "Søren", "Åsa", "Matthías", "Þóra", "Ingrid", "Lars", "Freja", "Björn",
            "José", "María", "Álvaro", "Lucía", "Iñaki", "Begoña", "Rocío", "Andrés",
            "François", "Chloé", "Rémi", "Amélie", "Théo", "Zoé", "Gaël", "Océane",
            "Müller", "Jürgen", "Käthe", "Günther", "Björk", "Ólafur", "Þór", "Sævar",
            "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Đặng", "Bùi", "Đỗ",
            "Öztürk", "Şule", "Çağla", "İbrahim", "Gülşen", "Ayşe", "Ömer", "Zeynep",
            "Oleksandr", "Kateryna", "Dmytro", "Olha", "Mykola", "Iryna", "Serhii", "Nataliia",
            "Kwame", "Amara", "Chidi", "Ngozi", "Thabo", "Zanele", "Kofi", "Adaeze");

    private static final List<String> LAST = Arrays.asList(
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "O'Connor", "O'Brien", "D'Angelo", "O'Neill", "D'Souza", "O'Sullivan",
            "Smith-Jones", "Parker-Hughes", "Lloyd-Webber", "Fitz-Gerald", "Van Der Berg",
            "Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Wagner", "Bäcker", "Schröder",
            "Patel", "Sharma", "Reddy", "Iyer", "Chatterjee", "Nair", "Banerjee", "Mehta",
            "Nguyễn", "Trần", "Phạm", "Hoàng", "Vũ", "Đặng", "Bùi", "Ngô",
            "Öztürk", "Yılmaz", "Kaya", "Demir", "Çelik", "Şahin", "Güneş", "Arslan",
            "Kowalski", "Nowak", "Wiśniewski", "Wójcik", "Kamiński", "Lewandowski", "Zieliński",
            "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci",
            "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Costa", "Almeida", "Carvalho",
            "Ivanov", "Petrov", "Sidorov", "Volkov", "Sokolov", "Popov", "Lebedev", "Kozlov",
            "Tanaka", "Suzuki", "Takahashi", "Watanabe", "Ito", "Yamamoto", "Nakamura", "Sato",
            "Okafor", "Adeyemi", "Mensah", "Diallo", "Traoré", "Abebe", "Mwangi", "Dlamini",
            "Haddad", "Khoury", "Nasser", "Aziz", "Rahman", "Karim", "Saleh", "Farouk");

    private static final List<String> DOMAINS = Arrays.asList(
            "example.com", "Example.com", "EXAMPLE.COM", "example.org", "example.net",
            "example.co.uk", "example.de", "example.io", "mail.example.com", "corp.example.com",
            "eu.example.org", "mail.example.co.uk", "internal.example.net", "m.example.io");

    /** Declared after the vocabularies above: static initialisers run in source order. */
    private static final List<Identity> POOL = build();

    private static List<Identity> build() {
        // Fixed seed: the pool must be identical in every JVM or validation cannot regenerate.
        Random seeded = new Random(0x0CB5E1D);
        List<Identity> pool = new ArrayList<Identity>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            // Co-prime strides so first and last names do not advance in lockstep blocks.
            String first = FIRST.get((i * 7) % FIRST.size());
            String last = LAST.get((i * 13) % LAST.size());
            String domain = DOMAINS.get((i * 3) % DOMAINS.size());
            pool.add(new Identity(
                    localPart(first, last, i, seeded) + "@" + domain,
                    first,
                    last,
                    canonicalPhone(i)));
        }
        return pool;
    }

    /** The several conventions organisations actually use for the part before the @. */
    private static String localPart(String first, String last, int index, Random random) {
        String f = ascii(first);
        String l = ascii(last);
        String suffix = index % 4 == 0 ? "" : String.valueOf(index % 997);
        switch (index % 7) {
            case 0:
                return f + "." + l + suffix;
            case 1:
                return f.substring(0, 1) + l + suffix;
            case 2:
                return f + l + suffix;
            case 3:
                return l + "." + f + suffix;
            case 4:
                return f + "_" + l + suffix;
            case 5:
                return f + "." + l + "+" + (random.nextBoolean() ? "news" : "billing") + suffix;
            default:
                return f + "." + l.substring(0, 1) + suffix;
        }
    }

    /** Email local parts stay ASCII even when the person's name is not. */
    private static String ascii(String name) {
        String stripped = java.text.Normalizer.normalize(name, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .replaceAll("[^A-Za-z]", "");
        return stripped.isEmpty() ? "user" : stripped;
    }

    /**
     * Canonical E.164 number. The subscriber part never starts with a zero, because real
     * numbering plans do not allocate those and a leading zero is a giveaway of fake data.
     */
    private static String canonicalPhone(int index) {
        String[] countryCodes = {"1", "44", "91", "49", "61", "33", "81", "55"};
        String cc = countryCodes[index % countryCodes.length];
        long subscriber = 2000000000L + (Math.abs((long) index * 7919L) % 7999999999L);
        return "+" + cc + subscriber;
    }
}
