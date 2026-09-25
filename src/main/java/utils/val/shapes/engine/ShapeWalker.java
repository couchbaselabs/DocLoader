package utils.val.shapes.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import utils.val.shapes.annotations.After;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.DateString;
import utils.val.shapes.annotations.Empty;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.Length;
import utils.val.shapes.annotations.LongRange;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.OneOf;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Precision;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.RecursionDepth;
import utils.val.shapes.annotations.Identity;
import utils.val.shapes.annotations.Sequence;
import utils.val.shapes.annotations.Shape;
import utils.val.shapes.pools.Identities;

/**
 * Builds a JsonObject from a shape class by reading its field annotations.
 * Each FieldKind has exactly one handler; no runtime type inference decides structure.
 */
public final class ShapeWalker {

    /** Name of the type discriminator field written on every document. */
    public static final String TYPE_FIELD = "_type";

    /** Name of the structured document-id field written on every document. */
    public static final String ID_FIELD = "_id";

    private static final LocalDateTime EPOCH_BASE = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
    private static final ZoneOffset[] OFFSETS = {
            ZoneOffset.UTC,
            ZoneOffset.ofHoursMinutes(5, 30),
            ZoneOffset.ofHours(1),
            ZoneOffset.ofHours(9),
            ZoneOffset.ofHours(-5),
            ZoneOffset.ofHours(-8)};
    private static final Map<Class<?>, Set<String>> IDENTITY_FIELDS =
            new ConcurrentHashMap<Class<?>, Set<String>>();
    // ── free-text generation ──────────────────────────────────────────────
    // Sentences are assembled from templates with typed slots, so long values read as real
    // free text rather than word salad, without storing millions of strings.

    private static final String[] SENTENCES = {
            "The {item} was {action} on {date} following a {reason}.",
            "{actor} reviewed the {item} and marked it {status}.",
            "Reference {ref}: {reason}. No further action is required.",
            "Customer contacted the {branch} branch regarding the {item}.",
            "This {item} was {action} after the {reason} was confirmed by {actor}.",
            "Awaiting confirmation from {actor} before the {item} can be {action}.",
            "A {reason} was recorded against reference {ref} on {date}.",
            "The {item} remains {status} pending review by the {branch} team.",
            "{actor} has escalated reference {ref} due to a {reason}.",
            "Delivery of the {item} was rescheduled to {date} at customer request.",
            "Note added by {actor}: the {item} is {status} and requires no reply.",
            "Following the {reason}, the {item} was returned to the {branch} branch.",
            "Reference {ref} was reconciled against the {item} and closed as {status}.",
            "The {branch} team confirmed the {item} was {action} without incident.",
            "Payment for the {item} failed on {date}; a {reason} was logged.",
            "An adjustment was applied to reference {ref} after a {reason}.",
            "{actor} requested a replacement {item}, currently {status}.",
            "The {item} could not be {action} because of an unresolved {reason}.",
            "Stock for the {item} was counted at the {branch} branch on {date}.",
            "Reference {ref} supersedes the earlier record and is now {status}."};

    private static final String[] ITEMS = {
            "order", "shipment", "invoice", "return", "replacement", "device", "handset",
            "router", "subscription", "line rental", "installation", "engineer visit",
            "refund", "statement", "consignment", "purchase order", "delivery note"};
    private static final String[] ACTIONS = {
            "dispatched", "cancelled", "amended", "authorised", "processed", "fulfilled",
            "credited", "reissued", "held for review", "returned to stock", "written off"};
    private static final String[] REASONS = {
            "billing discrepancy", "failed card authorisation", "address mismatch",
            "stock shortage", "damaged carton", "duplicate submission", "pricing error",
            "customer cancellation", "courier exception", "compliance hold", "late payment"};
    private static final String[] ACTORS = {
            "the account manager", "the support desk", "the finance team", "the duty manager",
            "the supplier", "the regional lead", "the compliance officer", "the branch supervisor"};
    private static final String[] STATUSES_TEXT = {
            "resolved", "outstanding", "under review", "closed", "reopened", "on hold",
            "approved", "declined", "partially fulfilled"};
    private static final String[] BRANCHES = {
            "northern", "southern", "central", "coastal", "metro", "regional", "overseas"};
    /** Short templates carry slots too, so brief values stay varied instead of repeating. */
    private static final String[] SHORT_TEMPLATES = {
            "Awaiting {item}", "Ref {ref} closed", "Returned: {reason}", "{status} by {actor}",
            "Check {item} at {branch} branch", "No action on ref {ref}", "{item} {action}",
            "Escalated: {reason}", "Confirmed by {actor}", "Due {date}", "Ref {ref}: {status}",
            "{item} held for checks", "Duplicate of ref {ref}", "Notified on {date}",
            "Pending {actor} review", "{reason} logged", "{item} {status}",
            "See ref {ref}", "{branch} branch notified", "{item} due {date}",
            "{action} on {date}", "Query from {actor}", "{reason} on ref {ref}",
            "Reissue {item}", "{status} since {date}"};

    private ShapeWalker() {
    }

    /**
     * Per-document state threaded through the walk: the seeded source of randomness, and the
     * one identity every @Identity field on this document reads from.
     */
    public static final class Ctx {
        public final Random random;
        public final Identities.Identity identity;

        public Ctx(Random random) {
            this.random = random;
            this.identity = Identities.pick(random);
        }
    }

    /**
     * Rebuilds the structured id from a finished document.
     *
     * A partial mutate merges fields from two generations, so the id has to be derived from
     * the result rather than carried over from either side, or it can end up naming a value
     * the document no longer holds.
     */
    public static String resolveId(Class<?> shapeClass, JsonObject doc, long seq) {
        Shape shape = shapeClass.getAnnotation(Shape.class);
        if (shape == null || shape.idPattern().isEmpty()) {
            return null;
        }
        String result = shape.idPattern().replace("{seq}", String.format("%07d", seq));
        for (Field field : annotatedFields(shapeClass)) {
            String token = "{" + field.getName() + "}";
            if (result.contains(token)) {
                Object value = PathWriter.get(doc, field.getAnnotation(Path.class).value());
                if (value != null) {
                    result = result.replace(token, String.valueOf(value));
                }
            }
        }
        return result;
    }

    /**
     * Top-level document fields on this shape whose values come from the shared identity.
     *
     * They must mutate as a group: changing an email without the matching name would leave a
     * document describing two different people, which is never a state worth loading.
     */
    public static Set<String> identityFields(Class<?> shapeClass) {
        Set<String> cached = IDENTITY_FIELDS.get(shapeClass);
        if (cached != null) {
            return cached;
        }
        Set<String> paths = new HashSet<String>();
        for (Field field : annotatedFields(shapeClass)) {
            if (field.getAnnotation(Identity.class) != null) {
                String path = field.getAnnotation(Path.class).value();
                int dot = path.indexOf('.');
                paths.add(dot < 0 ? path : path.substring(0, dot));
            }
        }
        Set<String> immutable = Collections.unmodifiableSet(paths);
        IDENTITY_FIELDS.put(shapeClass, immutable);
        return immutable;
    }

    private static Object identityValue(Identity.Attribute attribute, Ctx ctx) {
        Identities.Identity who = ctx.identity;
        switch (attribute) {
            case EMAIL:
                return who.emailAs(ctx.random);
            case EMAIL_EXACT:
                return who.email;
            case FIRST_NAME:
                return who.firstName;
            case LAST_NAME:
                return who.lastName;
            case FULL_NAME:
                return who.fullName();
            case PHONE:
                return who.phoneAs(ctx.random);
            case PHONE_EXACT:
                return who.phone;
            default:
                throw new IllegalStateException("Unhandled identity attribute: " + attribute);
        }
    }

    /** Builds one top-level document, including its type discriminator and structured id. */
    public static JsonObject walk(Class<?> shapeClass, Ctx ctx, long seq) {
        Shape shape = shapeClass.getAnnotation(Shape.class);
        if (shape == null) {
            throw new IllegalStateException(shapeClass.getName() + " is missing @Shape");
        }
        JsonObject doc = JsonObject.create();
        doc.put(TYPE_FIELD, shape.type());
        Map<String, Object> topLevelValues = new HashMap<String, Object>();
        writeFields(shapeClass, doc, ctx, 1, 0, topLevelValues);
        if (!shape.idPattern().isEmpty()) {
            doc.put(ID_FIELD, resolveIdPattern(shape.idPattern(), topLevelValues, seq));
        }
        return doc;
    }

    /** Builds a nested object: no discriminator, no id. */
    private static JsonObject buildObject(Class<?> type, Ctx ctx, int elementIndex, int depth) {
        JsonObject obj = JsonObject.create();
        writeFields(type, obj, ctx, elementIndex, depth, null);
        return obj;
    }

    private static void writeFields(Class<?> owner, JsonObject target, Ctx ctx,
                                    int elementIndex, int depth, Map<String, Object> captured) {
        // Fields carrying @After are generated last so the value they depend on already exists.
        List<Field> fields = annotatedFields(owner);
        Map<String, Object> generated = new HashMap<String, Object>();
        for (Field field : fields) {
            if (field.getAnnotation(After.class) == null) {
                writeField(field, owner, target, ctx, elementIndex, depth, generated);
            }
        }
        for (Field field : fields) {
            if (field.getAnnotation(After.class) != null) {
                writeField(field, owner, target, ctx, elementIndex, depth, generated);
            }
        }
        if (captured != null) {
            captured.putAll(generated);
        }
    }

    private static void writeField(Field field, Class<?> owner, JsonObject target, Ctx ctx,
                                   int elementIndex, int depth, Map<String, Object> generated) {
        String path = field.getAnnotation(Path.class).value();
        FieldKind kind = field.getAnnotation(Kind.class).value();

        // Presence precedence: MISSING beats NULL beats EMPTY beats a generated value.
        Optional optional = field.getAnnotation(Optional.class);
        if (optional != null && ctx.random.nextDouble() < optional.prob()) {
            return;
        }
        Nullable nullable = field.getAnnotation(Nullable.class);
        if (nullable != null && ctx.random.nextDouble() < nullable.prob()) {
            PathWriter.set(target, path, null);
            return;
        }
        Empty empty = field.getAnnotation(Empty.class);
        if (empty != null && ctx.random.nextDouble() < empty.prob()) {
            PathWriter.set(target, path, emptyValue(kind, field));
            return;
        }

        switch (kind) {
            case SCALAR: {
                Object value = scalarValue(field, owner, field.getType(), ctx, elementIndex, generated);
                PathWriter.set(target, path, value);
                generated.put(field.getName(), value);
                break;
            }
            case OBJECT:
                PathWriter.set(target, path, buildObject(field.getType(), ctx, elementIndex, depth));
                break;
            case ARRAY_OF_SCALAR:
                PathWriter.set(target, path, scalarArray(field, owner, ctx,
                        raw(argOf(field.getGenericType(), 0)), generated));
                break;
            case ARRAY_OF_OBJECT:
                PathWriter.set(target, path, objectArray(field, ctx, depth,
                        raw(argOf(field.getGenericType(), 0))));
                break;
            case ARRAY_OF_ARRAY_OF_SCALAR:
                PathWriter.set(target, path, nestedScalarArray(field, owner, ctx,
                        raw(argOf(argOf(field.getGenericType(), 0), 0)), generated));
                break;
            case ARRAY_OF_ARRAY_OF_OBJECT:
                PathWriter.set(target, path, nestedObjectArray(field, ctx, depth,
                        raw(argOf(argOf(field.getGenericType(), 0), 0))));
                break;
            case RECURSIVE_SELF:
                PathWriter.set(target, path, recursiveSelf(field, owner, ctx, depth));
                break;
            case MIXED_TYPE: {
                Object value = mixedValue(field, owner, ctx, elementIndex, generated);
                PathWriter.set(target, path, value);
                generated.put(field.getName(), value);
                break;
            }
            case OBJECT_DYNAMIC_KEYS:
                PathWriter.set(target, path, dynamicKeyedObject(field, owner, ctx, depth));
                break;
            default:
                throw new IllegalStateException("Unhandled FieldKind: " + kind);
        }
    }

    // ── FieldKind handlers ────────────────────────────────────────────────

    private static JsonArray scalarArray(Field field, Class<?> owner, Ctx ctx,
                                         Class<?> elementType, Map<String, Object> generated) {
        ArraySize size = field.getAnnotation(ArraySize.class);
        int n = arrayLength(size, ctx, false);
        JsonArray array = JsonArray.create();
        for (int i = 0; i < n; i++) {
            array.add(scalarValue(field, owner, elementType, ctx, i + 1, generated));
        }
        return array;
    }

    private static JsonArray objectArray(Field field, Ctx ctx, int depth, Class<?> elementType) {
        ArraySize size = field.getAnnotation(ArraySize.class);
        int n = arrayLength(size, ctx, false);
        JsonArray array = JsonArray.create();
        for (int i = 0; i < n; i++) {
            array.add(buildObject(elementType, ctx, i + 1, depth));
        }
        return array;
    }

    private static JsonArray nestedScalarArray(Field field, Class<?> owner, Ctx ctx,
                                               Class<?> elementType, Map<String, Object> generated) {
        ArraySize size = field.getAnnotation(ArraySize.class);
        int outer = arrayLength(size, ctx, true);
        JsonArray array = JsonArray.create();
        for (int i = 0; i < outer; i++) {
            int inner = arrayLength(size, ctx, false);
            JsonArray innerArray = JsonArray.create();
            for (int j = 0; j < inner; j++) {
                innerArray.add(scalarValue(field, owner, elementType, ctx, j + 1, generated));
            }
            array.add(innerArray);
        }
        return array;
    }

    private static JsonArray nestedObjectArray(Field field, Ctx ctx, int depth, Class<?> elementType) {
        ArraySize size = field.getAnnotation(ArraySize.class);
        int outer = arrayLength(size, ctx, true);
        JsonArray array = JsonArray.create();
        for (int i = 0; i < outer; i++) {
            int inner = arrayLength(size, ctx, false);
            JsonArray innerArray = JsonArray.create();
            for (int j = 0; j < inner; j++) {
                innerArray.add(buildObject(elementType, ctx, j + 1, depth));
            }
            array.add(innerArray);
        }
        return array;
    }

    private static JsonArray recursiveSelf(Field field, Class<?> owner, Ctx ctx, int depth) {
        RecursionDepth rd = field.getAnnotation(RecursionDepth.class);
        if (rd == null) {
            throw new IllegalStateException(owner.getName() + "." + field.getName()
                    + " is RECURSIVE_SELF but has no @RecursionDepth");
        }
        JsonArray array = JsonArray.create();
        boolean descend;
        if (depth >= rd.max()) {
            descend = false;
        } else if (depth < rd.min()) {
            descend = true;
        } else {
            descend = ctx.random.nextDouble() < rd.continueProb();
        }
        if (descend) {
            int children = between(ctx, rd.childrenMin(), rd.childrenMax());
            for (int i = 0; i < children; i++) {
                array.add(buildObject(owner, ctx, i + 1, depth + 1));
            }
        }
        return array;
    }

    /** Same path, a different scalar type per document. */
    private static Object mixedValue(Field field, Class<?> owner, Ctx ctx,
                                     int elementIndex, Map<String, Object> generated) {
        OneOf oneOf = field.getAnnotation(OneOf.class);
        if (oneOf == null || oneOf.value().length == 0) {
            throw new IllegalStateException(owner.getName() + "." + field.getName()
                    + " is MIXED_TYPE but has no @OneOf candidates");
        }
        Class<?>[] candidates = oneOf.value();
        Class<?> chosen = candidates[ctx.random.nextInt(candidates.length)];
        if (chosen == void.class || chosen == Void.class) {
            return null;
        }
        return scalarValue(field, owner, chosen, ctx, elementIndex, generated);
    }

    /** Object keyed by data rather than schema: { "G47085637": { ... }, ... }. */
    private static JsonObject dynamicKeyedObject(Field field, Class<?> owner, Ctx ctx, int depth) {
        ArraySize size = field.getAnnotation(ArraySize.class);
        int n = arrayLength(size, ctx, false);
        Class<?> valueType = raw(argOf(field.getGenericType(), 1));
        ChoiceFrom keySource = field.getAnnotation(ChoiceFrom.class);
        if (keySource == null) {
            throw new IllegalStateException(owner.getName() + "." + field.getName()
                    + " is OBJECT_DYNAMIC_KEYS but has no @ChoiceFrom naming the key pool");
        }
        // A size the pool cannot satisfy is a shape-authoring mistake, not a runtime condition,
        // so it is rejected rather than quietly producing a smaller object.
        int poolSize = PoolResolver.size(owner, keySource.value());
        int maxRequested = size == null ? 3 : Math.max(size.max(), size.largeProb() > 0 ? size.largeMax() : size.max());
        if (maxRequested > poolSize) {
            throw new IllegalStateException(owner.getName() + "." + field.getName()
                    + " asks for up to " + maxRequested + " distinct keys but pool '"
                    + keySource.value() + "' holds only " + poolSize);
        }
        JsonObject obj = JsonObject.create();
        // Keys are sampled without replacement: a repeat would overwrite the previous entry and
        // silently leave fewer entries than @ArraySize asked for.
        Set<String> used = new HashSet<String>();
        for (int i = 0; i < n; i++) {
            String key;
            do {
                key = String.valueOf(PoolResolver.pick(owner, keySource.value(), ctx.random));
            } while (!used.add(key));
            obj.put(key, buildObject(valueType, ctx, i + 1, depth));
        }
        return obj;
    }

    private static Object emptyValue(FieldKind kind, Field field) {
        switch (kind) {
            case ARRAY_OF_SCALAR:
            case ARRAY_OF_OBJECT:
            case ARRAY_OF_ARRAY_OF_SCALAR:
            case ARRAY_OF_ARRAY_OF_OBJECT:
            case RECURSIVE_SELF:
                return JsonArray.create();
            case OBJECT:
            case OBJECT_DYNAMIC_KEYS:
                return JsonObject.create();
            default:
                return "";
        }
    }

    // ── value generation ──────────────────────────────────────────────────

    private static Object scalarValue(Field field, Class<?> owner, Class<?> type, Ctx ctx,
                                      int elementIndex, Map<String, Object> generated) {
        if (field.getAnnotation(Sequence.class) != null) {
            return elementIndex;
        }
        After after = field.getAnnotation(After.class);
        if (after != null) {
            return afterValue(field, after, generated, ctx);
        }
        Identity identity = field.getAnnotation(Identity.class);
        if (identity != null) {
            return identityValue(identity.value(), ctx);
        }
        ChoiceFrom choice = field.getAnnotation(ChoiceFrom.class);
        if (choice != null && type == String.class) {
            return PoolResolver.pick(owner, choice.value(), ctx.random);
        }
        DateString date = field.getAnnotation(DateString.class);
        if (date != null) {
            return dateValue(date, ctx);
        }
        Precision precision = field.getAnnotation(Precision.class);
        if (precision != null || type == BigDecimal.class) {
            return bigDecimalValue(field, precision, ctx);
        }
        LongRange longRange = field.getAnnotation(LongRange.class);
        if (longRange != null) {
            return longBetween(ctx, longRange.min(), longRange.max());
        }
        Range range = field.getAnnotation(Range.class);
        if (type == int.class || type == Integer.class) {
            return range == null ? ctx.random.nextInt(1000) : (int) numeric(range, ctx);
        }
        if (type == long.class || type == Long.class) {
            return range == null ? (long) ctx.random.nextInt(1000000) : (long) numeric(range, ctx);
        }
        if (type == double.class || type == Double.class) {
            double raw = range == null ? ctx.random.nextDouble() * 1000 : numeric(range, ctx);
            return Math.round(raw * 100.0) / 100.0;
        }
        if (type == boolean.class || type == Boolean.class) {
            return ctx.random.nextBoolean();
        }
        return stringValue(field, ctx);
    }

    /** A date a bounded distance after another field's date, so the pair stays coherent. */
    private static Object afterValue(Field field, After after, Map<String, Object> generated, Ctx ctx) {
        Object base = generated.get(after.value());
        LocalDateTime start;
        DateString spec = field.getAnnotation(DateString.class);
        String pattern = spec == null ? "yyyy-MM-dd'T'HH:mm:ss'Z'" : patternOf(spec, ctx);
        if (base instanceof String) {
            try {
                start = LocalDateTime.parse((String) base, DateTimeFormatter.ofPattern(pattern));
            } catch (RuntimeException e) {
                start = EPOCH_BASE;
            }
        } else if (base instanceof Number) {
            start = LocalDateTime.ofEpochSecond(((Number) base).longValue() / 1000, 0, ZoneOffset.UTC);
        } else {
            start = EPOCH_BASE;
        }
        LocalDateTime when = after.maxMinutes() > 0
                ? start.plus(between(ctx, after.minMinutes(), after.maxMinutes()), ChronoUnit.MINUTES)
                : start.plus(between(ctx, after.minDays(), after.maxDays()), ChronoUnit.DAYS);
        return format(when, pattern, ctx);
    }

    private static String patternOf(DateString spec, Ctx ctx) {
        String[] formats = spec.formats();
        return formats.length == 0 ? spec.format() : formats[ctx.random.nextInt(formats.length)];
    }

    /**
     * Patterns carrying an offset symbol need a zoned value to format against, and get a
     * varied offset so the corpus is not uniformly UTC.
     */
    private static String format(LocalDateTime when, String pattern, Ctx ctx) {
        if (hasOffsetSymbol(pattern)) {
            return when.atOffset(OFFSETS[ctx.random.nextInt(OFFSETS.length)])
                    .format(DateTimeFormatter.ofPattern(pattern));
        }
        return when.format(DateTimeFormatter.ofPattern(pattern));
    }

    /** True when the pattern contains an offset/zone letter outside a quoted literal. */
    private static boolean hasOffsetSymbol(String pattern) {
        boolean quoted = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\'') {
                quoted = !quoted;
            } else if (!quoted && (c == 'x' || c == 'X' || c == 'Z' || c == 'O' || c == 'V' || c == 'z')) {
                return true;
            }
        }
        return false;
    }

    private static Object dateValue(DateString spec, Ctx ctx) {
        int span = spec.daysBack() + spec.daysForward();
        LocalDateTime when = EPOCH_BASE
                .minus(spec.daysBack(), ChronoUnit.DAYS)
                .plus(span > 0 ? ctx.random.nextInt(span) : 0, ChronoUnit.DAYS)
                .plus(ctx.random.nextInt(24), ChronoUnit.HOURS)
                .plus(ctx.random.nextInt(60), ChronoUnit.MINUTES)
                .plus(ctx.random.nextInt(60), ChronoUnit.SECONDS)
                .plus(ctx.random.nextInt(1000000), ChronoUnit.MICROS);
        if (spec.epochProb() > 0 && ctx.random.nextDouble() < spec.epochProb()) {
            return when.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        return format(when, patternOf(spec, ctx), ctx);
    }

    private static BigDecimal bigDecimalValue(Field field, Precision precision, Ctx ctx) {
        int scale = precision == null ? 2 : precision.scale();
        Range range = field.getAnnotation(Range.class);
        LongRange longRange = field.getAnnotation(LongRange.class);
        BigDecimal value;
        if (longRange != null) {
            value = BigDecimal.valueOf(longBetween(ctx, longRange.min(), longRange.max()));
        } else if (range != null) {
            value = BigDecimal.valueOf(numeric(range, ctx));
        } else {
            value = BigDecimal.valueOf(ctx.random.nextDouble() * 1000000);
        }
        return value.setScale(scale, RoundingMode.HALF_UP);
    }

    private static String stringValue(Field field, Ctx ctx) {
        Length length = field.getAnnotation(Length.class);
        if (length == null) {
            return filler(ctx, 24);
        }
        int n;
        if (length.largeProb() > 0 && ctx.random.nextDouble() < length.largeProb()) {
            n = between(ctx, length.max() + 1, Math.max(length.max() + 1, length.largeMax()));
        } else {
            n = between(ctx, length.min(), length.max());
        }
        return filler(ctx, n);
    }

    private static double numeric(Range range, Ctx ctx) {
        return range.min() + ctx.random.nextDouble() * (range.max() - range.min());
    }

    private static long longBetween(Ctx ctx, long lo, long hi) {
        if (hi <= lo) {
            return lo;
        }
        long span = hi - lo;
        return lo + (long) (ctx.random.nextDouble() * span);
    }

    /** Array length, with an optional long tail so a few documents carry outsized arrays. */
    private static int arrayLength(ArraySize size, Ctx ctx, boolean outer) {
        if (size == null) {
            return between(ctx, 1, 3);
        }
        int lo = outer ? size.outerMin() : size.min();
        int hi = outer ? size.outerMax() : size.max();
        if (!outer && size.largeProb() > 0 && ctx.random.nextDouble() < size.largeProb()) {
            return between(ctx, hi + 1, Math.max(hi + 1, size.largeMax()));
        }
        return between(ctx, lo, hi);
    }


    /**
     * Whole sentences up to the target length. Sentences are emitted until the next one would
     * overshoot, so the cap is never exceeded even though the result lands a little under it:
     * the ceiling is what index-key and document-size limits care about, and truncating
     * mid-word to hit an exact count would defeat the point of generating prose at all.
     */
    private static String filler(Ctx ctx, int length) {
        // Short values get clipped phrases, longer ones whole sentences; both stop before
        // the next unit would overshoot.
        boolean shortForm = length < 80;
        String separator = shortForm ? "; " : " ";
        StringBuilder sb = new StringBuilder(length + 64);
        while (true) {
            String unit = shortForm
                    ? render(SHORT_TEMPLATES[ctx.random.nextInt(SHORT_TEMPLATES.length)], ctx)
                    : sentence(ctx);
            int added = unit.length() + (sb.length() > 0 ? separator.length() : 0);
            if (sb.length() + added > length) {
                break;
            }
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(unit);
        }
        if (sb.length() == 0) {
            // The first pick was too long for the target: try the others before resorting to
            // a clip, so narrow fields still read as whole phrases.
            int start = ctx.random.nextInt(SHORT_TEMPLATES.length);
            String fallback = null;
            for (int i = 0; i < SHORT_TEMPLATES.length; i++) {
                String candidate = render(SHORT_TEMPLATES[(start + i) % SHORT_TEMPLATES.length], ctx);
                if (candidate.length() <= length) {
                    return candidate;
                }
                fallback = candidate;
            }
            return clipToWord(fallback, length);
        }
        return sb.toString();
    }

    /** Trims to the last whole word that fits, so a clipped value never ends mid-word. */
    private static String clipToWord(String text, int length) {
        if (text.length() <= length) {
            return text;
        }
        String clipped = text.substring(0, length);
        int lastSpace = clipped.lastIndexOf(' ');
        return (lastSpace > 0 ? clipped.substring(0, lastSpace) : clipped).trim();
    }

    private static String sentence(Ctx ctx) {
        return render(SENTENCES[ctx.random.nextInt(SENTENCES.length)], ctx);
    }

    private static String render(String template, Ctx ctx) {
        StringBuilder out = new StringBuilder(template.length() + 32);
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c != '{') {
                out.append(c);
                i++;
                continue;
            }
            int close = template.indexOf('}', i);
            String slot = template.substring(i + 1, close);
            out.append(slotValue(slot, ctx));
            i = close + 1;
        }
        if (out.length() > 0) {
            out.setCharAt(0, Character.toUpperCase(out.charAt(0)));
        }
        return fixArticles(out.toString());
    }

    /** Slot values are substituted blind, so "a" before a vowel has to be corrected after. */
    private static String fixArticles(String text) {
        return text.replaceAll("\\ba (?=[aeiouAEIOU])", "an ")
                   .replaceAll("\\bA (?=[aeiouAEIOU])", "An ");
    }

    private static String slotValue(String slot, Ctx ctx) {
        if ("item".equals(slot)) {
            return ITEMS[ctx.random.nextInt(ITEMS.length)];
        }
        if ("action".equals(slot)) {
            return ACTIONS[ctx.random.nextInt(ACTIONS.length)];
        }
        if ("reason".equals(slot)) {
            return REASONS[ctx.random.nextInt(REASONS.length)];
        }
        if ("actor".equals(slot)) {
            return ACTORS[ctx.random.nextInt(ACTORS.length)];
        }
        if ("status".equals(slot)) {
            return STATUSES_TEXT[ctx.random.nextInt(STATUSES_TEXT.length)];
        }
        if ("branch".equals(slot)) {
            return BRANCHES[ctx.random.nextInt(BRANCHES.length)];
        }
        if ("ref".equals(slot)) {
            return "REF-" + String.format("%06d", ctx.random.nextInt(1000000));
        }
        if ("date".equals(slot)) {
            return String.format("%04d-%02d-%02d",
                    2025 + ctx.random.nextInt(2), 1 + ctx.random.nextInt(12), 1 + ctx.random.nextInt(28));
        }
        throw new IllegalStateException("Unknown sentence slot: " + slot);
    }

    private static String resolveIdPattern(String pattern, Map<String, Object> values, long seq) {
        String result = pattern.replace("{seq}", String.format("%07d", seq));
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            result = result.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
        }
        return result;
    }

    // ── reflection helpers ────────────────────────────────────────────────

    /** Annotated instance fields, sorted by name so generation is reproducible across JVMs. */
    private static List<Field> annotatedFields(Class<?> owner) {
        List<Field> fields = new ArrayList<Field>();
        for (Field field : owner.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (field.getAnnotation(Path.class) == null || field.getAnnotation(Kind.class) == null) {
                continue;
            }
            field.setAccessible(true);
            fields.add(field);
        }
        fields.sort(Comparator.comparing(Field::getName));
        return fields;
    }

    private static Type argOf(Type type, int index) {
        if (type instanceof ParameterizedType) {
            Type[] args = ((ParameterizedType) type).getActualTypeArguments();
            if (index < args.length) {
                return args[index];
            }
        }
        throw new IllegalStateException("Expected a parameterized type with argument " + index + ", got: " + type);
    }

    private static Class<?> raw(Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        throw new IllegalStateException("Cannot resolve a raw class from: " + type);
    }

    private static int between(Ctx ctx, int lo, int hi) {
        return hi <= lo ? lo : lo + ctx.random.nextInt(hi - lo + 1);
    }
}
