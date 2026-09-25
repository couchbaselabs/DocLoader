package utils.val.shapes.engine;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves {@code @ChoiceFrom("NAME")} to a static List field declared on the owning
 * shape class or one of its enclosing classes. Lookups are cached per (class, name).
 */
public final class PoolResolver {

    private static final Map<String, List<?>> CACHE = new ConcurrentHashMap<String, List<?>>();

    private PoolResolver() {
    }

    public static int size(Class<?> owner, String poolName) {
        return resolve(owner, poolName).size();
    }

    public static Object pick(Class<?> owner, String poolName, Random random) {
        List<?> pool = resolve(owner, poolName);
        return pool.get(random.nextInt(pool.size()));
    }

    private static List<?> resolve(Class<?> owner, String poolName) {
        String cacheKey = owner.getName() + "#" + poolName;
        List<?> cached = CACHE.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        for (Class<?> c = owner; c != null; c = c.getEnclosingClass()) {
            try {
                Field field = c.getDeclaredField(poolName);
                field.setAccessible(true);
                Object value = field.get(null);
                if (value instanceof List && !((List<?>) value).isEmpty()) {
                    List<?> pool = (List<?>) value;
                    CACHE.put(cacheKey, pool);
                    return pool;
                }
            } catch (NoSuchFieldException ignored) {
                // walk outward to the enclosing class
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Pool '" + poolName + "' on " + c.getName() + " is not accessible", e);
            }
        }
        throw new IllegalStateException(
                "No non-empty static List field named '" + poolName + "' found on " + owner.getName()
                        + " or its enclosing classes");
    }
}
