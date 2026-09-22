package utils.val.shapes;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

/**
 * Discovers every DocShape on the classpath and picks between them by declared weight.
 * Adding a shape means adding a service-file line; nothing here changes.
 *
 * An optional type filter restricts loading to a subset and may override individual
 * weights; the mix renormalises implicitly because selection is relative to their sum.
 */
public class ShapeRegistry {

    private final List<Class<?>> shapeClasses = new ArrayList<Class<?>>();
    private final List<String> shapeTypes = new ArrayList<String>();
    private final int[] cumulativeWeights;
    private final int totalWeight;

    public ShapeRegistry() {
        this(null);
    }

    /**
     * @param typeWeights type names to load mapped to a weight override, where a null value
     *                    keeps the shape's declared weight. Null or empty loads every type.
     */
    public ShapeRegistry(Map<String, Integer> typeWeights) {
        boolean filtered = typeWeights != null && !typeWeights.isEmpty();
        List<DocShape> discovered = new ArrayList<DocShape>();
        Set<String> available = new TreeSet<String>();
        for (DocShape shape : ServiceLoader.load(DocShape.class, ShapeRegistry.class.getClassLoader())) {
            available.add(shape.type());
            if (!filtered || typeWeights.containsKey(shape.type())) {
                discovered.add(shape);
            }
        }
        if (available.isEmpty()) {
            throw new IllegalStateException(
                    "No DocShape implementations found on the classpath. "
                            + "Check META-INF/services/utils.val.shapes.DocShape");
        }
        if (filtered) {
            Set<String> unknown = new TreeSet<String>(typeWeights.keySet());
            unknown.removeAll(available);
            if (!unknown.isEmpty()) {
                throw new IllegalArgumentException(
                        "Unknown document type(s): " + unknown + ". Available types: " + available);
            }
        }
        // Sorted so selection is reproducible regardless of ServiceLoader ordering.
        discovered.sort(Comparator.comparing(s -> s.getClass().getName()));

        this.cumulativeWeights = new int[discovered.size()];
        int running = 0;
        for (int i = 0; i < discovered.size(); i++) {
            DocShape shape = discovered.get(i);
            Integer override = filtered ? typeWeights.get(shape.type()) : null;
            running += override != null ? override.intValue() : shape.weight();
            this.cumulativeWeights[i] = running;
            this.shapeClasses.add(shape.getClass());
            this.shapeTypes.add(shape.type());
        }
        this.totalWeight = running;
    }

    public Class<?> pick(Random random) {
        int roll = random.nextInt(totalWeight);
        for (int i = 0; i < cumulativeWeights.length; i++) {
            if (roll < cumulativeWeights[i]) {
                return shapeClasses.get(i);
            }
        }
        return shapeClasses.get(shapeClasses.size() - 1);
    }

    public List<String> registeredTypes() {
        return shapeTypes;
    }
}
