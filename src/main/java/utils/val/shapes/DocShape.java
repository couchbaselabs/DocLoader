package utils.val.shapes;

import utils.val.shapes.annotations.Shape;

/**
 * Marker for a document type. Implementations declare their structure with field
 * annotations and are discovered via META-INF/services/utils.val.shapes.DocShape.
 */
public interface DocShape {

    default String type() {
        return shape().type();
    }

    default int weight() {
        return shape().weight();
    }

    default Shape shape() {
        Shape shape = getClass().getAnnotation(Shape.class);
        if (shape == null) {
            throw new IllegalStateException(getClass().getName() + " is missing @Shape");
        }
        return shape;
    }
}
