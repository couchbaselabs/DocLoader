package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Decimal places for a BigDecimal field, for exact-precision cases that a double cannot hold.
 * Bounds come from {@link Range} or {@link LongRange}; unbounded fields use a wide default.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Precision {
    int scale();
}
