package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Bounds for an integral scalar, held as longs so values above 2^53 stay exact.
 * {@link Range} is double-backed and loses precision there.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LongRange {
    long min();

    long max();
}
