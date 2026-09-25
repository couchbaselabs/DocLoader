package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Writes an empty value with the given probability: [] for array kinds, {} for OBJECT,
 * and "" for String scalars. All three are distinct from both null and MISSING.
 *
 * Evaluated after Optional and Nullable.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Empty {
    double prob();
}
