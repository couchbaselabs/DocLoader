package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Writes an explicit JSON null with the given probability. Distinct from {@link Optional},
 * which omits the key entirely: N1QL treats IS NULL and IS MISSING differently.
 *
 * Evaluated after Optional and before Empty.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Nullable {
    double prob();
}
