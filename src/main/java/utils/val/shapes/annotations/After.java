package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Makes this field depend on another field of the same object, so the pair stays coherent
 * (an end date genuinely after its start date, rather than independently random).
 *
 * Dependent fields are generated after their target, whatever the declaration order.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface After {
    /** Java field name on the same class whose value this one follows. */
    String value();

    int minDays() default 1;

    int maxDays() default 30;

    /**
     * Minute-level offsets, for pairs that are minutes apart rather than days. When
     * maxMinutes() is set it takes precedence over the day bounds.
     */
    int minMinutes() default 0;

    int maxMinutes() default 0;
}
