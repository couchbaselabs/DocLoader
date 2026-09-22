package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RecursionDepth {
    /** Levels always produced before the probability roll starts applying. */
    int min() default 1;

    /** Hard stop; recursion never exceeds this depth regardless of continueProb. */
    int max() default 5;

    /** Probability of descending one more level, evaluated once past min(). */
    double continueProb() default 0.6;

    /** Number of child nodes produced at each level that continues. */
    int childrenMin() default 1;

    int childrenMax() default 2;
}
