package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Character-length target for a generated String, with an optional long tail so a small
 * fraction of documents carry outsized values (index key limits, planner cost estimation).
 *
 * max() is a hard ceiling and is never exceeded. min() is a target rather than a guarantee:
 * text is emitted in whole words and phrases, so a value can land short of it rather than
 * being truncated mid-word. The same applies within the long-tail branch.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Length {
    int min() default 8;

    int max() default 32;

    /** Probability of drawing from the long tail instead of [min,max]. */
    double largeProb() default 0.0;

    int largeMax() default 4096;
}
