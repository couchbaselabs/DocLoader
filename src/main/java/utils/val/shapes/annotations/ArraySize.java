package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ArraySize {
    int min() default 1;

    int max() default 3;

    /** Outer array bounds for the ARRAY_OF_ARRAY_* kinds; min()/max() then apply to the inner arrays. */
    int outerMin() default 1;

    int outerMax() default 3;

    /**
     * Probability of drawing from the long tail instead of [min,max], so a small fraction of
     * documents carry outsized arrays. Real corpora are skewed this way and it is what
     * exercises planner cost estimation rather than one uniform size.
     */
    double largeProb() default 0.0;

    int largeMax() default 200;
}
