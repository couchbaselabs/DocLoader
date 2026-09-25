package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DateString {
    /** java.time pattern. Ignored when formats() is non-empty. */
    String format() default "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * Alternative patterns, one picked per document. Mixing offsets, precision and
     * separators is what makes date parsing a real test rather than a formality.
     */
    String[] formats() default {};

    int daysBack() default 365;

    int daysForward() default 0;

    /**
     * Probability the value is written as epoch milliseconds (a JSON number) instead of a
     * string, so the same path holds both representations across documents.
     */
    double epochProb() default 0.0;
}
