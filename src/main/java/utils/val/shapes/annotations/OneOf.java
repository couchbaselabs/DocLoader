package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Candidate types for a MIXED_TYPE field: the same path holds a different type from one
 * document to the next, which is what drives collation, ordering and plan-selection bugs.
 *
 * Value generation for the chosen type reuses the ordinary scalar rules, so @ChoiceFrom,
 * @Range and @Length still apply where relevant.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface OneOf {
    Class<?>[] value();
}
