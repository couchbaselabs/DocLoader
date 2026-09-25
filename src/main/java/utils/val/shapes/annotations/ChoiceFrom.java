package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Names a static List field on the declaring class (or its enclosing class)
 * that this field's values are drawn from.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ChoiceFrom {
    String value();
}
