package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Shape {
    /** Value written to the "_type" discriminator field, and the name used in the type filter. */
    String type();

    /** Relative selection weight against all other registered shapes. */
    int weight();

    /** Pattern for the "_id" field. Placeholders: {fieldName} and {seq}. */
    String idPattern() default "";
}
