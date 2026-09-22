package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import utils.val.shapes.engine.FieldKind;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Kind {
    FieldKind value();
}
