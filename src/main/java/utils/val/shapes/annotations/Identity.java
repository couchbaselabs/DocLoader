package utils.val.shapes.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Draws from the shared identity pool rather than a pool local to the shape, so the same
 * person recurs across document types and correlation queries have something to join on.
 *
 * One identity is chosen per document and every @Identity field on it reads from that same
 * identity, so a document never mixes one person's email with another's name.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Identity {

    Attribute value();

    enum Attribute {
        /** Canonical address, re-cased per document so LOWER() matching is meaningful. */
        EMAIL,
        /** Canonical address, always exactly as stored in the pool. */
        EMAIL_EXACT,
        FIRST_NAME,
        LAST_NAME,
        FULL_NAME,
        /** Canonical number, reformatted per document so normalisation has work to do. */
        PHONE,
        PHONE_EXACT
    }
}
