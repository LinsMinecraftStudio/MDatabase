package io.github.lijinhong11.mdatabase.serialization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a numeric {@code @Column} field as auto-increment.
 * Applied to the {@code CREATE TABLE} statement. The field type must be a {@link Number} subclass.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface AutoIncrement {
}
