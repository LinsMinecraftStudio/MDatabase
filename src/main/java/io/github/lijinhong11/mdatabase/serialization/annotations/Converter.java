package io.github.lijinhong11.mdatabase.serialization.annotations;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a custom {@link ObjectConverter} for a field.
 * The converter handles type translation between the Java type and the SQL column.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Converter {
    /**
     * @return the converter class implementing {@link ObjectConverter}
     */
    Class<? extends ObjectConverter<?>> value();
}
