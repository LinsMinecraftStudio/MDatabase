package io.github.lijinhong11.mdatabase.serialization.annotations;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Converter {
    Class<? extends ObjectConverter<?>> value();
}
