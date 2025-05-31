package io.github.lijinhong11.mdatabase.serialization;

public interface ObjectConverter<T> {
    T convert(Object o);

    String getSqlType();
}
