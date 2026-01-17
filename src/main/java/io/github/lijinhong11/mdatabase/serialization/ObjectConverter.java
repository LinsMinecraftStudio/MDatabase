package io.github.lijinhong11.mdatabase.serialization;

import org.jetbrains.annotations.Nullable;

public interface ObjectConverter<T> {
    @Nullable T convert(@Nullable Object o);

    Object convertBack(T t);

    String getSqlType();
}
