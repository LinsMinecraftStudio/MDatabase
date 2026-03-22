package io.github.lijinhong11.mdatabase.serialization.converters;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIntegerConverter implements ObjectConverter<AtomicInteger> {
    @Override
    public @Nullable AtomicInteger convert(@Nullable Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof Number number) {
            return new AtomicInteger(number.intValue());
        }

        return new AtomicInteger(Integer.parseInt(String.valueOf(o)));
    }

    @Override
    public Object convertBack(AtomicInteger t) {
        return t.get();
    }

    @Override
    public String getSqlType() {
        return "INTEGER";
    }
}
