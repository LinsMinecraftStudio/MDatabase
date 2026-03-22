package io.github.lijinhong11.mdatabase.serialization.converters;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicBooleanConverter implements ObjectConverter<AtomicBoolean> {
    @Override
    public @Nullable AtomicBoolean convert(@Nullable Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof Boolean value) {
            return new AtomicBoolean(value);
        }

        if (o instanceof Number number) {
            return new AtomicBoolean(number.intValue() != 0);
        }

        return new AtomicBoolean(Boolean.parseBoolean(String.valueOf(o)));
    }

    @Override
    public Object convertBack(AtomicBoolean t) {
        return t.get();
    }

    @Override
    public String getSqlType() {
        return "BOOLEAN";
    }
}
