package io.github.lijinhong11.mdatabase.serialization.converters;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongConverter implements ObjectConverter<AtomicLong> {
    @Override
    public @Nullable AtomicLong convert(@Nullable Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof Number number) {
            return new AtomicLong(number.longValue());
        }

        return new AtomicLong(Long.parseLong(String.valueOf(o)));
    }

    @Override
    public Object convertBack(AtomicLong t) {
        return t.get();
    }

    @Override
    public String getSqlType() {
        return "BIGINT";
    }
}
