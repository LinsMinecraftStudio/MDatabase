package io.github.lijinhong11.mdatabase.serialization.converters;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;

import java.util.UUID;

public class UUIDConverter implements ObjectConverter<UUID> {
    @Override
    public UUID convert(Object o) {
        if (o instanceof String s) {
            return UUID.fromString(s);
        }

        return (UUID) o;
    }

    @Override
    public String getSqlType() {
        return "TEXT";
    }
}
