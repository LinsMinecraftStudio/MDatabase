package io.github.lijinhong11.mdatabase.serialization.converters;

import io.github.lijinhong11.mdatabase.serialization.ObjectConverter;
import org.jetbrains.annotations.Nullable;

import java.util.Locale;

public class LocaleConverter implements ObjectConverter<Locale> {
    @Override
    public Locale convert(@Nullable Object o) {
        if (o instanceof String s) {
            return Locale.forLanguageTag(s);
        }

        if (o instanceof Locale l) {
            return l;
        }

        return null;
    }

    @Override
    public Object convertBack(Locale t) {
        return t.toLanguageTag();
    }

    @Override
    public String getSqlType() {
        return "TEXT";
    }
}
