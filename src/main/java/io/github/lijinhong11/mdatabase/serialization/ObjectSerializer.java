package io.github.lijinhong11.mdatabase.serialization;

import io.github.lijinhong11.mdatabase.serialization.annotations.Column;
import io.github.lijinhong11.mdatabase.serialization.annotations.Converter;
import io.github.lijinhong11.mdatabase.serialization.converters.UUIDConverter;
import io.github.lijinhong11.mdatabase.serialization.exceptions.CannotInstanceException;
import io.github.lijinhong11.mdatabase.serialization.exceptions.SerializationException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class ObjectSerializer {
    private static final Map<Class<?>, ObjectConverter<?>> CONVERTERS = new ConcurrentHashMap<>();
    private static final Map<Class<?>, List<Field>> FIELD_CACHE = new ConcurrentHashMap<>();

    private ObjectSerializer() {}

    static {
        registerConverter(UUID.class, new UUIDConverter());
    }

    public static <T> void registerConverter(Class<T> clazz, ObjectConverter<T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    public static <T> T serializeOne(Class<T> clazz, ResultSet set) {
        try {
            T obj = clazz.getDeclaredConstructor().newInstance();
            for (Field field : getCachedFields(clazz)) {
                try {
                    setFieldValue(obj, field, set);
                } catch (SQLException e) {
                    if (!isColumnExists(set, getColumnName(field))) {
                        continue;
                    }

                    throw new SerializationException("Failed to set field " + field.getName(), e);
                }
            }
            return obj;
        } catch (Exception e) {
            throw new CannotInstanceException(clazz, e);
        }
    }

    public static <T> List<T> serializeMulti(Class<T> clazz, ResultSet set) {
        List<T> list = new ArrayList<>();
        try {
            while (set.next()) {
                list.add(serializeOne(clazz, set));
            }
        } catch (SQLException e) {
            throw new SerializationException("Failed to serialize multiple objects", e);
        }
        return list;
    }

    public static Object convertBack(Object obj) {
        if (obj == null) {
            return null;
        }

        return convertBack(obj, obj.getClass());
    }

    public static Object convertBack(Object obj, Class<?> clazz) {
        if (obj == null) {
            return null;
        }

        if (clazz.isEnum()) {
            return obj.toString();
        }

        if (CONVERTERS.containsKey(clazz)) {
            return CONVERTERS.get(clazz).convertBack(obj);
        }

        return obj;
    }

    private static <T> List<Field> getCachedFields(Class<T> clazz) {
        if (FIELD_CACHE.containsKey(clazz)) {
            return FIELD_CACHE.get(clazz);
        } else {
            List<Field> fields = getAllFields(clazz);
            FIELD_CACHE.put(clazz, fields);

            return fields;
        }
    }

    private static boolean isColumnExists(ResultSet set, String columnName) {
        try {
            return set.findColumn(columnName) > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    private static <T> void setFieldValue(T obj, Field field, ResultSet set) throws SQLException, IllegalAccessException {
        String columnName = getColumnName(field);
        Object value = getValueFromResultSet(set, columnName, field.getType());
        if (value != null) {
            field.set(obj, value);
        }
    }

    @Nullable
    public static String getColumnName(Field field) {
        if (field.isAnnotationPresent(Column.class)) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                if (column.name().equals(Column.AUTO_NAMED)) {
                    return field.getName();
                }

                return column.name();
            }
        }

        return null;
    }

    private static Object getValueFromResultSet(ResultSet set, String columnName, Class<?> type) throws SQLException {
        if (type == String.class) {
            return set.getString(columnName);
        } else if (type == int.class || type == Integer.class) {
            return set.getInt(columnName);
        } else if (type == long.class || type == Long.class) {
            return set.getLong(columnName);
        } else if (type == boolean.class || type == Boolean.class) {
            return set.getBoolean(columnName);
        } else if (type == double.class || type == Double.class) {
            return set.getDouble(columnName);
        } else if (type == float.class || type == Float.class) {
            return set.getFloat(columnName);
        } else if (type == Date.class) {
            return set.getDate(columnName);
        } else if (type == Timestamp.class) {
            return set.getTimestamp(columnName);
        } else if (type == Time.class) {
            return set.getTime(columnName);
        } else if (type == BigDecimal.class) {
            return set.getBigDecimal(columnName);
        } else if (type == Blob.class) {
            return set.getBlob(columnName);
        } else if (type == Clob.class) {
            return set.getClob(columnName);
        } else if (type == NClob.class) {
            return set.getNClob(columnName);
        } else if (type == byte[].class) {
            return set.getBytes(columnName);
        } else if (type.isEnum()) {
            String value = set.getString(columnName);
            return value != null ? Enum.valueOf((Class<? extends Enum>)type, value) : null;
        } else {
            ObjectConverter<?> converter = CONVERTERS.get(type);
            if (converter != null) {
                return converter.convert(set.getObject(columnName));
            }
        }

        throw new UnsupportedOperationException("Unsupported type: " + type.getName());
    }

    public static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isFinal(field.getModifiers())) {
                continue;
            }

            if (field.isAnnotationPresent(Converter.class)) {
                try {
                    CONVERTERS.put(field.getType(), field.getAnnotation(Converter.class).value().getDeclaredConstructor().newInstance());
                } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException |
                         InstantiationException e) {
                    throw new RuntimeException(e);
                }
            }

            field.setAccessible(true);
            fields.add(field);
        }

        return fields;
    }

    public static String getSqlType(Class<?> javaType) {
        if (javaType == String.class)
            return "TEXT";
        if (javaType == int.class || javaType == Integer.class)
            return "INTEGER";
        if (javaType == long.class || javaType == Long.class)
            return "BIGINT";
        if (javaType == boolean.class || javaType == Boolean.class)
            return "BOOLEAN";
        if (javaType == double.class || javaType == Double.class)
            return "DOUBLE";
        if (javaType == float.class || javaType == Float.class)
            return "FLOAT";
        if (javaType == Date.class || javaType == Timestamp.class)
            return "DATETIME";
        if (javaType == BigDecimal.class)
            return "DECIMAL(18,6)";
        if (javaType.isEnum())
            return "VARCHAR(100)";
        if (javaType.isArray())
            return "BLOB";


        ObjectConverter<?> converter = ObjectSerializer.CONVERTERS.get(javaType);
        if (converter != null) {
            return converter.getSqlType();
        }

        throw new IllegalArgumentException("Unsupported type: " + javaType.getName());
    }
}
