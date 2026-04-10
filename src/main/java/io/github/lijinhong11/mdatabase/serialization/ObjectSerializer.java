package io.github.lijinhong11.mdatabase.serialization;

import io.github.lijinhong11.mdatabase.exceptions.InstantiationFailedException;
import io.github.lijinhong11.mdatabase.exceptions.SerializationException;
import io.github.lijinhong11.mdatabase.serialization.annotations.Column;
import io.github.lijinhong11.mdatabase.serialization.annotations.Converter;
import io.github.lijinhong11.mdatabase.serialization.converters.AtomicBooleanConverter;
import io.github.lijinhong11.mdatabase.serialization.converters.AtomicIntegerConverter;
import io.github.lijinhong11.mdatabase.serialization.converters.AtomicLongConverter;
import io.github.lijinhong11.mdatabase.serialization.converters.LocaleConverter;
import io.github.lijinhong11.mdatabase.serialization.converters.UUIDConverter;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ObjectSerializer {
    private static final Map<Class<?>, ObjectConverter<?>> CONVERTERS = new ConcurrentHashMap<>();
    private static final Map<Class<?>, List<Field>> FIELD_CACHE = new ConcurrentHashMap<>();

    static {
        registerConverter(UUID.class, new UUIDConverter());
        registerConverter(Locale.class, new LocaleConverter());
        registerConverter(AtomicInteger.class, new AtomicIntegerConverter());
        registerConverter(AtomicLong.class, new AtomicLongConverter());
        registerConverter(AtomicBoolean.class, new AtomicBooleanConverter());
    }

    private ObjectSerializer() {
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
            throw new InstantiationFailedException(clazz, e);
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

    public static <T> Object convertBack(T obj) {
        if (obj == null) {
            return null;
        }

        return convertBack(obj, obj.getClass());
    }

    public static Object convertBack(Object obj, Type type) {
        if (obj == null) {
            return null;
        }

        Class<?> rawType = getRawType(type);
        if (rawType.isEnum()) {
            return obj.toString();
        }

        if (requiresBinarySerialization(rawType)) {
            return toBinary(normalizeBinaryValue(obj, type));
        }

        ObjectConverter<Object> converter = findConverter((Class<Object>) rawType);
        if (converter != null) {
            return converter.convertBack(obj);
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
        Object value = getValueFromResultSet(set, columnName, field.getGenericType());
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

    private static Object getValueFromResultSet(ResultSet set, String columnName, Type declaredType) throws SQLException {
        Class<?> type = getRawType(declaredType);
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
            return value != null ? Enum.valueOf((Class<? extends Enum>) type, value) : null;
        } else if (requiresBinarySerialization(type)) {
            return denormalizeBinaryValue(fromBinary(set.getObject(columnName)), declaredType);
        } else {
            ObjectConverter<?> converter = findConverter(type);
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

    public static String getSqlType(Type javaType) {
        Class<?> rawType = getRawType(javaType);
        if (rawType == String.class)
            return "TEXT";
        if (rawType == int.class || rawType == Integer.class)
            return "INTEGER";
        if (rawType == long.class || rawType == Long.class)
            return "BIGINT";
        if (rawType == boolean.class || rawType == Boolean.class)
            return "BOOLEAN";
        if (rawType == double.class || rawType == Double.class)
            return "DOUBLE";
        if (rawType == float.class || rawType == Float.class)
            return "FLOAT";
        if (rawType == Date.class || rawType == Timestamp.class)
            return "DATETIME";
        if (rawType == BigDecimal.class)
            return "DECIMAL(20,7)";
        if (rawType.isEnum())
            return "VARCHAR(100)";
        if (rawType.isArray())
            return "BLOB";
        if (requiresBinarySerialization(rawType))
            return "BLOB";

        ObjectConverter<?> converter = findConverter(rawType);
        if (converter != null) {
            return converter.getSqlType();
        }

        throw new IllegalArgumentException("Unsupported type: " + rawType.getName());
    }

    @Nullable
    private static Object fromBinary(@Nullable Object o) {
        if (o == null) {
            return null;
        }

        byte[] bytes;
        if (o instanceof byte[] rawBytes) {
            bytes = rawBytes;
        } else if (o instanceof Blob blob) {
            try {
                bytes = blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new SerializationException("Failed to read Blob", e);
            }
        } else {
            throw new SerializationException("Expected byte[] or Blob but got " + o.getClass().getName());
        }

        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInput = new ObjectInputStream(input)) {
            return objectInput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("Failed to deserialize object", e);
        }
    }

    private static byte[] toBinary(Object value) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(value);
            objectOutput.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize object", e);
        }
    }

    private static Object normalizeBinaryValue(Object value, Type declaredType) {
        if (value == null) {
            return null;
        }

        Class<?> rawType = getRawType(declaredType);
        if (isMapType(rawType)) {
            Type keyType = getTypeArgument(declaredType, 0);
            Type valueType = getTypeArgument(declaredType, 1);
            Map<?, ?> source = (Map<?, ?>) value;
            Map<Object, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                Object normalizedKey = normalizeAnyValue(entry.getKey(), keyType);
                Object normalizedValue = normalizeAnyValue(entry.getValue(), valueType);
                result.put(normalizedKey, normalizedValue);
            }
            return result;
        }

        if (isCustomObjectType(rawType)) {
            return normalizeCustomObject(value, rawType);
        }

        Collection<?> source = (Collection<?>) value;
        List<Object> result = new ArrayList<>(source.size());
        Type elementType = getTypeArgument(declaredType, 0);
        for (Object element : source) {
            result.add(normalizeAnyValue(element, elementType));
        }
        return result;
    }

    private static Object normalizeAnyValue(Object value, Type declaredType) {
        if (value == null) {
            return null;
        }

        Class<?> rawType = getRawType(declaredType);
        if (declaredType == Object.class) {
            rawType = value.getClass();
        }

        if (rawType.isEnum()) {
            return value.toString();
        }

        if (requiresBinarySerialization(rawType)) {
            return normalizeBinaryValue(value, declaredType == Object.class ? value.getClass() : declaredType);
        }

        ObjectConverter<Object> converter = findConverter((Class<Object>) rawType);
        if (converter != null) {
            return converter.convertBack(value);
        }

        return value;
    }

    private static Object denormalizeBinaryValue(Object value, Type declaredType) {
        if (value == null) {
            return null;
        }

        Class<?> rawType = getRawType(declaredType);
        if (isMapType(rawType)) {
            Type keyType = getTypeArgument(declaredType, 0);
            Type valueType = getTypeArgument(declaredType, 1);
            Map<?, ?> source = (Map<?, ?>) value;
            Map<Object, Object> result = instantiateMap(rawType);
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                Object key = denormalizeAnyValue(entry.getKey(), keyType);
                Object mapValue = denormalizeAnyValue(entry.getValue(), valueType);
                result.put(key, mapValue);
            }
            return result;
        }

        if (isCustomObjectType(rawType)) {
            return denormalizeCustomObject(value, rawType);
        }

        Collection<?> source = (Collection<?>) value;
        Collection<Object> result = instantiateCollection(rawType);
        Type elementType = getTypeArgument(declaredType, 0);
        for (Object element : source) {
            result.add(denormalizeAnyValue(element, elementType));
        }
        return result;
    }

    private static Object denormalizeAnyValue(Object value, Type declaredType) {
        if (value == null) {
            return null;
        }

        Class<?> rawType = getRawType(declaredType);
        Class<?> effectiveType = wrapPrimitiveType(rawType);
        if (declaredType == Object.class) {
            return value;
        }

        if (effectiveType.isEnum()) {
            return Enum.valueOf((Class<? extends Enum>) effectiveType, String.valueOf(value));
        }

        if (requiresBinarySerialization(effectiveType)) {
            return denormalizeBinaryValue(value, declaredType);
        }

        ObjectConverter<Object> converter = findConverter((Class<Object>) effectiveType);
        if (converter != null) {
            return converter.convert(value);
        }

        if (effectiveType.isInstance(value)) {
            return value;
        }

        if (value instanceof Number number) {
            if (effectiveType == Integer.class) {
                return number.intValue();
            }
            if (effectiveType == Long.class) {
                return number.longValue();
            }
            if (effectiveType == Double.class) {
                return number.doubleValue();
            }
            if (effectiveType == Float.class) {
                return number.floatValue();
            }
        }

        if (effectiveType == Boolean.class && value instanceof Number number) {
            return number.intValue() != 0;
        }

        if (effectiveType == Boolean.class && value instanceof String text) {
            return Boolean.parseBoolean(text);
        }

        if (effectiveType == String.class) {
            return String.valueOf(value);
        }

        throw new SerializationException("Cannot cast value of type " + value.getClass().getName() + " to " + rawType.getName());
    }

    private static Object normalizeCustomObject(Object value, Class<?> rawType) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Field field : getCachedFields(rawType)) {
            String columnName = getColumnName(field);
            if (columnName == null) {
                continue;
            }

            try {
                result.put(columnName, normalizeAnyValue(field.get(value), field.getGenericType()));
            } catch (IllegalAccessException e) {
                throw new SerializationException("Failed to access field " + field.getName(), e);
            }
        }
        return result;
    }

    private static Object denormalizeCustomObject(Object value, Class<?> rawType) {
        if (!(value instanceof Map<?, ?> source)) {
            throw new SerializationException("Expected Map but got " + value.getClass().getName());
        }

        try {
            Object instance = rawType.getDeclaredConstructor().newInstance();
            for (Field field : getCachedFields(rawType)) {
                String columnName = getColumnName(field);
                if (columnName == null || !source.containsKey(columnName)) {
                    continue;
                }

                Object fieldValue = denormalizeAnyValue(source.get(columnName), field.getGenericType());
                if (fieldValue != null) {
                    field.set(instance, fieldValue);
                }
            }
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new SerializationException("Failed to instantiate custom object " + rawType.getName(), e);
        }
    }

    private static boolean requiresBinarySerialization(Class<?> type) {
        return Collection.class.isAssignableFrom(type) || isMapType(type) || isCustomObjectType(type);
    }

    private static boolean isCustomObjectType(Class<?> type) {
        if (type.isPrimitive() || type.isEnum() || type.isArray()) {
            return false;
        }
        if (type == String.class || Number.class.isAssignableFrom(type) || type == Boolean.class || type == Character.class) {
            return false;
        }
        if (Date.class.isAssignableFrom(type) || java.util.Date.class.isAssignableFrom(type) || Time.class.isAssignableFrom(type)
                || Timestamp.class.isAssignableFrom(type) || BigDecimal.class.isAssignableFrom(type)) {
            return false;
        }
        if (Blob.class.isAssignableFrom(type) || Clob.class.isAssignableFrom(type) || NClob.class.isAssignableFrom(type)) {
            return false;
        }
        if (findConverter((Class<Object>) type) != null) {
            return false;
        }
        Package objectPackage = type.getPackage();
        return objectPackage == null || !objectPackage.getName().startsWith("java.");
    }

    private static boolean isMapType(Class<?> type) {
        return Map.class.isAssignableFrom(type);
    }

    private static Type getTypeArgument(Type type, int index) {
        if (type instanceof ParameterizedType parameterizedType) {
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (index < typeArguments.length) {
                return typeArguments[index];
            }
        }
        return Object.class;
    }

    private static Class<?> getRawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }

        if (type instanceof ParameterizedType parameterizedType) {
            return getRawType(parameterizedType.getRawType());
        }

        if (type instanceof WildcardType wildcardType) {
            Type[] upperBounds = wildcardType.getUpperBounds();
            return upperBounds.length == 0 ? Object.class : getRawType(upperBounds[0]);
        }

        if (type instanceof TypeVariable<?> typeVariable) {
            Type[] bounds = typeVariable.getBounds();
            return bounds.length == 0 ? Object.class : getRawType(bounds[0]);
        }

        return Object.class;
    }

    private static Class<?> wrapPrimitiveType(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == int.class) {
            return Integer.class;
        }
        if (type == long.class) {
            return Long.class;
        }
        if (type == boolean.class) {
            return Boolean.class;
        }
        if (type == double.class) {
            return Double.class;
        }
        if (type == float.class) {
            return Float.class;
        }
        if (type == byte.class) {
            return Byte.class;
        }
        if (type == short.class) {
            return Short.class;
        }
        if (type == char.class) {
            return Character.class;
        }
        return type;
    }

    private static Collection<Object> instantiateCollection(Class<?> rawType) {
        if (rawType.isInterface() || Modifier.isAbstract(rawType.getModifiers())) {
            if (Set.class.isAssignableFrom(rawType)) {
                return new LinkedHashSet<>();
            }
            return new ArrayList<>();
        }

        try {
            return (Collection<Object>) rawType.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new SerializationException("Failed to instantiate collection type " + rawType.getName(), e);
        }
    }

    private static Map<Object, Object> instantiateMap(Class<?> rawType) {
        if (rawType.isInterface() || Modifier.isAbstract(rawType.getModifiers())) {
            return new LinkedHashMap<>();
        }

        try {
            return (Map<Object, Object>) rawType.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new SerializationException("Failed to instantiate map type " + rawType.getName(), e);
        }
    }

    @Nullable
    private static <T> ObjectConverter<T> findConverter(Class<T> clazz) {
        ObjectConverter<?> exactConverter = CONVERTERS.get(clazz);
        if (exactConverter != null) {
            return (ObjectConverter<T>) exactConverter;
        }

        for (Map.Entry<Class<?>, ObjectConverter<?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return (ObjectConverter<T>) entry.getValue();
            }
        }

        return null;
    }
}
