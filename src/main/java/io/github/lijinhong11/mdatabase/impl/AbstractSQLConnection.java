package io.github.lijinhong11.mdatabase.impl;

import io.github.lijinhong11.mdatabase.DatabaseConnection;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.serialization.ObjectSerializer;
import io.github.lijinhong11.mdatabase.serialization.annotations.AutoIncrement;
import io.github.lijinhong11.mdatabase.serialization.annotations.Column;
import io.github.lijinhong11.mdatabase.serialization.annotations.PrimaryKey;
import io.github.lijinhong11.mdatabase.serialization.annotations.Table;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;
import io.github.lijinhong11.mdatabase.sql.sentence.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

abstract class AbstractSQLConnection implements DatabaseConnection {
    private static final Logger LOGGER = Logger.getLogger("MDatabase");

    private boolean debug = false;

    abstract Connection getConnection() throws SQLException;

    @Override
    public abstract @NotNull DatabaseType getType();

    @Override
    public abstract void close();

    @Override
    public boolean execute(SQL sql) throws SQLException {
        try (Connection connection = getConnection()) {
            return sql.build(connection, getType()).execute();
        }
    }

    @Override
    public ResultSet query(SelectSQL sql) throws SQLException {
        try (Connection connection = getConnection()) {
            return sql.build(connection, getType()).executeQuery();
        }
    }

    @Override
    public <T> @NotNull T selectOne(Class<T> clazz, @NotNull Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        SelectSQL sql = SQL.select()
                .allColumns()
                .from(table.name())
                .where(condition)
                .limit(1);

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        ResultSet query = query(sql);
        return ObjectSerializer.serializeOne(clazz, query);
    }

    @Override
    public <T> @NotNull List<T> selectMulti(@NotNull Class<T> clazz) throws SQLException {
        return selectMulti(clazz, null);
    }

    @Override
    public <T> @NotNull List<T> selectMulti(@NotNull Class<T> clazz, @Nullable Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        SelectSQL sql = SQL.select()
                .allColumns()
                .from(table.name());

        if (condition != null) {
            sql.where(condition);
        }

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        ResultSet set = query(sql);
        return ObjectSerializer.serializeMulti(clazz, set);
    }

    @Override
    public boolean ping() throws SQLException {
        try (Connection connection = getConnection()) {
            return connection.isValid(1);
        }
    }

    @Override
    public void createTableByClass(@NotNull Class<?> clazz) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        List<Field> field = ObjectSerializer.getAllFields(clazz);

        CreateTableSQL sql = SQL.createTable().table(table.name()).ifNotExists();

        for (Field f : field) {
            if (f.isAnnotationPresent(Column.class)) {
                Column column = f.getAnnotation(Column.class);
                if (column != null) {
                    Class<?> type = f.getType();

                    String sqlType = ObjectSerializer.getSqlType(type);
                    String columnName = ObjectSerializer.getColumnName(f);

                    sql.column(columnName, sqlType);

                    if (f.isAnnotationPresent(AutoIncrement.class)) {
                        sql.autoIncrement(columnName);
                    }

                    if (f.isAnnotationPresent(PrimaryKey.class)) {
                        sql.primaryKey(columnName);
                    }

                    if (!column.nullable()) {
                        sql.notNull(columnName);
                    }

                    if (!Objects.isNull(column.defaultValue()) && !column.defaultValue().isBlank()) {
                        sql.defaultValue(columnName, column.defaultValue());
                    }
                }
            }
        }

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        sql.build(getConnection(), getType()).execute();
    }

    @Override
    public <T> void insertObject(@NotNull Class<T> clazz, @NotNull T object, boolean upsert) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        List<Field> fields = ObjectSerializer.getAllFields(clazz);

        InsertSQL sql = upsert ? SQL.upsert().into(table.name()) : SQL.insert().into(table.name());

        List<String> conflictKeys = new ArrayList<>();

        for (Field f : fields) {
            if (f.isAnnotationPresent(Column.class)) {
                String columnName = ObjectSerializer.getColumnName(f);
                try {
                    sql.value(columnName, ObjectSerializer.convertBack(f.get(object)));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                if (upsert && (getType() == DatabaseType.SQLITE || getType() == DatabaseType.POSTGRESQL)) {
                    boolean isPrimary = f.isAnnotationPresent(PrimaryKey.class);
                    if (isPrimary) {
                        conflictKeys.add(columnName);
                    }
                }
            }
        }

        if (upsert && (getType() == DatabaseType.SQLITE || getType() == DatabaseType.POSTGRESQL)) {
            if (conflictKeys.isEmpty()) {
                throw new IllegalStateException("Upsert requires at least one @PrimaryKey or @Column(primaryKey=true) in " + clazz.getName());
            }

            sql = sql.conflictKeys(conflictKeys.toArray(new String[0]));
        }

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        sql.build(getConnection(), getType()).execute();
    }

    @Override
    public <T> void updateObject(@NotNull Class<T> clazz, @NotNull T object, @NotNull Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        List<Field> field = ObjectSerializer.getAllFields(clazz);

        UpdateSQL sql = SQL.update().table(table.name());
        for (Field f : field) {
            if (f.isAnnotationPresent(Column.class)) {
                String columnName = ObjectSerializer.getColumnName(f);
                if (!f.isAnnotationPresent(PrimaryKey.class)) {
                    try {
                        sql = sql.set(columnName, ObjectSerializer.convertBack(f.get(object)));
                    } catch (IllegalAccessException e) {
                        //never happen
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        sql.where(condition);

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        sql.build(getConnection(), getType()).execute();
    }

    @Override
    public void deleteObject(@NotNull Class<?> clazz, @NotNull Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        DeleteSQL sql = SQL.delete().from(table.name()).where(condition);

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        sql.build(getConnection(), getType()).execute();
    }

    @Override
    public void setDebug(boolean debug) {
        this.debug = debug;
    }
}