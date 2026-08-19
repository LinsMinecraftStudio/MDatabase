package io.github.lijinhong11.mdatabase.impl;

import io.github.lijinhong11.mdatabase.DatabaseConnection;
import io.github.lijinhong11.mdatabase.Transaction;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.exceptions.WrongTypeException;
import io.github.lijinhong11.mdatabase.serialization.ObjectSerializer;
import io.github.lijinhong11.mdatabase.serialization.annotations.AutoIncrement;
import io.github.lijinhong11.mdatabase.serialization.annotations.AutoMigrate;
import io.github.lijinhong11.mdatabase.serialization.annotations.Column;
import io.github.lijinhong11.mdatabase.serialization.annotations.PrimaryKey;
import io.github.lijinhong11.mdatabase.serialization.annotations.Table;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;
import io.github.lijinhong11.mdatabase.sql.sentence.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

abstract class AbstractDatabaseConnection implements DatabaseConnection {
    private static final Logger LOGGER = Logger.getLogger("MDatabase");

    private boolean debug = false;
    private final ThreadLocal<Connection> transactionConnection = new ThreadLocal<>();

    abstract Connection createRawConnection() throws SQLException;

    Connection getConnection() throws SQLException {
        Connection txConn = transactionConnection.get();
        if (txConn != null) {
            return txConn;
        }
        return createRawConnection();
    }

    boolean isInTransaction() {
        return transactionConnection.get() != null;
    }

    @Override
    public abstract @NotNull DatabaseType getType();

    @Override
    public abstract void close();

    @Override
    public boolean execute(@NotNull SQL sql) throws SQLException {
        Connection connection = getConnection();
        try {
            return sql.build(connection, getType()).execute();
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
    }

    @Override
    public void workspace(@NotNull SQL @NotNull ... sqls) throws SQLException {
        Connection connection = getConnection();
        try {
            for (SQL sql : sqls) {
                sql.build(connection, getType()).execute();
            }
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
    }

    @Override
    public @NotNull ResultSet query(@NotNull SelectSQL sql) throws SQLException {
        Connection connection = getConnection();
        try {
            return sql.build(connection, getType()).executeQuery();
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
    }

    private <T> T selectOneInternal(String tableName, Class<T> clazz, Condition condition) throws SQLException {
        SelectSQL sql = SQL.select()
                .allColumns()
                .from(tableName)
                .where(condition)
                .limit(1);

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        Connection connection = getConnection();
        try {
            ResultSet rs = sql.build(connection, getType()).executeQuery();
            try {
                return ObjectSerializer.serializeOne(clazz, rs);
            } finally {
                rs.close();
            }
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
    }

    @Override
    public <T> @NotNull T selectOne(@NotNull Class<T> clazz, @NotNull Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        return selectOneInternal(table.name(), clazz, condition);
    }

    @Override
    public <T> @NotNull T selectOne(@NotNull String table, @NotNull Class<T> clazz, @NotNull Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        return selectOneInternal(table, clazz, condition);
    }


    @Override
    public <T> @NotNull List<T> selectMulti(@NotNull Class<T> clazz) throws SQLException {
        return selectMulti(clazz, null);
    }

    @Override
    public <T> @NotNull List<T> selectMulti(@NotNull String table, @NotNull Class<T> clazz) throws SQLException {
        return selectMulti(table, clazz, null);
    }

    private <T> List<T> selectMultiInternal(String tableName, Class<T> clazz, Condition condition) throws SQLException {
        SelectSQL sql = SQL.select()
                .allColumns()
                .from(tableName);

        if (condition != null) {
            sql.where(condition);
        }

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        Connection connection = getConnection();
        try {
            ResultSet rs = sql.build(connection, getType()).executeQuery();
            try {
                return ObjectSerializer.serializeMulti(clazz, rs);
            } finally {
                rs.close();
            }
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
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

        return selectMultiInternal(table.name(), clazz, condition);
    }

    @Override
    public <T> @NotNull List<T> selectMulti(@NotNull String table, @NotNull Class<T> clazz, @Nullable Condition condition) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        return selectMultiInternal(table, clazz, condition);
    }

    @Override
    public boolean ping() throws SQLException {
        Connection connection = getConnection();
        try {
            return connection.isValid(1);
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }
    }

    private Set<String> getExistingColumns(String tableName) throws SQLException {
        String sql = switch (getType()) {
            case SQLITE -> "SELECT name FROM pragma_table_info(?)";
            case MYSQL, MARIADB ->
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
            case POSTGRESQL ->
                    "SELECT column_name FROM information_schema.columns WHERE table_name = ?";
        };

        Set<String> columns = new HashSet<>();
        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, tableName);
            ResultSet rs = ps.executeQuery();
            try {
                while (rs.next()) {
                    columns.add(rs.getString(1));
                }
            } finally {
                rs.close();
            }
            ps.close();
        } finally {
            if (!isInTransaction()) {
                conn.close();
            }
        }
        return columns;
    }

    @Override
    public void autoMigrate(@NotNull Class<?> clazz) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        String tableName = table.name();
        List<Field> fields = ObjectSerializer.getAllFields(clazz);
        Set<String> existingColumns = getExistingColumns(tableName);
        Set<String> classColumnNames = new HashSet<>();

        for (Field f : fields) {
            if (f.isAnnotationPresent(Column.class)) {
                classColumnNames.add(ObjectSerializer.getColumnName(f));
            }
        }

        boolean doDrop = clazz.isAnnotationPresent(AutoMigrate.class)
                && clazz.getAnnotation(AutoMigrate.class).drop();

        for (Field f : fields) {
            if (!f.isAnnotationPresent(Column.class)) {
                continue;
            }
            Column column = f.getAnnotation(Column.class);
            if (column == null) {
                continue;
            }

            String columnName = ObjectSerializer.getColumnName(f);
            String sqlType = ObjectSerializer.getSqlType(f.getGenericType());
            boolean notNull = !column.nullable();
            String defaultVal = (Objects.isNull(column.defaultValue()) || column.defaultValue().isBlank())
                    ? null : column.defaultValue();

            if (!column.renamedFrom().isBlank() && existingColumns.contains(column.renamedFrom())
                    && !existingColumns.contains(columnName)) {
                AlterTableSQL rename = SQL.alterTable().table(tableName)
                        .renameColumn(column.renamedFrom(), columnName);
                if (debug) {
                    LOGGER.info("Auto-migrating rename: " + rename.getSql(getType()));
                }
                Connection renameConn = getConnection();
                try {
                    rename.build(renameConn, getType()).execute();
                } finally {
                    if (!isInTransaction()) {
                        renameConn.close();
                    }
                }
                existingColumns.remove(column.renamedFrom());
                existingColumns.add(columnName);
                continue;
            }

            if (existingColumns.contains(columnName)) {
                continue;
            }

            AlterTableSQL alter = SQL.alterTable().table(tableName)
                    .addColumn(columnName, sqlType, notNull, defaultVal);

            if (debug) {
                LOGGER.info("Auto-migrating add: " + alter.getSql(getType()));
            }

            Connection conn = getConnection();
            try {
                alter.build(conn, getType()).execute();
            } finally {
                if (!isInTransaction()) {
                    conn.close();
                }
            }
            existingColumns.add(columnName);
        }

        if (doDrop) {
            for (String dbColumn : existingColumns) {
                if (!classColumnNames.contains(dbColumn)) {
                    AlterTableSQL drop = SQL.alterTable().table(tableName).dropColumn(dbColumn);
                    if (debug) {
                        LOGGER.info("Auto-migrating drop: " + drop.getSql(getType()));
                    }
                    Connection dropConn = getConnection();
                    try {
                        drop.build(dropConn, getType()).execute();
                    } finally {
                        if (!isInTransaction()) {
                            dropConn.close();
                        }
                    }
                }
            }
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

        if (!table.engine().isBlank()) {
            sql.options("ENGINE=" + table.engine());
        }

        for (Field f : field) {
            if (f.isAnnotationPresent(Column.class)) {
                Column column = f.getAnnotation(Column.class);
                if (column != null) {
                    Class<?> type = f.getType();

                    String sqlType = ObjectSerializer.getSqlType(f.getGenericType());
                    String columnName = ObjectSerializer.getColumnName(f);

                    sql.column(columnName, sqlType);

                    if (f.isAnnotationPresent(AutoIncrement.class)) {
                        if (!type.isAssignableFrom(Number.class)) {
                            throw new WrongTypeException("Class " + type + "isn't incrementable");
                        }
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

        Connection connection = getConnection();
        try {
            sql.build(connection, getType()).execute();
        } finally {
            if (!isInTransaction()) {
                connection.close();
            }
        }

        if (clazz.isAnnotationPresent(AutoMigrate.class)) {
            autoMigrate(clazz);
        }
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
                    sql.value(columnName, ObjectSerializer.convertBack(f.get(object), f.getGenericType()));
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

            sql.conflictKeys(conflictKeys.toArray(new String[0]));
        }

        if (debug) {
            LOGGER.info("Invoking SQL: " + sql.getSql(getType()));
        }

        Connection insertConn = getConnection();
        try {
            sql.build(insertConn, getType()).execute();
        } finally {
            if (!isInTransaction()) {
                insertConn.close();
            }
        }
    }

    @Override
    public <T> void insertBatch(@NotNull Class<T> clazz, @NotNull List<T> objects) throws SQLException {
        if (objects.isEmpty()) {
            return;
        }

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        List<Field> fields = ObjectSerializer.getAllFields(clazz);
        List<Field> columnFields = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Column.class)) {
                columnFields.add(f);
                columnNames.add(ObjectSerializer.getColumnName(f));
            }
        }

        String sql = "INSERT INTO " + table.name() + " (" +
                String.join(", ", columnNames) + ") VALUES (" +
                String.join(", ", columnNames.stream().map(c -> "?").toArray(String[]::new)) + ")";

        if (debug) {
            LOGGER.info("Batch inserting " + objects.size() + " rows: " + sql);
        }

        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            try {
                for (T obj : objects) {
                    int idx = 1;
                    for (Field f : columnFields) {
                        Object value;
                        try {
                            value = ObjectSerializer.convertBack(f.get(obj), f.getGenericType());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                        ps.setObject(idx++, value);
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            } finally {
                ps.close();
            }
        } finally {
            if (!isInTransaction()) {
                conn.close();
            }
        }
    }

    @Override
    public <T> void updateBatch(@NotNull Class<T> clazz, @NotNull List<T> objects) throws SQLException {
        if (objects.isEmpty()) {
            return;
        }

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("the class must be annotated with @Table");
        }

        Table table = clazz.getAnnotation(Table.class);
        if (Objects.isNull(table.name()) || table.name().isBlank()) {
            throw new IllegalArgumentException("the table name cannot be empty");
        }

        List<Field> fields = ObjectSerializer.getAllFields(clazz);
        List<Field> valueFields = new ArrayList<>();
        List<Field> keyFields = new ArrayList<>();
        List<String> valueNames = new ArrayList<>();
        List<String> keyNames = new ArrayList<>();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Column.class)) {
                if (f.isAnnotationPresent(PrimaryKey.class)) {
                    keyFields.add(f);
                    keyNames.add(ObjectSerializer.getColumnName(f));
                } else {
                    valueFields.add(f);
                    valueNames.add(ObjectSerializer.getColumnName(f));
                }
            }
        }

        if (keyFields.isEmpty()) {
            throw new IllegalStateException("No @PrimaryKey field found in " + clazz.getName());
        }

        String setClause = String.join(", ", valueNames.stream().map(c -> c + " = ?").toArray(String[]::new));
        String whereClause = String.join(" AND ", keyNames.stream().map(c -> c + " = ?").toArray(String[]::new));
        String sql = "UPDATE " + table.name() + " SET " + setClause + " WHERE " + whereClause;

        if (debug) {
            LOGGER.info("Batch updating " + objects.size() + " rows: " + sql);
        }

        Connection conn = getConnection();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            try {
                for (T obj : objects) {
                    int idx = 1;
                    for (Field f : valueFields) {
                        Object value;
                        try {
                            value = ObjectSerializer.convertBack(f.get(obj), f.getGenericType());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                        ps.setObject(idx++, value);
                    }
                    for (Field f : keyFields) {
                        Object value;
                        try {
                            value = ObjectSerializer.convertBack(f.get(obj), f.getGenericType());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                        ps.setObject(idx++, value);
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            } finally {
                ps.close();
            }
        } finally {
            if (!isInTransaction()) {
                conn.close();
            }
        }
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
                        sql.set(columnName, ObjectSerializer.convertBack(f.get(object), f.getGenericType()));
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

        Connection updateConn = getConnection();
        try {
            sql.build(updateConn, getType()).execute();
        } finally {
            if (!isInTransaction()) {
                updateConn.close();
            }
        }
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

        Connection deleteConn = getConnection();
        try {
            sql.build(deleteConn, getType()).execute();
        } finally {
            if (!isInTransaction()) {
                deleteConn.close();
            }
        }
    }

    @Override
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @Override
    public Transaction beginTransaction() throws SQLException {
        if (isInTransaction()) {
            throw new SQLException("Transaction already active");
        }
        Connection conn = createRawConnection();
        conn.setAutoCommit(false);
        transactionConnection.set(conn);
        return new Transaction(this);
    }

    @Override
    public void commit() throws SQLException {
        Connection conn = transactionConnection.get();
        if (conn == null) {
            throw new SQLException("No active transaction");
        }
        try {
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
            conn.close();
            transactionConnection.remove();
        }
    }

    @Override
    public void rollback() throws SQLException {
        Connection conn = transactionConnection.get();
        if (conn == null) {
            throw new SQLException("No active transaction");
        }
        try {
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
            conn.close();
            transactionConnection.remove();
        }
    }
}
