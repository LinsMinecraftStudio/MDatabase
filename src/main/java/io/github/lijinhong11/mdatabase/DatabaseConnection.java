package io.github.lijinhong11.mdatabase;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;
import io.github.lijinhong11.mdatabase.sql.sentence.SQL;
import io.github.lijinhong11.mdatabase.sql.sentence.SelectSQL;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * The main API for database operations.
 * <p>
 * Provides connection management, CRUD via annotated classes, a fluent SQL builder,
 * batch operations, transactions, and automatic schema migration.
 * <p>
 * Create instances via {@link io.github.lijinhong11.mdatabase.impl.DatabaseConnections}:
 * <pre>{@code
 * DatabaseConnection conn = DatabaseConnections.sqlite("mydb.db", new DatabaseParameters());
 * }</pre>
 *
 * <h3>Usage example</h3>
 * <pre>{@code
 * conn.createTableByClass(User.class);
 * User user = new User(); user.name = "Alice";
 * conn.insertObject(User.class, user, false);
 * User found = conn.selectOne(User.class, Conditions.eq("id", 1));
 * }</pre>
 */
public interface DatabaseConnection {

    /**
     * Close the underlying connection pool and release all resources.
     *
     * @throws SQLException if a database error occurs during shutdown
     */
    void close() throws SQLException;

    /**
     * Execute an arbitrary SQL statement built via the fluent builder.
     *
     * @param sql the SQL to execute
     * @return {@code true} if the result is a ResultSet, {@code false} for an update count
     * @throws SQLException if a database error occurs
     */
    boolean execute(@NotNull SQL sql) throws SQLException;

    /**
     * Execute multiple SQL statements on a single connection.
     * Statements run in order; if one fails, subsequent statements are not executed.
     *
     * @param sqls the SQL statements to execute in sequence
     * @throws SQLException if any statement fails
     */
    void workspace(@NotNull SQL... sqls) throws SQLException;

    /**
     * Execute a raw SELECT query and return the live ResultSet.
     * The caller is responsible for managing the connection lifecycle of the returned ResultSet.
     * Prefer {@link #selectOne} or {@link #selectMulti} for most use cases.
     *
     * @param sql the SELECT SQL to execute
     * @return the result set
     * @throws SQLException if a database error occurs
     */
    @NotNull ResultSet query(@NotNull SelectSQL sql) throws SQLException;

    /**
     * Select a single object matching the condition.
     *
     * @param <T>       the entity type
     * @param clazz     the entity class (must be annotated with {@code @Table})
     * @param condition the WHERE condition
     * @return the matched object
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> T selectOne(@NotNull Class<T> clazz, @NotNull Condition condition) throws SQLException;

    /**
     * Select a single object from a specific table.
     *
     * @param <T>       the entity type
     * @param table     explicit table name (overrides the one from {@code @Table})
     * @param clazz     the entity class
     * @param condition the WHERE condition
     * @return the matched object
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> T selectOne(@NotNull String table, @NotNull Class<T> clazz, @NotNull Condition condition) throws SQLException;

    /**
     * Select all rows of the given type.
     *
     * @param <T>   the entity type
     * @param clazz the entity class
     * @return all rows as a list
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> List<T> selectMulti(@NotNull Class<T> clazz) throws SQLException;

    /**
     * Select all rows of the given type from a specific table.
     *
     * @param <T>   the entity type
     * @param table explicit table name
     * @param clazz the entity class
     * @return all rows as a list
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> List<T> selectMulti(@NotNull String table, @NotNull Class<T> clazz) throws SQLException;

    /**
     * Select rows matching the condition.
     *
     * @param <T>       the entity type
     * @param clazz     the entity class
     * @param condition the WHERE condition (nullable for all rows)
     * @return matched rows as a list
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> List<T> selectMulti(@NotNull Class<T> clazz, @Nullable Condition condition) throws SQLException;

    /**
     * Select rows matching the condition from a specific table.
     *
     * @param <T>       the entity type
     * @param table     explicit table name
     * @param clazz     the entity class
     * @param condition the WHERE condition (nullable for all rows)
     * @return matched rows as a list
     * @throws SQLException if a database error occurs
     */
    @NotNull <T> List<T> selectMulti(@NotNull String table, @NotNull Class<T> clazz, @Nullable Condition condition) throws SQLException;

    /**
     * Create a table from an entity class definition.
     * If the class is annotated with {@code @AutoMigrate}, missing columns will be added
     * and (if {@code drop = true}) orphaned columns will be dropped.
     *
     * @param clazz the entity class annotated with {@code @Table}
     * @throws SQLException if a database error occurs
     */
    void createTableByClass(@NotNull Class<?> clazz) throws SQLException;

    /**
     * Compare the database table with the entity class and apply changes:
     * <ol>
     *   <li>Rename columns where {@code @Column(renamedFrom = "...")} is specified</li>
     *   <li>Add columns that exist in the class but not in the table</li>
     *   <li>If {@code @AutoMigrate(drop = true)}, drop columns not present in the class</li>
     * </ol>
     * <p>
     * This method does NOT create the table if it doesn't exist — use
     * {@link #createTableByClass} for the initial creation.
     *
     * @param clazz the entity class annotated with {@code @Table}
     * @throws SQLException if a database error occurs
     */
    void autoMigrate(@NotNull Class<?> clazz) throws SQLException;

    /**
     * Insert or upsert a single object.
     * <p>
     * For upsert on SQLite/PostgreSQL, the class must have at least one {@code @PrimaryKey} field.
     * For MySQL/MariaDB, upsert uses {@code ON DUPLICATE KEY UPDATE}.
     *
     * @param <T>    the entity type
     * @param clazz  the entity class
     * @param object the object to insert/upsert
     * @param upsert {@code true} for upsert, {@code false} for plain insert
     * @throws SQLException if a database error occurs
     */
    <T> void insertObject(@NotNull Class<T> clazz, @NotNull T object, boolean upsert) throws SQLException;

    /**
     * Batch insert multiple objects using JDBC {@link java.sql.Statement#executeBatch()}.
     * All rows are inserted on a single connection. An empty list is a no-op.
     *
     * @param <T>     the entity type
     * @param clazz   the entity class
     * @param objects the objects to insert
     * @throws SQLException if a database error occurs
     */
    <T> void insertBatch(@NotNull Class<T> clazz, @NotNull List<T> objects) throws SQLException;

    /**
     * Batch update multiple objects identified by their {@code @PrimaryKey} fields.
     * Non-primary-key {@code @Column} fields become the SET clause; primary key fields
     * become the WHERE clause. An empty list is a no-op.
     *
     * @param <T>     the entity type
     * @param clazz   the entity class (must have at least one {@code @PrimaryKey})
     * @param objects the objects to update
     * @throws SQLException if a database error occurs
     */
    <T> void updateBatch(@NotNull Class<T> clazz, @NotNull List<T> objects) throws SQLException;

    /**
     * Update a single object. Non-primary-key fields are updated; primary key fields
     * are excluded from the SET clause.
     *
     * @param <T>       the entity type
     * @param clazz     the entity class
     * @param object    the object with updated values
     * @param condition the WHERE condition identifying the row
     * @throws SQLException if a database error occurs
     */
    <T> void updateObject(@NotNull Class<T> clazz, @NotNull T object, @NotNull Condition condition) throws SQLException;

    /**
     * Delete rows matching the condition.
     *
     * @param clazz     the entity class
     * @param condition the WHERE condition
     * @throws SQLException if a database error occurs
     */
    void deleteObject(@NotNull Class<?> clazz, @NotNull Condition condition) throws SQLException;

    /**
     * Check if the database connection is alive.
     *
     * @return {@code true} if the connection is valid
     * @throws SQLException if a database error occurs
     */
    boolean ping() throws SQLException;

    /**
     * Return the database type this connection is backed by.
     *
     * @return the database type
     * @see DatabaseType
     */
    @NotNull DatabaseType getType();

    /**
     * Enable or disable debug logging of generated SQL statements.
     *
     * @param debug {@code true} to log SQL, {@code false} to suppress
     */
    void setDebug(boolean debug);

    /**
     * Begin a transaction. Returns an auto-closeable {@link Transaction} object
     * that rolls back if not explicitly committed.
     * <pre>{@code
     * try (Transaction tx = conn.beginTransaction()) {
     *     conn.insertObject(User.class, user, false);
     *     tx.commit();
     * }
     * }</pre>
     *
     * @return a new transaction handle
     * @throws SQLException if a database error occurs or a transaction is already active
     */
    Transaction beginTransaction() throws SQLException;

    /**
     * Commit the current transaction. Must be called within an active transaction.
     *
     * @throws SQLException if no transaction is active or commit fails
     */
    void commit() throws SQLException;

    /**
     * Rollback the current transaction. Must be called within an active transaction.
     *
     * @throws SQLException if no transaction is active or rollback fails
     */
    void rollback() throws SQLException;
}
