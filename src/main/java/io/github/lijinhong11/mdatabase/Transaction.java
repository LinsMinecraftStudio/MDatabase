package io.github.lijinhong11.mdatabase;

import java.sql.SQLException;

/**
 * A database transaction handle obtained via {@link DatabaseConnection#beginTransaction()}.
 * <p>
 * Implements {@link AutoCloseable} for use with try-with-resources:
 * <pre>{@code
 * try (Transaction tx = conn.beginTransaction()) {
 *     conn.insertObject(User.class, user, false);
 *     conn.insertObject(Order.class, order, false);
 *     tx.commit();
 * }
 * }</pre>
 * If {@link #close()} is called without an explicit {@link #commit()}, the transaction
 * is automatically rolled back.
 */
public final class Transaction implements AutoCloseable {
    private final DatabaseConnection connection;
    private boolean completed;

    /**
     * Construct a transaction for the given connection.
     * Prefer using {@link DatabaseConnection#beginTransaction()} instead.
     *
     * @param connection the database connection
     */
    public Transaction(DatabaseConnection connection) {
        this.connection = connection;
        this.completed = false;
    }

    /**
     * Commit this transaction. Idempotent — subsequent calls are ignored.
     *
     * @throws SQLException if the commit fails
     */
    public void commit() throws SQLException {
        if (!completed) {
            connection.commit();
            completed = true;
        }
    }

    /**
     * Rollback this transaction. Idempotent — subsequent calls are ignored.
     *
     * @throws SQLException if the rollback fails
     */
    public void rollback() throws SQLException {
        if (!completed) {
            connection.rollback();
            completed = true;
        }
    }

    /**
     * Close this transaction, rolling back if not yet committed.
     * Called automatically when used in a try-with-resources block.
     *
     * @throws SQLException if the rollback fails
     */
    @Override
    public void close() throws SQLException {
        if (!completed) {
            rollback();
        }
    }
}
