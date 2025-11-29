package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.exceptions.IllegalIdentifierException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class SQL {
    protected final StringBuilder sqlBuilder = new StringBuilder();
    protected final List<Object> parameters = new ArrayList<>();

    /**
     * Create a SELECT SQL builder
     *
     * @return a new SelectSQL instance
     */
    public static SelectSQL select() {
        return new SelectSQL();
    }

    /**
     * Create an INSERT SQL builder
     *
     * @return a new InsertSQL instance
     */
    public static InsertSQL insert() {
        return new InsertSQL(false);
    }

    /**
     * Create an UPSERT (INSERT ... ON DUPLICATE KEY UPDATE / ON CONFLICT) SQL
     * builder
     *
     * @return a new InsertSQL instance configured for upsert operations
     */
    public static InsertSQL upsert() {
        return new InsertSQL(true);
    }

    /**
     * Create an UPDATE SQL builder
     *
     * @return a new UpdateSQL instance
     */
    public static UpdateSQL update() {
        return new UpdateSQL();
    }

    /**
     * Create a DELETE SQL builder
     *
     * @return a new DeleteSQL instance
     */
    public static DeleteSQL delete() {
        return new DeleteSQL();
    }

    /**
     * Create a DROP SQL builder for dropping tables, indexes, or views
     *
     * @return a new DropSQL instance
     */
    public static DropSQL drop() {
        return new DropSQL();
    }

    /**
     * Create a CREATE TABLE SQL builder
     *
     * @return a new CreateTableSQL instance
     */
    public static CreateTableSQL createTable() {
        return new CreateTableSQL();
    }

    /**
     * Create an ALTER TABLE SQL builder
     *
     * @return a new AlterTableSQL instance
     */
    public static AlterTableSQL alterTable() {
        return new AlterTableSQL();
    }

    /**
     * Create a CREATE INDEX SQL builder
     *
     * @return a new CreateIndexSQL instance
     */
    public static CreateIndexSQL createIndex() {
        return new CreateIndexSQL();
    }

    /**
     * Create a CREATE VIEW SQL builder
     *
     * @return a new CreateViewSQL instance
     */
    public static CreateViewSQL createView() {
        return new CreateViewSQL();
    }

    /**
     * Create a TRUNCATE TABLE SQL builder
     *
     * @return a new TruncateSQL instance
     */
    public static TruncateSQL truncate() {
        return new TruncateSQL();
    }

    /**
     * Build a PreparedStatement from this SQL builder
     *
     * @param connection the database connection
     * @param type       the database type
     * @return a PreparedStatement with parameters set
     * @throws SQLException if a database access error occurs
     */
    public PreparedStatement build(Connection connection, DatabaseType type) throws SQLException {
        parameters.clear();
        String sql = getSql(type);
        PreparedStatement stmt = connection.prepareStatement(sql);

        int expectedParams = countParametersInSql(sql);
        if (parameters.size() != expectedParams) {
            throw new SQLException("Parameter count mismatch. Expected " + expectedParams +
                    " but got " + parameters.size());
        }

        for (int i = 0; i < parameters.size(); i++) {
            stmt.setObject(i + 1, parameters.get(i));
        }

        return stmt;
    }

    private int countParametersInSql(String sql) {
        int count = 0;
        int index = -1;
        while ((index = sql.indexOf('?', index + 1)) != -1) {
            count++;
        }
        return count;
    }

    abstract String getSql(DatabaseType type);

    void validateIdentifier(String identifier) {
        if (!identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalIdentifierException("Invalid SQL identifier: " + identifier);
        }
    }
}