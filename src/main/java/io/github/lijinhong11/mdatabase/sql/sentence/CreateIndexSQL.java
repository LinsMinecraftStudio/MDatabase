package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexSQL extends SQL {
    private final List<String> columns = new ArrayList<>();
    private String indexName;
    private String tableName;
    private boolean unique;
    private boolean ifNotExists;

    CreateIndexSQL() {
    }

    public CreateIndexSQL index(String indexName) {
        validateIdentifier(indexName);
        this.indexName = indexName;
        return this;
    }

    public CreateIndexSQL on(String tableName) {
        validateIdentifier(tableName);
        this.tableName = tableName;
        return this;
    }

    public CreateIndexSQL column(String columnName) {
        validateIdentifier(columnName);
        columns.add(columnName);
        return this;
    }

    public CreateIndexSQL columns(String... columnNames) {
        for (String columnName : columnNames) {
            validateIdentifier(columnName);
            columns.add(columnName);
        }
        return this;
    }

    public CreateIndexSQL unique() {
        this.unique = true;
        return this;
    }

    public CreateIndexSQL ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        if (indexName == null) {
            throw new IllegalStateException("Index name must be specified");
        }
        if (tableName == null) {
            throw new IllegalStateException("Table name must be specified");
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("At least one column must be specified");
        }

        sqlBuilder.setLength(0);

        if (unique) {
            sqlBuilder.append("CREATE UNIQUE INDEX ");
        } else {
            sqlBuilder.append("CREATE INDEX ");
        }

        if (ifNotExists) {
            if (type == DatabaseType.SQLITE || type == DatabaseType.POSTGRESQL) {
                sqlBuilder.append("IF NOT EXISTS ");
            }
        }

        sqlBuilder.append(indexName).append(" ON ").append(tableName).append(" (")
                .append(String.join(", ", columns)).append(")");

        return sqlBuilder.toString();
    }

    @Override
    public java.sql.PreparedStatement build(java.sql.Connection connection, DatabaseType type)
            throws java.sql.SQLException {
        return connection.prepareStatement(getSql(type));
    }
}
