package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;

public class CreateViewSQL extends SQL {
    private String viewName;
    private String selectQuery;
    private SelectSQL selectSQL;
    private boolean orReplace;
    private boolean ifNotExists;

    CreateViewSQL() {
    }

    public CreateViewSQL view(String viewName) {
        validateIdentifier(viewName);
        this.viewName = viewName;
        return this;
    }

    public CreateViewSQL as(String selectQuery) {
        if (selectQuery == null || selectQuery.trim().isEmpty()) {
            throw new IllegalArgumentException("Select query cannot be null or empty");
        }
        this.selectQuery = selectQuery.trim();
        this.selectSQL = null;
        return this;
    }

    public CreateViewSQL as(SelectSQL selectSQL) {
        if (selectSQL == null) {
            throw new IllegalArgumentException("SelectSQL cannot be null");
        }
        this.selectSQL = selectSQL;
        this.selectQuery = null;
        return this;
    }

    public CreateViewSQL orReplace() {
        this.orReplace = true;
        return this;
    }

    public CreateViewSQL ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        if (viewName == null) {
            throw new IllegalStateException("View name must be specified");
        }

        String query;
        if (selectSQL != null) {
            query = selectSQL.getSql(type);
        } else if (selectQuery != null && !selectQuery.isEmpty()) {
            query = selectQuery;
        } else {
            throw new IllegalStateException("Select query must be specified");
        }

        sqlBuilder.setLength(0);

        sqlBuilder.append("CREATE ");

        if (orReplace) {
            if (type == DatabaseType.SQLITE) {
                throw new UnsupportedOperationException(
                        "SQLite does not support OR REPLACE. Use DROP VIEW then CREATE VIEW instead.");
            }
            sqlBuilder.append("OR REPLACE ");
        }

        sqlBuilder.append("VIEW ");

        if (ifNotExists) {
            if (type == DatabaseType.POSTGRESQL) {
                sqlBuilder.append("IF NOT EXISTS ");
            }
        }

        sqlBuilder.append(viewName).append(" AS ").append(query);

        return sqlBuilder.toString();
    }

    @Override
    public java.sql.PreparedStatement build(java.sql.Connection connection, DatabaseType type)
            throws java.sql.SQLException {
        return connection.prepareStatement(getSql(type));
    }
}
