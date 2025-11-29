package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;

public class TruncateSQL extends SQL {
    private String tableName;
    private boolean restartIdentity;
    private boolean cascade;

    TruncateSQL() {
    }

    public TruncateSQL table(String tableName) {
        validateIdentifier(tableName);
        this.tableName = tableName;
        return this;
    }

    public TruncateSQL restartIdentity() {
        this.restartIdentity = true;
        return this;
    }

    public TruncateSQL cascade() {
        this.cascade = true;
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        if (tableName == null) {
            throw new IllegalStateException("Table name must be specified");
        }

        sqlBuilder.setLength(0);

        if (type == DatabaseType.POSTGRESQL) {
            sqlBuilder.append("TRUNCATE TABLE ").append(tableName);
            if (restartIdentity) {
                sqlBuilder.append(" RESTART IDENTITY");
            }
            if (cascade) {
                sqlBuilder.append(" CASCADE");
            }
        } else {
            sqlBuilder.append("TRUNCATE TABLE ").append(tableName);
        }

        return sqlBuilder.toString();
    }

    @Override
    public java.sql.PreparedStatement build(java.sql.Connection connection, DatabaseType type)
            throws java.sql.SQLException {
        return connection.prepareStatement(getSql(type));
    }
}
