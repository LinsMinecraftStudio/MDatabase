package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;

public class DropSQL extends SQL {
    private String name;
    private boolean ifExists;
    private boolean isTable;
    private boolean isIndex;
    private boolean isView;
    private String tableName;

    DropSQL() {
    }

    private DropSQL name(String name) {
        validateIdentifier(name);
        this.name = name;
        return this;
    }

    public DropSQL ifExists() {
        this.ifExists = true;
        return this;
    }

    public DropSQL table(String tableName) {
        this.isTable = true;
        this.isIndex = false;
        this.isView = false;
        return name(tableName);
    }

    public DropSQL index(String indexName) {
        this.isTable = false;
        this.isIndex = true;
        this.isView = false;
        return name(indexName);
    }

    public DropSQL view(String viewName) {
        this.isTable = false;
        this.isIndex = false;
        this.isView = true;
        return name(viewName);
    }

    public DropSQL fromTable(String tableName) {
        validateIdentifier(tableName);
        this.tableName = tableName;
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        sqlBuilder.setLength(0);

        if (isTable) {
            buildDropTable();
        } else if (isIndex) {
            buildDropIndex();
        } else if (isView) {
            buildDropView();
        } else {
            throw new IllegalStateException("Neither table, index nor view specified");
        }

        return sqlBuilder.toString();
    }

    private void buildDropTable() {
        sqlBuilder.append("DROP TABLE ");
        appendIfExists();
        sqlBuilder.append(name);
    }

    private void buildDropIndex() {
        sqlBuilder.append("DROP INDEX ");
        appendIfExists();
        sqlBuilder.append(name);

        if (tableName != null) {
            sqlBuilder.append(" ON ").append(tableName);
        }
    }

    private void buildDropView() {
        sqlBuilder.append("DROP VIEW ");
        appendIfExists();
        sqlBuilder.append(name);
    }

    private void appendIfExists() {
        if (ifExists) {
            sqlBuilder.append("IF EXISTS ");
        }
    }
}