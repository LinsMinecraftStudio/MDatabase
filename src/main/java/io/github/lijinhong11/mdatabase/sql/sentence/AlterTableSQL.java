package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;

import java.util.ArrayList;
import java.util.List;

public class AlterTableSQL extends SQL {
    private final List<AlterOperation> operations = new ArrayList<>();
    private String tableName;

    AlterTableSQL() {
    }

    public AlterTableSQL table(String tableName) {
        validateIdentifier(tableName);
        this.tableName = tableName;
        return this;
    }

    public AlterTableSQL addColumn(String columnName, String dataType) {
        return addColumn(columnName, dataType, false, null);
    }

    public AlterTableSQL addColumn(String columnName, String dataType, boolean notNull, String defaultValue) {
        return addColumn(columnName, dataType, notNull, defaultValue, null);
    }

    public AlterTableSQL addColumn(String columnName, String dataType, boolean notNull, String defaultValue,
                                   String position) {
        validateIdentifier(columnName);
        operations.add(
                new AlterOperation(OperationType.ADD_COLUMN, columnName, dataType, notNull, defaultValue, position));
        return this;
    }

    public AlterTableSQL dropColumn(String columnName) {
        validateIdentifier(columnName);
        operations.add(new AlterOperation(OperationType.DROP_COLUMN, columnName, null, false, null, null));
        return this;
    }

    public AlterTableSQL modifyColumn(String columnName, String newDataType) {
        return modifyColumn(columnName, newDataType, false, null);
    }

    public AlterTableSQL modifyColumn(String columnName, String newDataType, boolean notNull, String defaultValue) {
        return modifyColumn(columnName, newDataType, notNull, defaultValue, null);
    }

    public AlterTableSQL modifyColumn(String columnName, String newDataType, boolean notNull, String defaultValue,
                                      String position) {
        validateIdentifier(columnName);
        if (position != null && !position.equals("FIRST") && !position.startsWith("AFTER ")) {
            validateIdentifier(position);
            position = "AFTER " + position;
        }
        operations.add(
                new AlterOperation(OperationType.MODIFY_COLUMN, columnName, newDataType, notNull, defaultValue,
                        position));
        return this;
    }

    public AlterTableSQL moveColumnFirst(String columnName, String dataType) {
        return modifyColumn(columnName, dataType, false, null, "FIRST");
    }

    public AlterTableSQL moveColumnAfter(String columnName, String dataType, String afterColumnName) {
        return modifyColumn(columnName, dataType, false, null, afterColumnName);
    }

    public AlterTableSQL renameColumn(String oldColumnName, String newColumnName) {
        return renameColumn(oldColumnName, newColumnName, null);
    }

    public AlterTableSQL renameColumn(String oldColumnName, String newColumnName, String dataType) {
        validateIdentifier(oldColumnName);
        validateIdentifier(newColumnName);
        operations.add(
                new AlterOperation(OperationType.RENAME_COLUMN, oldColumnName, dataType, false, null, newColumnName));
        return this;
    }

    public AlterTableSQL addPrimaryKey(String... columnNames) {
        for (String columnName : columnNames) {
            validateIdentifier(columnName);
        }
        operations.add(new AlterOperation(OperationType.ADD_PRIMARY_KEY, null, null, false, null,
                String.join(", ", columnNames)));
        return this;
    }

    public AlterTableSQL dropPrimaryKey() {
        operations.add(new AlterOperation(OperationType.DROP_PRIMARY_KEY, null, null, false, null, null));
        return this;
    }

    public AlterTableSQL addForeignKey(String columnName, String referenceTable, String referenceColumn) {
        return addForeignKey(columnName, referenceTable, referenceColumn, null);
    }

    public AlterTableSQL addForeignKey(String columnName, String referenceTable, String referenceColumn,
                                       String constraintName) {
        validateIdentifier(columnName);
        validateIdentifier(referenceTable);
        validateIdentifier(referenceColumn);
        if (constraintName != null) {
            validateIdentifier(constraintName);
        }
        String extraInfo = constraintName != null
                ? constraintName + ":" + referenceTable + "(" + referenceColumn + ")"
                : referenceTable + "(" + referenceColumn + ")";
        operations.add(new AlterOperation(OperationType.ADD_FOREIGN_KEY, columnName, null, false, null, extraInfo));
        return this;
    }

    public AlterTableSQL dropForeignKey(String constraintName) {
        validateIdentifier(constraintName);
        operations.add(new AlterOperation(OperationType.DROP_FOREIGN_KEY, null, null, false, null, constraintName));
        return this;
    }

    public AlterTableSQL addUnique(String... columnNames) {
        for (String columnName : columnNames) {
            validateIdentifier(columnName);
        }
        operations.add(new AlterOperation(OperationType.ADD_UNIQUE, null, null, false, null,
                String.join(", ", columnNames)));
        return this;
    }

    public AlterTableSQL dropUnique(String constraintName) {
        validateIdentifier(constraintName);
        operations.add(new AlterOperation(OperationType.DROP_UNIQUE, null, null, false, null, constraintName));
        return this;
    }

    public AlterTableSQL renameTable(String newTableName) {
        validateIdentifier(newTableName);
        operations.add(new AlterOperation(OperationType.RENAME_TABLE, null, null, false, null, newTableName));
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        if (tableName == null) {
            throw new IllegalStateException("Table name must be specified");
        }

        if (operations.isEmpty()) {
            throw new IllegalStateException("At least one alter operation must be specified");
        }

        sqlBuilder.setLength(0);

        if (type == DatabaseType.SQLITE && operations.size() > 1) {
            throw new UnsupportedOperationException("SQLite only supports one operation per ALTER TABLE statement");
        }

        for (int i = 0; i < operations.size(); i++) {
            if (i > 0) {
                sqlBuilder.append("; ");
            }
            sqlBuilder.append("ALTER TABLE ").append(tableName).append(" ");
            buildOperation(operations.get(i), type);
        }

        return sqlBuilder.toString();
    }

    private void buildOperation(AlterOperation op, DatabaseType type) {
        switch (op.type) {
            case ADD_COLUMN -> {
                if (type == DatabaseType.POSTGRESQL || type == DatabaseType.SQLITE) {
                    if (op.extraInfo != null && (op.extraInfo.equals("FIRST") || op.extraInfo.startsWith("AFTER "))) {
                        throw new UnsupportedOperationException(
                                type + " does not support column positioning (FIRST/AFTER)");
                    }
                }
                sqlBuilder.append("ADD COLUMN ").append(op.columnName).append(" ").append(op.dataType);
                if (op.notNull) {
                    sqlBuilder.append(" NOT NULL");
                }
                if (op.defaultValue != null && !op.defaultValue.isEmpty()) {
                    sqlBuilder.append(" DEFAULT ").append(op.defaultValue);
                }
                if (op.extraInfo != null && (type == DatabaseType.MYSQL || type == DatabaseType.MARIADB)) {
                    if (op.extraInfo.equals("FIRST")) {
                        sqlBuilder.append(" FIRST");
                    } else if (op.extraInfo.startsWith("AFTER ")) {
                        String afterColumn = op.extraInfo.substring(6);
                        validateIdentifier(afterColumn);
                        sqlBuilder.append(" AFTER ").append(afterColumn);
                    }
                }
            }
            case DROP_COLUMN -> {
                if (type == DatabaseType.SQLITE) {
                    throw new UnsupportedOperationException("SQLite does not support DROP COLUMN");
                }
                sqlBuilder.append("DROP COLUMN ").append(op.columnName);
            }
            case MODIFY_COLUMN -> {
                if (type == DatabaseType.SQLITE) {
                    throw new UnsupportedOperationException("SQLite does not support MODIFY COLUMN");
                }
                if (type == DatabaseType.POSTGRESQL) {
                    if (op.extraInfo != null && (op.extraInfo.equals("FIRST") || op.extraInfo.startsWith("AFTER "))) {
                        throw new UnsupportedOperationException(
                                "PostgreSQL does not support column positioning (FIRST/AFTER)");
                    }
                    List<String> alterClauses = new ArrayList<>();
                    if (op.dataType != null) {
                        alterClauses.add("ALTER COLUMN " + op.columnName + " TYPE " + op.dataType);
                    }
                    if (op.notNull) {
                        alterClauses.add("ALTER COLUMN " + op.columnName + " SET NOT NULL");
                    }
                    if (op.defaultValue != null) {
                        if (op.defaultValue.isEmpty()) {
                            alterClauses.add("ALTER COLUMN " + op.columnName + " DROP DEFAULT");
                        } else {
                            alterClauses.add("ALTER COLUMN " + op.columnName + " SET DEFAULT " + op.defaultValue);
                        }
                    }
                    if (alterClauses.isEmpty()) {
                        throw new IllegalStateException(
                                "MODIFY COLUMN requires at least one modification (dataType, notNull, or defaultValue)");
                    }
                    sqlBuilder.append(String.join(", ", alterClauses));
                } else {
                    sqlBuilder.append("MODIFY COLUMN ").append(op.columnName);
                    if (op.dataType != null) {
                        sqlBuilder.append(" ").append(op.dataType);
                    }
                    if (op.notNull) {
                        sqlBuilder.append(" NOT NULL");
                    }
                    if (op.defaultValue != null && !op.defaultValue.isEmpty()) {
                        sqlBuilder.append(" DEFAULT ").append(op.defaultValue);
                    }
                    if (op.extraInfo != null) {
                        if (op.extraInfo.equals("FIRST")) {
                            sqlBuilder.append(" FIRST");
                        } else if (op.extraInfo.startsWith("AFTER ")) {
                            String afterColumn = op.extraInfo.substring(6);
                            validateIdentifier(afterColumn);
                            sqlBuilder.append(" AFTER ").append(afterColumn);
                        }
                    }
                }
            }
            case RENAME_COLUMN -> {
                if (type == DatabaseType.SQLITE) {
                    sqlBuilder.append("RENAME COLUMN ").append(op.columnName).append(" TO ").append(op.extraInfo);
                } else if (type == DatabaseType.POSTGRESQL) {
                    sqlBuilder.append("RENAME COLUMN ").append(op.columnName).append(" TO ").append(op.extraInfo);
                } else {
                    sqlBuilder.append("CHANGE COLUMN ").append(op.columnName).append(" ").append(op.extraInfo);
                    if (op.dataType != null) {
                        sqlBuilder.append(" ").append(op.dataType);
                    } else {
                        throw new IllegalStateException("MySQL/MariaDB RENAME COLUMN requires data type. " +
                                "Use renameColumn(oldName, newName, dataType) instead.");
                    }
                }
            }
            case ADD_PRIMARY_KEY -> {
                sqlBuilder.append("ADD PRIMARY KEY (").append(op.extraInfo).append(")");
            }
            case DROP_PRIMARY_KEY -> {
                if (type == DatabaseType.SQLITE) {
                    throw new UnsupportedOperationException("SQLite does not support DROP PRIMARY KEY");
                }
                sqlBuilder.append("DROP PRIMARY KEY");
            }
            case RENAME_TABLE -> {
                sqlBuilder.append("RENAME TO ").append(op.extraInfo);
            }
            case ADD_FOREIGN_KEY -> {
                String constraintName = null;
                String reference = op.extraInfo;
                if (op.extraInfo != null && op.extraInfo.contains(":")) {
                    int colonIndex = op.extraInfo.indexOf(':');
                    constraintName = op.extraInfo.substring(0, colonIndex);
                    reference = op.extraInfo.substring(colonIndex + 1);
                }

                if (constraintName != null) {
                    sqlBuilder.append("ADD CONSTRAINT ").append(constraintName).append(" ");
                }
                sqlBuilder.append("FOREIGN KEY (").append(op.columnName).append(") REFERENCES ").append(reference);
            }
            case DROP_FOREIGN_KEY -> {
                if (type == DatabaseType.SQLITE) {
                    throw new UnsupportedOperationException("SQLite does not support DROP FOREIGN KEY");
                }
                if (type == DatabaseType.POSTGRESQL) {
                    sqlBuilder.append("DROP CONSTRAINT ").append(op.extraInfo);
                } else {
                    sqlBuilder.append("DROP FOREIGN KEY ").append(op.extraInfo);
                }
            }
            case ADD_UNIQUE -> {
                sqlBuilder.append("ADD UNIQUE (").append(op.extraInfo).append(")");
            }
            case DROP_UNIQUE -> {
                if (type == DatabaseType.SQLITE) {
                    throw new UnsupportedOperationException("SQLite does not support DROP UNIQUE");
                }
                if (type == DatabaseType.POSTGRESQL) {
                    sqlBuilder.append("DROP CONSTRAINT ").append(op.extraInfo);
                } else {
                    sqlBuilder.append("DROP INDEX ").append(op.extraInfo);
                }
            }
        }
    }

    @Override
    public java.sql.PreparedStatement build(java.sql.Connection connection, DatabaseType type)
            throws java.sql.SQLException {
        return connection.prepareStatement(getSql(type));
    }

    private enum OperationType {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        RENAME_COLUMN,
        RENAME_TABLE,
        ADD_PRIMARY_KEY,
        DROP_PRIMARY_KEY,
        ADD_FOREIGN_KEY,
        DROP_FOREIGN_KEY,
        ADD_UNIQUE,
        DROP_UNIQUE
    }

    private record AlterOperation(OperationType type, String columnName, String dataType, boolean notNull,
                                  String defaultValue, String extraInfo) {
    }
}
