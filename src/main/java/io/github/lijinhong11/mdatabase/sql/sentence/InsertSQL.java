package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;

import java.util.*;
import java.util.stream.Collectors;

public class InsertSQL extends SQL {
    private final Map<String, Object> values = new LinkedHashMap<>();
    private final boolean upsert;
    private String table;
    private Condition whereCondition;
    private List<String> conflictKeys;

    InsertSQL(boolean upsert) {
        this.upsert = upsert;
    }

    public InsertSQL into(String table) {
        validateIdentifier(table);
        this.table = table;
        return this;
    }

    public InsertSQL value(String column, Object value) {
        validateIdentifier(column);
        values.put(column, value);
        return this;
    }

    public InsertSQL where(Condition condition) {
        this.whereCondition = condition;
        return this;
    }

    /**
     * 设置冲突字段（如主键或唯一键），仅用于 SQLite / PostgreSQL。
     */
    public InsertSQL conflictKeys(String... keys) {
        this.conflictKeys = Arrays.asList(keys);
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        sqlBuilder.setLength(0);
        parameters.clear();

        sqlBuilder.append("INSERT INTO ").append(table);

        if (!values.isEmpty()) {
            sqlBuilder.append(" (")
                    .append(String.join(", ", values.keySet()))
                    .append(") VALUES (")
                    .append(String.join(", ", Collections.nCopies(values.size(), "?")))
                    .append(")");
            parameters.addAll(values.values());
        }

        if (!upsert) {
            appendWhere();
            return sqlBuilder.toString(); // 普通插入
        }

        switch (type) {
            case SQLITE, POSTGRESQL -> {
                if (conflictKeys == null || conflictKeys.isEmpty()) {
                    throw new IllegalStateException(type + " UPSERT requires conflict keys");
                }
                sqlBuilder.append(" ON CONFLICT(")
                        .append(String.join(", ", conflictKeys))
                        .append(") DO UPDATE SET ");
                sqlBuilder.append(values.keySet().stream()
                        .map(col -> col + " = excluded." + col)
                        .collect(Collectors.joining(", ")));
            }
            case MYSQL, MARIADB -> {
                sqlBuilder.append(" ON DUPLICATE KEY UPDATE ");
                sqlBuilder.append(values.keySet().stream()
                        .map(col -> col + " = VALUES(" + col + ")")
                        .collect(Collectors.joining(", ")));
            }

            default -> throw new UnsupportedOperationException("Unsupported database type: " + type);
        }

        appendWhere();
        return sqlBuilder.toString();
    }

    private void appendWhere() {
        if (whereCondition != null) {
            sqlBuilder.append(" WHERE ").append(whereCondition.getSql());
            parameters.addAll(whereCondition.getParameters());
        }
    }
}