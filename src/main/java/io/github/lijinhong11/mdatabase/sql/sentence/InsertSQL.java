package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;

import java.util.*;

public class InsertSQL extends SQL {
    private final Map<String, Object> values = new LinkedHashMap<>();
    private final boolean upsert;

    private String table;
    private Condition whereCondition;

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

    @Override
    public String getSql(DatabaseType type) {
        sqlBuilder.setLength(0);
        sqlBuilder.append("INSERT ");

        if (upsert && type == DatabaseType.SQLITE) {
            sqlBuilder.append("OR REPLACE ");
        }

        sqlBuilder.append("INTO ").append(table);

        if (!values.isEmpty()) {
            sqlBuilder.append(" (")
                    .append(String.join(", ", values.keySet()))
                    .append(") VALUES (")
                    .append(String.join(", ", Collections.nCopies(values.size(), "?")))
                    .append(")");
            parameters.addAll(values.values());
        }

        if (upsert && type == DatabaseType.MYSQL) {
            sqlBuilder.append(" ON DUPLICATE KEY UPDATE ");

            List<String> updates = new ArrayList<>();
            for (String column : values.keySet()) {
                updates.add(column + "=VALUES(" + column + ")");
            }

            sqlBuilder.append(String.join(", ", updates));
        }

        if (whereCondition != null) {
            sqlBuilder.append(" WHERE ").append(whereCondition.getSql());
            parameters.addAll(whereCondition.getParameters());
        }

        return sqlBuilder.toString();
    }
}
