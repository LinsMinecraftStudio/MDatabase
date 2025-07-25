package io.github.lijinhong11.mdatabase.sql.sentence;

import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import io.github.lijinhong11.mdatabase.sql.conditions.Condition;

public class DeleteSQL extends SQL {
    private String table;
    private Condition whereCondition;

    DeleteSQL() {}

    public DeleteSQL from(String table) {
        validateIdentifier(table);
        this.table = table;
        return this;
    }

    public DeleteSQL where(Condition condition) {
        this.whereCondition = condition;
        return this;
    }

    @Override
    public String getSql(DatabaseType type) {
        sqlBuilder.setLength(0);
        sqlBuilder.append("DELETE FROM ").append(table);

        if (whereCondition != null) {
            sqlBuilder.append(" WHERE ").append(whereCondition.getSql());
            parameters.addAll(whereCondition.getParameters());
        }

        return sqlBuilder.toString();
    }
}
