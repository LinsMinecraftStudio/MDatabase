package io.github.lijinhong11.mdatabase.sql.conditions;

import java.util.Collections;
import java.util.List;

class SimpleCondition implements Condition {
    private final String column;
    private final String operator;
    private final Object value;

    public SimpleCondition(String column, String operator, Object value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String getSql() {
        return column + " " + operator + " ?";
    }

    @Override
    public List<Object> getParameters() {
        return Collections.singletonList(value);
    }
}
