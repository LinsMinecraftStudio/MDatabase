package io.github.lijinhong11.mdatabase.sql.conditions;

import java.util.List;

public interface Condition {
    String getSql();
    List<Object> getParameters();
}

