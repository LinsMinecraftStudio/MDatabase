package io.github.lijinhong11.mdatabase.sql.conditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Conditions {
    /**
     * Create an equality condition (column = value)
     * 
     * @param column the column name
     * @param value  the value to compare
     * @return a Condition representing the equality comparison
     */
    public static Condition eq(String column, Object value) {
        return new SimpleCondition(column, "=", value);
    }

    /**
     * Create a not-equal condition (column <> value)
     * 
     * @param column the column name
     * @param value  the value to compare
     * @return a Condition representing the not-equal comparison
     */
    public static Condition ne(String column, Object value) {
        return new SimpleCondition(column, "<>", value);
    }

    /**
     * Create a greater-than condition (column > value)
     * 
     * @param column the column name
     * @param value  the value to compare
     * @return a Condition representing the greater-than comparison
     */
    public static Condition gt(String column, Object value) {
        return new SimpleCondition(column, ">", value);
    }

    /**
     * Create a less-than condition (column < value)
     * 
     * @param column the column name
     * @param value  the value to compare
     * @return a Condition representing the less-than comparison
     */
    public static Condition lt(String column, Object value) {
        return new SimpleCondition(column, "<", value);
    }

    /**
     * Create a LIKE condition (column LIKE pattern)
     * 
     * @param column  the column name
     * @param pattern the LIKE pattern (supports % and _ wildcards)
     * @return a Condition representing the LIKE comparison
     */
    public static Condition like(String column, String pattern) {
        return new SimpleCondition(column, "LIKE", pattern);
    }

    /**
     * Create an IS NULL condition (column IS NULL)
     * 
     * @param column the column name
     * @return a Condition representing the IS NULL check
     */
    public static Condition isNull(String column) {
        return new SimpleCondition(column, "IS", null);
    }

    /**
     * Create an IS NOT NULL condition (column IS NOT NULL)
     * 
     * @param column the column name
     * @return a Condition representing the IS NOT NULL check
     */
    public static Condition isNotNull(String column) {
        return new SimpleCondition(column, "IS NOT", null);
    }

    /**
     * Combine multiple conditions with AND operator
     * 
     * @param ands the conditions to combine with AND
     * @return a Condition representing the AND combination of all conditions
     */
    public static Condition and(Condition... ands) {
        return new AppendableCondition(ands, "AND");
    }

    /**
     * Combine multiple conditions with OR operator
     * 
     * @param ors the conditions to combine with OR
     * @return a Condition representing the OR combination of all conditions
     */
    public static Condition or(Condition... ors) {
        return new AppendableCondition(ors, "OR");
    }

    /**
     * Create an IN condition (column IN (value1, value2, ...))
     * 
     * @param column the column name
     * @param values the list of values to check against
     * @return a Condition representing the IN clause
     */
    public static Condition in(String column, List<?> values) {
        String placeholders = String.join(", ", Collections.nCopies(values.size(), "?"));
        return new SimpleCondition(column, "IN (" + placeholders + ")", values);
    }

    /**
     * Create a NOT IN condition (column NOT IN (value1, value2, ...))
     * 
     * @param column the column name
     * @param values the list of values to check against
     * @return a Condition representing the NOT IN clause
     */
    public static Condition notIn(String column, List<?> values) {
        String placeholders = String.join(", ", Collections.nCopies(values.size(), "?"));
        return new SimpleCondition(column, "NOT IN (" + placeholders + ")", values);
    }

    /**
     * Create a BETWEEN condition (column BETWEEN lower AND upper)
     * 
     * @param column the column name
     * @param lower  the lower bound (inclusive)
     * @param upper  the upper bound (inclusive)
     * @return a Condition representing the BETWEEN range check
     */
    public static Condition between(String column, Object lower, Object upper) {
        return new SimpleCondition(column, "BETWEEN ? AND ?", Arrays.asList(lower, upper));
    }
}
