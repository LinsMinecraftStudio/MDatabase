package io.github.lijinhong11.mdatabase.serialization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as a database column. Required on every field that should be persisted.
 * <pre>{@code
 * @Column                    // column name = field name
 * public String email;
 *
 * @Column(name = "phone_no") // explicit column name
 * public String phone;
 *
 * @Column(nullable = false, defaultValue = "0")
 * public int score;
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {
    /**
     * Constant value meaning "use the field name as column name".
     */
    String AUTO_NAMED = "AUTO_FOLLOW_FIELD_NAME";

    /**
     * The column name in the database. Defaults to the Java field name.
     *
     * @return the column name
     */
    String name() default AUTO_NAMED;

    /**
     * The default value for this column in {@code CREATE TABLE}.
     *
     * @return the default value expression, or empty string for none
     */
    String defaultValue() default "";

    /**
     * Whether this column allows {@code NULL}. Defaults to {@code true}.
     *
     * @return {@code true} if nullable
     */
    boolean nullable() default true;

    /**
     * Previous column name, used by {@code autoMigrate} to emit
     * {@code ALTER TABLE RENAME COLUMN old TO new}.
     * Only effective when the current column name differs from this value.
     *
     * @return the old column name, or empty string if no rename needed
     */
    String renamedFrom() default "";
}
