package io.github.lijinhong11.mdatabase.serialization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a database table.
 * <pre>{@code
 * @Table(name = "users")
 * public class User {
 *     @PrimaryKey @Column public int id;
 *     @Column public String name;
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {
    /**
     * The table name in the database.
     *
     * @return the table name
     */
    String name();

    /**
     * Optional storage engine (e.g. {@code "InnoDB"} for MySQL).
     *
     * @return the engine name, or empty string for default
     */
    String engine() default "";
}
