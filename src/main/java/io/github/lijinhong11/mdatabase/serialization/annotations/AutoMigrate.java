package io.github.lijinhong11.mdatabase.serialization.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * When present on a {@code @Table} class, {@code createTableByClass} will
 * automatically add missing columns after table creation. Optionally drops
 * orphaned columns when {@link #drop()} is {@code true}.
 *
 * <pre>{@code
 * @AutoMigrate
 * @Table(name = "users")
 * public class User { ... }
 *
 * @AutoMigrate(drop = true)  // also drops columns not in the class
 * @Table(name = "users")
 * public class UserV2 { ... }
 * }</pre>
 *
 * <h3>Limitations</h3>
 * <ul>
 *   <li><b>Field type changes</b> are NOT migrated — {@code ALTER TABLE MODIFY COLUMN}
 *       is not emitted. Changing a field from {@code String} to {@code int} may cause
 *       deserialization failures on existing data.</li>
 *   <li><b>Field renames</b> without {@link Column#renamedFrom()} will leave the old
 *       column in the table and add a new one. Use {@code @Column(renamedFrom = "old")}
 *       on the renamed field to emit {@code ALTER TABLE RENAME COLUMN}.</li>
 *   <li><b>Dropping columns</b> ({@code drop = true}) permanently deletes data.
 *       Ensure you have backups before enabling.</li>
 *   <li><b>Primary keys and indexes</b> are not modified — only plain column
 *       additions, renames, and drops are handled.</li>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AutoMigrate {
    /**
     * Whether to drop columns that exist in the table but not in the class.
     * Defaults to {@code false} for safety — columns with data will NOT be dropped.
     *
     * @return {@code true} to enable column dropping
     */
    boolean drop() default false;
}
