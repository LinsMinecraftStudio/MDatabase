package io.github.lijinhong11.mdatabase.enums;

public enum DatabaseType {
    SQLITE,
    MYSQL,
    MARIADB,
    POSTGRESQL;

    public static DatabaseType getByName(String name) {
        for (DatabaseType value : values()) {
            if (value.name().equalsIgnoreCase(name)) {
                return value;
            }
        }
        return null;
    }
}
