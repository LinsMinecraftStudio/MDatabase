package io.github.lijinhong11.mdatabase.impl;

import io.github.lijinhong11.mdatabase.DatabaseConnection;
import io.github.lijinhong11.mdatabase.DatabaseParameters;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public final class DatabaseConnections {
    private DatabaseConnections() {
    }

    /**
     * Create a database connection by the database type
     *
     * @param type       the database type
     * @param sqlite     the sqlite database file
     * @param host       the database host address
     * @param port       the database port number
     * @param database   the database name
     * @param username   the database username
     * @param password   the database password
     * @param parameters the connection pool parameters
     * @return a DatabaseConnection instance for MySQL
     */
    @Contract("null, _, _, _, _, _, _, _ -> fail")
    public static DatabaseConnection createByType(DatabaseType type, @Nullable File sqlite, String host, int port, String database, String username, String password,
                                                  DatabaseParameters parameters) {
        if (type == null) {
            throw new RuntimeException(new NullPointerException("database type"));
        }

        switch (type) {
            case POSTGRESQL -> {
                return postgresql(host, port, database, username, password, parameters);
            }
            case SQLITE -> {
                if (sqlite == null) {
                    throw new RuntimeException(new NullPointerException("sqlite database file"));
                }
                return sqlite(sqlite.getAbsolutePath(), parameters);
            }
            case MARIADB -> {
                return mariadb(host, port, database, username, password, parameters);
            }
            case MYSQL -> {
                return mysql(host, port, database, username, password, parameters);
            }
        }

        return null;
    }

    /**
     * Create a SQLite database connection
     *
     * @param file       the path to the SQLite database file
     * @param parameters the connection pool parameters
     * @return a DatabaseConnection instance for SQLite
     */
    public static DatabaseConnection sqlite(String file, DatabaseParameters parameters) {
        return new SQLiteConnection(file, parameters);
    }

    /**
     * Create a MySQL database connection
     *
     * @param host       the database host address
     * @param port       the database port number
     * @param database   the database name
     * @param username   the database username
     * @param password   the database password
     * @param parameters the connection pool parameters
     * @return a DatabaseConnection instance for MySQL
     */
    public static DatabaseConnection mysql(String host, int port, String database, String username, String password,
                                           DatabaseParameters parameters) {
        return new MySQLConnection(host, port, database, username, password, parameters);
    }

    /**
     * Create a MariaDB database connection
     *
     * @param host       the database host address
     * @param port       the database port number
     * @param database   the database name
     * @param username   the database username
     * @param password   the database password
     * @param parameters the connection pool parameters
     * @return a DatabaseConnection instance for MariaDB
     */
    public static DatabaseConnection mariadb(String host, int port, String database, String username, String password,
                                             DatabaseParameters parameters) {
        return new MariaDBConnection(host, port, database, username, password, parameters);
    }

    /**
     * Create a PostgreSQL database connection
     *
     * @param host       the database host address
     * @param port       the database port number
     * @param database   the database name
     * @param username   the database username
     * @param password   the database password
     * @param parameters the connection pool parameters
     * @return a DatabaseConnection instance for PostgreSQL
     */
    public static DatabaseConnection postgresql(String host, int port, String database, String username,
                                                String password, DatabaseParameters parameters) {
        return new PostgreSQLConnection(host, port, database, username, password, parameters);
    }
}
