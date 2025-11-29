package io.github.lijinhong11.mdatabase.impl;

import io.github.lijinhong11.mdatabase.DatabaseConnection;
import io.github.lijinhong11.mdatabase.DatabaseParameters;

public final class SQLConnections {
    private SQLConnections() {
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
