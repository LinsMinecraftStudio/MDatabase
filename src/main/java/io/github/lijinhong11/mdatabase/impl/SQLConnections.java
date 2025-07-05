package io.github.lijinhong11.mdatabase.impl;

import io.github.lijinhong11.mdatabase.DatabaseConnection;
import io.github.lijinhong11.mdatabase.DatabaseParameters;

public final class SQLConnections {
    private SQLConnections() {
    }

    public static DatabaseConnection sqlite(String file, DatabaseParameters parameters) {
        return new SQLiteConnection(file, parameters);
    }

    public static DatabaseConnection mysql(String host, int port, String database, String username, String password, DatabaseParameters parameters) {
        return new MySQLConnection(host, port, database, username, password, parameters);
    }

    public static DatabaseConnection mariadb(String host, int port, String database, String username, String password, DatabaseParameters parameters) {
        return new MariaDBConnection(host, port, database, username, password, parameters);
    }

    public static DatabaseConnection postgresql(String host, int port, String database, String username, String password, DatabaseParameters parameters) {
        return new PostgreSQLConnection(host, port, database, username, password, parameters);
    }
}
