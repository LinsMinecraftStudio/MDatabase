package io.github.lijinhong11.mdatabase.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.lijinhong11.mdatabase.DatabaseParameters;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database connection backed by the MariaDB Connector/J driver.
 * Supports both MySQL ({@code jdbc:mysql://}) and MariaDB ({@code jdbc:mariadb://}) URL prefixes.
 */
class MariaDBConnection extends AbstractDatabaseConnection {
    private static final String JDBC_DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver";
    private static final String MYSQL_URL_FORMAT = "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC";
    private static final String MARIADB_URL_FORMAT = "jdbc:mariadb://%s:%d/%s?useSSL=false&serverTimezone=UTC";

    private final HikariDataSource dataSource;
    private final DatabaseType databaseType;

    MariaDBConnection(DatabaseType type, String host, int port, String database, String username, String password,
                      DatabaseParameters parameters) {
        String urlFormat = type == DatabaseType.MYSQL ? MYSQL_URL_FORMAT : MARIADB_URL_FORMAT;
        this.databaseType = type;

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(urlFormat.formatted(host, port, database));
        cfg.setDriverClassName(JDBC_DRIVER_CLASS_NAME);
        cfg.setUsername(username);
        cfg.setPassword(password);
        parameters.applyTo(cfg);
        this.dataSource = new HikariDataSource(cfg);
    }

    @Override
    Connection createRawConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public @NotNull DatabaseType getType() {
        return databaseType;
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
