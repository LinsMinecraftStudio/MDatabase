package io.github.lijinhong11.mdatabase.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.lijinhong11.mdatabase.DatabaseParameters;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

class MySQLConnection extends AbstractSQLConnection {
    private static final String JDBC_URL_FORMAT = "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC";
    private static final String JDBC_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    private final HikariDataSource dataSource;

    public MySQLConnection(String host, int port, String database, String username, String password, DatabaseParameters parameters) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(JDBC_URL_FORMAT.formatted(host, port, database));
        cfg.setDriverClassName(JDBC_DRIVER_CLASS_NAME);
        cfg.setUsername(username);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(parameters.getMaxPoolSize());
        cfg.setAutoCommit(true);
        cfg.setIdleTimeout(parameters.getIdleTimeout());
        try (HikariDataSource dataSource = new HikariDataSource(cfg)) {
            this.dataSource = dataSource;
        }
    }


    @Override
    Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }

    @Override
    public @NotNull DatabaseType getType() {
        return DatabaseType.MYSQL;
    }
}
