package io.github.lijinhong11.mdatabase.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.lijinhong11.mdatabase.DatabaseParameters;
import io.github.lijinhong11.mdatabase.enums.DatabaseType;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

class SQLiteConnection extends AbstractSQLConnection {
    private static final String JDBC_URL_FORMAT = "jdbc:sqlite:%s";
    private static final String JDBC_DRIVER_CLASS_NAME = "org.sqlite.JDBC";

    private final HikariDataSource dataSource;

    public SQLiteConnection(String absolutePath, DatabaseParameters parameters) {
        if (absolutePath == null || absolutePath.isBlank()) {
            throw new IllegalArgumentException("the absolute path of database file cannot be null or blank");
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format(JDBC_URL_FORMAT, absolutePath));
        config.setDriverClassName(JDBC_DRIVER_CLASS_NAME);
        config.setIdleTimeout(parameters.getIdleTimeout());
        config.setMaximumPoolSize(parameters.getMaxPoolSize());
        config.setAutoCommit(true);
        dataSource = new HikariDataSource(config);
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
        return DatabaseType.SQLITE;
    }
}
