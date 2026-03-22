package io.github.lijinhong11.mdatabase;

import com.zaxxer.hikari.HikariConfig;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.TimeUnit;

@Data
@NoArgsConstructor
public class DatabaseParameters {
    private int maxPoolSize = 10;
    private Integer minimumIdle;
    private boolean autoCommit = true;
    private long connectionTimeout = TimeUnit.SECONDS.toMillis(30L);
    private long validationTimeout = TimeUnit.SECONDS.toMillis(5L);
    private long idleTimeout = TimeUnit.MINUTES.toMillis(10L);
    private long maxLifetime = TimeUnit.MINUTES.toMillis(30L);
    private long leakDetectionThreshold = 0L;
    private long maxKeepAlive = TimeUnit.MINUTES.toMillis(2L);
    private String poolName;

    public void applyTo(HikariConfig config) {
        config.setMaximumPoolSize(maxPoolSize);
        config.setAutoCommit(autoCommit);
        config.setConnectionTimeout(connectionTimeout);
        config.setValidationTimeout(validationTimeout);
        config.setIdleTimeout(idleTimeout);
        config.setMaxLifetime(maxLifetime);
        config.setLeakDetectionThreshold(leakDetectionThreshold);
        config.setKeepaliveTime(maxKeepAlive);

        if (minimumIdle != null) {
            config.setMinimumIdle(minimumIdle);
        }

        if (poolName != null && !poolName.isBlank()) {
            config.setPoolName(poolName);
        }
    }
}
