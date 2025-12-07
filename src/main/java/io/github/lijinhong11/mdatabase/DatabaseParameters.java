package io.github.lijinhong11.mdatabase;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.TimeUnit;

@Data
@NoArgsConstructor
public class DatabaseParameters {
    private int maxPoolSize = 10;
    private long idleTimeout = 3000;
    private long maxKeepAlive = TimeUnit.MINUTES.toMillis(2L);
}
