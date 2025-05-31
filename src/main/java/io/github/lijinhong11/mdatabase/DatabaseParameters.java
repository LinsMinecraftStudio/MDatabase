package io.github.lijinhong11.mdatabase;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DatabaseParameters {
    private int maxPoolSize = 10;
    private long idleTimeout = 3000;
}
