package io.github.lijinhong11.mdatabase.exceptions;

public class InstantiationFailedException extends RuntimeException {
    public InstantiationFailedException(Class<?> clazz, Throwable cause) {
        super("Failed to create an instance of " + clazz.getName(), cause);
    }
}
