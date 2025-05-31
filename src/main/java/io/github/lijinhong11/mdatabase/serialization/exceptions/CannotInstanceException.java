package io.github.lijinhong11.mdatabase.serialization.exceptions;

public class CannotInstanceException extends RuntimeException {
    public CannotInstanceException(Class<?> clazz, Throwable cause) {
        super("Failed to create an instance of " + clazz.getName(), cause);
    }
}
