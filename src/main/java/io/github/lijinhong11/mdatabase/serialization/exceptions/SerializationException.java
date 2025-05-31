package io.github.lijinhong11.mdatabase.serialization.exceptions;

public class SerializationException extends RuntimeException {
    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
