package com.datastrato.graviton;

public class EntityAlreadyExistsException extends RuntimeException {

    public EntityAlreadyExistsException(String message) {
      super(message);
    }

    public EntityAlreadyExistsException(String message, Throwable cause) {
      super(message, cause);
    }
}
