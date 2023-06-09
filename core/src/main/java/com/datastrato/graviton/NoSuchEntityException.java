package com.datastrato.graviton;

public class NoSuchEntityException extends RuntimeException {

    public NoSuchEntityException(String message) {
      super(message);
    }

    public NoSuchEntityException(String message, Throwable cause) {
      super(message, cause);
    }
}
