package com.datastrato.graviton;

public class EntityNotEmptyException extends RuntimeException {
  public EntityNotEmptyException(String message) {
    super(message);
  }

  public EntityNotEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
