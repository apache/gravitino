package com.datastrato.graviton.exceptions;

/** Exception thrown when a table with specified name is not existed. */
public class NoSuchTableException extends NotFoundException {

  public NoSuchTableException(String message) {
    super(message);
  }

  public NoSuchTableException(String message, Throwable cause) {
    super(message, cause);
  }
}
