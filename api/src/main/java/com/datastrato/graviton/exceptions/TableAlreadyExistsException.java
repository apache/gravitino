package com.datastrato.graviton.exceptions;

/** Exception thrown when a table with specified name already exists. */
public class TableAlreadyExistsException extends AlreadyExistsException {

  public TableAlreadyExistsException(String message) {
    super(message);
  }

  public TableAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
