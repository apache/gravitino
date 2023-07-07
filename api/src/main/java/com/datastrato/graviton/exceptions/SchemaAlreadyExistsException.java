package com.datastrato.graviton.exceptions;

public class SchemaAlreadyExistsException extends AlreadyExistsException {

  public SchemaAlreadyExistsException(String message) {
    super(message);
  }

  public SchemaAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
