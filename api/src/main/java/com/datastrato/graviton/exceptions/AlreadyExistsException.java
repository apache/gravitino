package com.datastrato.graviton.exceptions;

public class AlreadyExistsException extends GravitonRuntimeException {

  public AlreadyExistsException(String message) {
    super(message);
  }

  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
