package com.datastrato.graviton.exceptions;

public class NotFoundException extends GravitonRuntimeException {

  public NotFoundException(String message) {
    super(message);
  }

  public NotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
