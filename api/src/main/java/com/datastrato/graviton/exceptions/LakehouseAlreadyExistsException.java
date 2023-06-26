package com.datastrato.graviton.exceptions;

public class LakehouseAlreadyExistsException extends AlreadyExistsException {

  public LakehouseAlreadyExistsException(String message) {
    super(message);
  }

  public LakehouseAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
