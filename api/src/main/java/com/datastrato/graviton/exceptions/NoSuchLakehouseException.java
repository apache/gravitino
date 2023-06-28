package com.datastrato.graviton.exceptions;

public class NoSuchLakehouseException extends NotFoundException {

  public NoSuchLakehouseException(String message) {
    super(message);
  }

  public NoSuchLakehouseException(String message, Throwable cause) {
    super(message, cause);
  }
}
