package com.datastrato.graviton.exceptions;

public class NoSuchCatalogException extends NotFoundException {

  public NoSuchCatalogException(String message) {
    super(message);
  }

  public NoSuchCatalogException(String message, Throwable cause) {
    super(message, cause);
  }
}
