package com.datastrato.graviton.exceptions;

/** Exception thrown when a namespace with specified name is not existed. */
public class NoSuchNamespaceException extends NotFoundException {

  public NoSuchNamespaceException(String message) {
    super(message);
  }

  public NoSuchNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
