package com.datastrato.graviton.exceptions;

/** Exception thrown when a namespace is not empty. */
public class NonEmptyNamespaceException extends GravitonRuntimeException {

  public NonEmptyNamespaceException(String message) {
    super(message);
  }

  public NonEmptyNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
