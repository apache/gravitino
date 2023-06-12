package com.datastrato.graviton.meta.catalog;

/** Exception thrown when a namespace is not empty. */
public class NonEmptyNamespaceException extends RuntimeException {

  public NonEmptyNamespaceException(String message) {
    super(message);
  }

  public NonEmptyNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }
}
