package com.datastrato.graviton.exceptions;

/** Exception thrown when a namespace is not empty. */
public class NonEmptySchemaException extends GravitonRuntimeException {

  public NonEmptySchemaException(String message) {
    super(message);
  }

  public NonEmptySchemaException(String message, Throwable cause) {
    super(message, cause);
  }
}
