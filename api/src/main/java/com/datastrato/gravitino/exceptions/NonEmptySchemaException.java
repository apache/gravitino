/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a namespace is not empty. */
public class NonEmptySchemaException extends GravitinoRuntimeException {

  public NonEmptySchemaException(String message) {
    super(message);
  }

  public NonEmptySchemaException(String message, Throwable cause) {
    super(message, cause);
  }
}
