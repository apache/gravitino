/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a schema is not found. */
public class NoSuchSchemaException extends NotFoundException {

  public NoSuchSchemaException(String message) {
    super(message);
  }

  public NoSuchSchemaException(String message, Throwable cause) {
    super(message, cause);
  }
}
