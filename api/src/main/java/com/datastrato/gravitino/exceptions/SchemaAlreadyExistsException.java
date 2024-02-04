/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when the schema already exists. */
public class SchemaAlreadyExistsException extends AlreadyExistsException {

  /**
   * Constructs a new SchemaAlreadyExistsException.
   *
   * @param message the detail message.
   */
  public SchemaAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructs a new SchemaAlreadyExistsException.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public SchemaAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
