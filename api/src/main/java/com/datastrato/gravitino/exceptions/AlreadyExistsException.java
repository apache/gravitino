/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when an entity or resource already exists. */
public class AlreadyExistsException extends GravitinoRuntimeException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public AlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public AlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
