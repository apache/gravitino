/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a table with specified name is not existed. */
public class NoSuchTableException extends NotFoundException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public NoSuchTableException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public NoSuchTableException(String message, Throwable cause) {
    super(message, cause);
  }
}
