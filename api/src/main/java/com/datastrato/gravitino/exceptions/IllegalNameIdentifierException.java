/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a name identifier is invalid. */
public class IllegalNameIdentifierException extends IllegalArgumentException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public IllegalNameIdentifierException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public IllegalNameIdentifierException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified cause.
   *
   * @param cause the cause.
   */
  public IllegalNameIdentifierException(Throwable cause) {
    super(cause);
  }

  /** Constructs a new exception with the specified detail message and cause. */
  public IllegalNameIdentifierException() {
    super();
  }
}
