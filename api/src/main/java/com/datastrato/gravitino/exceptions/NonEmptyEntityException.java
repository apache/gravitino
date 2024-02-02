/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

/** An exception thrown when a resource is not empty. */
public class NonEmptyEntityException extends GravitinoRuntimeException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public NonEmptyEntityException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public NonEmptyEntityException(String message, Throwable cause) {
    super(message, cause);
  }
}
