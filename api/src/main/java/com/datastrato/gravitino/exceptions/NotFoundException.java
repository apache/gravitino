/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Base class for all exceptions thrown when a resource is not found. */
public class NotFoundException extends GravitinoRuntimeException {

  /**
   * Constructs a new NotFoundException.
   *
   * @param message The detail message.
   */
  public NotFoundException(String message) {
    super(message);
  }

  /**
   * Constructs a new NotFoundException.
   *
   * @param message The detail message.
   * @param cause The cause of the exception.
   */
  public NotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
