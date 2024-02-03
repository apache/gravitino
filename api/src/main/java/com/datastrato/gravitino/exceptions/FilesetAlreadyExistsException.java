/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a file with specified name already exists. */
public class FilesetAlreadyExistsException extends AlreadyExistsException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public FilesetAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public FilesetAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
