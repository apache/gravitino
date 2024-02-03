/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a partition with specified name already exists. */
public class PartitionAlreadyExistsException extends AlreadyExistsException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public PartitionAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public PartitionAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
