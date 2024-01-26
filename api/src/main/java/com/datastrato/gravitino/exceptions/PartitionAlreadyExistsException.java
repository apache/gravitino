/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a partition with specified name already exists. */
public class PartitionAlreadyExistsException extends AlreadyExistsException {

  public PartitionAlreadyExistsException(String message) {
    super(message);
  }

  public PartitionAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
