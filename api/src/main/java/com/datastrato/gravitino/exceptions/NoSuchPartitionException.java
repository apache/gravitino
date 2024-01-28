/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a partition with specified name is not existed. */
public class NoSuchPartitionException extends NotFoundException {

  public NoSuchPartitionException(String message) {
    super(message);
  }

  public NoSuchPartitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
