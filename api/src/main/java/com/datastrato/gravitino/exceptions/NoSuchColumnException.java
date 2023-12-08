/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a column with specified name is not existed. */
public class NoSuchColumnException extends NotFoundException {

  public NoSuchColumnException(String message) {
    super(message);
  }

  public NoSuchColumnException(String message, Throwable cause) {
    super(message, cause);
  }
}
