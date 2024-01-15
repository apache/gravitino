/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a file with specified name is not existed. */
public class NoSuchFileException extends NotFoundException {

  public NoSuchFileException(String message) {
    super(message);
  }

  public NoSuchFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
