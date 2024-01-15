/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a file with specified name is not existed. */
public class NoSuchFilesetException extends NotFoundException {

  public NoSuchFilesetException(String message) {
    super(message);
  }

  public NoSuchFilesetException(String message, Throwable cause) {
    super(message, cause);
  }
}
