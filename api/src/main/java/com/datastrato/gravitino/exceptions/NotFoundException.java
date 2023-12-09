/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Base class for all exceptions thrown when a resource is not found. */
public class NotFoundException extends GravitinoRuntimeException {

  public NotFoundException(String message) {
    super(message);
  }

  public NotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
