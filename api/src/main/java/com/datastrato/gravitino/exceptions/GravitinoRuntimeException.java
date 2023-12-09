/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Base class for all Gravitino runtime exceptions. */
public class GravitinoRuntimeException extends RuntimeException {

  public GravitinoRuntimeException(String message) {
    super(message);
  }

  public GravitinoRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
