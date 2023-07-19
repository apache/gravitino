/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.exceptions;

public class GravitonRuntimeException extends RuntimeException {

  public GravitonRuntimeException(String message) {
    super(message);
  }

  public GravitonRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
