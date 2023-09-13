/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.exceptions;

public class NonEmptyEntityException extends GravitonRuntimeException {

  public NonEmptyEntityException(String message) {
    super(message);
  }

  public NonEmptyEntityException(String message, Throwable cause) {
    super(message, cause);
  }
}
