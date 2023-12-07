/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

/** An exception thrown when a resource is not empty. */
public class NonEmptyEntityException extends GravitinoRuntimeException {

  public NonEmptyEntityException(String message) {
    super(message);
  }

  public NonEmptyEntityException(String message, Throwable cause) {
    super(message, cause);
  }
}
