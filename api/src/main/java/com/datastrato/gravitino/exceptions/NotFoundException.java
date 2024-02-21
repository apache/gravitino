/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** Base class for all exceptions thrown when a resource is not found. */
public class NotFoundException extends GravitinoRuntimeException {

  /**
   * Constructs a new NotFoundException.
   *
   * @param message The detail message.
   * @param args The arguments to the message.
   */
  @FormatMethod
  public NotFoundException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new NotFoundException.
   *
   * @param cause The cause of the exception.
   * @param message The detail message.
   * @param args The arguments to the message.
   */
  @FormatMethod
  public NotFoundException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
