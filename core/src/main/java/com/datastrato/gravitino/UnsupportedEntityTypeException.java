/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** An exception thrown when an entity type is not supported for operations. */
public class UnsupportedEntityTypeException extends GravitinoRuntimeException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public UnsupportedEntityTypeException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public UnsupportedEntityTypeException(
      Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
