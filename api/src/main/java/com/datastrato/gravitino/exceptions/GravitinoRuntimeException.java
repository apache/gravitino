/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** Base class for all Gravitino runtime exceptions. */
public class GravitinoRuntimeException extends RuntimeException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public GravitinoRuntimeException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public GravitinoRuntimeException(@FormatString String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public GravitinoRuntimeException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
