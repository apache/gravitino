/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** An exception thrown when a namespace is invalid. */
public class IllegalNamespaceException extends IllegalArgumentException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public IllegalNamespaceException(@FormatString String message, Object... args) {
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
  public IllegalNamespaceException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }

  /**
   * Constructs a new exception with the specified cause.
   *
   * @param cause the cause.
   */
  public IllegalNamespaceException(Throwable cause) {
    super(cause);
  }

  /** Constructs a new exception with the specified detail message and cause. */
  public IllegalNamespaceException() {
    super();
  }
}
