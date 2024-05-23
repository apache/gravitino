/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;

/** Exception thrown when a tag with specified name is not existed. */
public class NoSuchTagException extends NotFoundException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public NoSuchTagException(String message, Object... args) {
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
  public NoSuchTagException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
