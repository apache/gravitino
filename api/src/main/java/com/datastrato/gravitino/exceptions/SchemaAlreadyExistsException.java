/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** An exception thrown when the schema already exists. */
public class SchemaAlreadyExistsException extends AlreadyExistsException {

  /**
   * Constructs a new SchemaAlreadyExistsException.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public SchemaAlreadyExistsException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new SchemaAlreadyExistsException.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public SchemaAlreadyExistsException(
      Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
