/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * Exception class indicating that an entity already exists. This exception is thrown when an
 * attempt is made to create an entity that already exists within the Gravitino framework.
 */
public class EntityAlreadyExistsException extends GravitinoRuntimeException {

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public EntityAlreadyExistsException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs an EntityAlreadyExistsException.
   *
   * @param cause the cause.
   * @param message the detail message.
   * @param args the arguments to the message.
   */
  @FormatMethod
  public EntityAlreadyExistsException(
      Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
