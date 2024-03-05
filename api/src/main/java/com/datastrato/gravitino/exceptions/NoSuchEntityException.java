/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/** This exception is thrown when an entity is not found. */
public class NoSuchEntityException extends RuntimeException {
  /** The no such entity message for the exception. */
  public static final String NO_SUCH_ENTITY_MESSAGE = "No such %s entity: %s";

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public NoSuchEntityException(@FormatString String message, Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a new NoSuchEntityException.
   *
   * @param message The detail message.
   * @param cause The cause of the exception.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public NoSuchEntityException(Throwable cause, @FormatString String message, Object... args) {
    super(String.format(message, args), cause);
  }
}
