/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a namespace is invalid. */
public class IllegalNamespaceException extends IllegalArgumentException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message.
   */
  public IllegalNamespaceException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public IllegalNamespaceException(String message, Throwable cause) {
    super(message, cause);
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
