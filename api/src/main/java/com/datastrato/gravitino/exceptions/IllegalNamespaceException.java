/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a namespace is invalid. */
public class IllegalNamespaceException extends IllegalArgumentException {

  public IllegalNamespaceException(String message) {
    super(message);
  }

  public IllegalNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalNamespaceException(Throwable cause) {
    super(cause);
  }

  public IllegalNamespaceException() {
    super();
  }
}
