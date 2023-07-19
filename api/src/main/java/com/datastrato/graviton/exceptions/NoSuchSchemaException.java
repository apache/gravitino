/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.exceptions;

public class NoSuchSchemaException extends NotFoundException {

  public NoSuchSchemaException(String message) {
    super(message);
  }

  public NoSuchSchemaException(String message, Throwable cause) {
    super(message, cause);
  }
}
