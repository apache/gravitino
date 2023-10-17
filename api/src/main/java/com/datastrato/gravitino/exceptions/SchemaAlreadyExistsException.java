/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

public class SchemaAlreadyExistsException extends AlreadyExistsException {

  public SchemaAlreadyExistsException(String message) {
    super(message);
  }

  public SchemaAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
