/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** Exception thrown when a file with specified name already exists. */
public class FileAlreadyExistsException extends AlreadyExistsException {

  public FileAlreadyExistsException(String message) {
    super(message);
  }

  public FileAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
