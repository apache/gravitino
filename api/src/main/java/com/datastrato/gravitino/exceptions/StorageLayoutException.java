/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.exceptions;

public class StorageLayoutException extends GravitinoRuntimeException {

  public StorageLayoutException(String message) {
    super(message);
  }

  public StorageLayoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
