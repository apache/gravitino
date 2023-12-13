/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;

public class StorageLayoutException extends GravitinoRuntimeException {

  public StorageLayoutException(String message) {
    super(message);
  }

  public StorageLayoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
