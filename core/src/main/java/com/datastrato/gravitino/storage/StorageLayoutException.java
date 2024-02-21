/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

public class StorageLayoutException extends GravitinoRuntimeException {

  /**
   * Constructs a new StorageLayoutException.
   *
   * @param message The detail message.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public StorageLayoutException(@FormatString String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new StorageLayoutException.
   *
   * @param cause The cause of the exception.
   * @param message The detail message.
   * @param args The argument of the formatted message.
   */
  @FormatMethod
  public StorageLayoutException(Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
  }
}
