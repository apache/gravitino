/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a metalake is not found. */
public class NoSuchMetalakeException extends NotFoundException {

  public NoSuchMetalakeException(String message) {
    super(message);
  }

  public NoSuchMetalakeException(String message, Throwable cause) {
    super(message, cause);
  }
}
