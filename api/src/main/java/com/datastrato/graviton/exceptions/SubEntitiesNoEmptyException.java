/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.exceptions;

public class SubEntitiesNoEmptyException extends GravitonRuntimeException {

  public SubEntitiesNoEmptyException(String message) {
    super(message);
  }

  public SubEntitiesNoEmptyException(String message, Throwable cause) {
    super(message, cause);
  }
}
