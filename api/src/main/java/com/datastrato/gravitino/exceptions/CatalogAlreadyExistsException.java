/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a resource already exists. */
public class CatalogAlreadyExistsException extends AlreadyExistsException {

  public CatalogAlreadyExistsException(String message) {
    super(message);
  }

  public CatalogAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
