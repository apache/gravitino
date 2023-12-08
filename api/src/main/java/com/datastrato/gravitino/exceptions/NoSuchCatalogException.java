/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

/** An exception thrown when a catalog is not found. */
public class NoSuchCatalogException extends NotFoundException {

  public NoSuchCatalogException(String message) {
    super(message);
  }

  public NoSuchCatalogException(String message, Throwable cause) {
    super(message, cause);
  }
}
