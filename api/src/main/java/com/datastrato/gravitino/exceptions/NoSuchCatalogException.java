/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.exceptions;

public class NoSuchCatalogException extends NotFoundException {

  public NoSuchCatalogException(String message) {
    super(message);
  }

  public NoSuchCatalogException(String message, Throwable cause) {
    super(message, cause);
  }
}
